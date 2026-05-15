"""
HSI / HHI LV2 實時行情採集守護進程

架構流程：
    Futu OpenD (127.0.0.1:11111)
         ↓ Push / Callback (TCP)
    TickerHandler / OrderBookHandler        ← 由 Futu SDK 在自己的 IO thread 呼叫
         ↓ queue.Queue (thread-safe，每張表一個)
    BatchWriter × 4 (背景 thread, 每 FLUSH_INTERVAL 秒批次寫入)
         ↓ supabase-py v2
    Supabase PostgreSQL (4 張獨立表)

防禦設計：
    1. 回調絕對不直接打網路：先入佇列，由背景 worker 批量寫入。
    2. 每張表獨立佇列 + 獨立 worker：某張表寫失敗不影響其他表。
    3. 佇列有 maxsize 上限：滿時丟棄最舊一筆並 WARN，避免記憶體無限膨脹。
    4. 寫入失敗：整批 requeue 等待下次重試。
    5. OpenD 斷線：health-check thread 偵測後重建 ctx + 重新訂閱。
    6. SIGINT / SIGTERM：set shutdown_event，所有 worker 最後 flush 一次再退出。
"""
from __future__ import annotations

import logging
import os
import queue
import signal
import sys
import threading
import time as time_module
from datetime import datetime, timedelta, timezone
from typing import Any

from dotenv import load_dotenv
from futu import (
    RET_OK,
    OpenQuoteContext,
    OrderBookHandlerBase,
    Session,
    SubType,
    TickerHandlerBase,
    set_all_thread_daemon,
)
from supabase import Client, create_client

load_dotenv()

# === 設定 ===========================================================
HK_TZ = timezone(timedelta(hours=8))

OPEND_HOST = os.getenv("OPEND_HOST", "127.0.0.1")
OPEND_PORT = int(os.getenv("OPEND_PORT", "11111"))
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")  # service_role key
FLUSH_INTERVAL = float(os.getenv("FLUSH_INTERVAL", "2.0"))
MAX_QUEUE_SIZE = int(os.getenv("MAX_QUEUE_SIZE", "50000"))
HEALTH_CHECK_INTERVAL = float(os.getenv("HEALTH_CHECK_INTERVAL", "30"))

CODES = ["HK.HSImain", "HK.HHImain"]
SUBTYPES = [SubType.TICKER, SubType.ORDER_BOOK]
# Session.ALL：涵蓋港股期指夜盤 (T+1 Session, 17:15 - 次日 03:00)
# is_detailed_orderbook=True：擺盤每檔額外帶逐筆委託明細 dict
SUBSCRIBE_SESSION = Session.ALL
SUBSCRIBE_DETAILED_ORDERBOOK = True

# code → table name
TICKER_TABLES: dict[str, str] = {
    "HK.HSImain": "hsi_ticker",
    "HK.HHImain": "hhi_ticker",
}
ORDER_BOOK_TABLES: dict[str, str] = {
    "HK.HSImain": "hsi_order_book",
    "HK.HHImain": "hhi_order_book",
}

# === Logging ========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("futu_collector")

# === 緩衝佇列：每張表一個 ===========================================
buffers: dict[str, queue.Queue] = {
    "hsi_ticker": queue.Queue(maxsize=MAX_QUEUE_SIZE),
    "hhi_ticker": queue.Queue(maxsize=MAX_QUEUE_SIZE),
    "hsi_order_book": queue.Queue(maxsize=MAX_QUEUE_SIZE),
    "hhi_order_book": queue.Queue(maxsize=MAX_QUEUE_SIZE),
}

shutdown_event = threading.Event()


# === 工具函數 =======================================================
def _safe_put(buf: queue.Queue, item: dict, table_name: str) -> None:
    """
    非阻塞寫入緩衝佇列。
    佇列滿時，丟棄最舊一筆並寫入新資料 + WARN。
    """
    try:
        buf.put_nowait(item)
    except queue.Full:
        try:
            dropped = buf.get_nowait()
            buf.put_nowait(item)
            logger.warning(
                "[%s] 緩衝已滿 (max=%d)，丟棄最舊一筆 (received_at=%s)",
                table_name,
                MAX_QUEUE_SIZE,
                dropped.get("received_at"),
            )
        except queue.Empty:
            # 罕見競爭情況：剛好被 worker 拿走，重試一次
            try:
                buf.put_nowait(item)
            except queue.Full:
                logger.error("[%s] 緩衝寫入失敗，丟棄當前一筆", table_name)


def _parse_hk_time(time_str: str | None) -> str | None:
    """
    Futu 香港交易所時間字串 → ISO8601 with +08:00 時區。
    支援格式：'YYYY-MM-DD HH:MM:SS.ffffff' / 'YYYY-MM-DD HH:MM:SS'
    """
    if not time_str:
        return None
    s = str(time_str).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=HK_TZ).isoformat()
        except ValueError:
            continue
    logger.warning("無法解析時間字串: %r", time_str)
    return None


def _json_safe(obj: Any) -> Any:
    """
    將 Futu / pandas 物件遞迴轉為 JSON 可序列化型別。
    處理：tuple → list, numpy/pandas 標量 → Python 原生
    供 raw_payload 寫入 JSONB 前使用。
    """
    if isinstance(obj, dict):
        return {str(k): _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_json_safe(x) for x in obj]
    if obj is None or isinstance(obj, (str, bool)):
        return obj
    if isinstance(obj, (int, float)):
        # NaN 視為 None（JSON 沒有 NaN）
        if isinstance(obj, float) and obj != obj:
            return None
        return obj
    # numpy / pandas 標量
    try:
        item = obj.item()  # numpy scalar → python scalar
        return _json_safe(item)
    except AttributeError:
        pass
    # pandas Timestamp / 其他無法直接序列化者
    return str(obj)


# === 回調 Handler ===================================================
class TickerHandler(TickerHandlerBase):
    """
    Ticker 回調：Futu 回傳的 content 是 pandas DataFrame，
    每列為一筆成交。
    """

    def on_recv_rsp(self, rsp_pb):
        ret_code, content = super().on_recv_rsp(rsp_pb)
        if ret_code != RET_OK:
            logger.error("Ticker 回調解析失敗: %s", content)
            return ret_code, content

        received_at = datetime.now(timezone.utc).isoformat()
        for _, row in content.iterrows():
            # 單筆失敗不應拖累整批，每筆獨立 try
            try:
                code = row.get("code")
                table = TICKER_TABLES.get(code)
                if not table:
                    continue
                price = row.get("price")
                volume = row.get("volume")
                if price is None or volume is None:
                    logger.warning("Ticker 缺少 price/volume，跳過: %s", dict(row))
                    continue
                trade_time = _parse_hk_time(row.get("time"))
                if trade_time is None:
                    logger.warning("Ticker 無有效 trade_time，跳過: %s", dict(row))
                    continue

                # 完整 row → raw_payload（保留 sequence / turnover / type / push_data_type / name）
                raw_payload = _json_safe(row.to_dict())

                payload = {
                    "code": code,
                    "trade_time": trade_time,
                    "price": float(price),
                    "volume": int(volume),
                    "ticker_direction": str(row.get("ticker_direction") or "") or None,
                    "received_at": received_at,
                    "raw_payload": raw_payload,
                }
                _safe_put(buffers[table], payload, table)
            except Exception:
                logger.exception("處理單筆 Ticker 例外，已跳過該筆")
        return ret_code, content


class OrderBookHandler(OrderBookHandlerBase):
    """
    OrderBook 回調：Futu 回傳的 content 是 dict，
    'Bid' / 'Ask' 為 list，每元素為 (price, volume, order_num, ext_dict)，
    最多 10 檔。
    """

    def on_recv_rsp(self, rsp_pb):
        ret_code, content = super().on_recv_rsp(rsp_pb)
        if ret_code != RET_OK:
            logger.error("OrderBook 回調解析失敗: %s", content)
            return ret_code, content

        try:
            received_at = datetime.now(timezone.utc).isoformat()
            code = content.get("code")
            table = ORDER_BOOK_TABLES.get(code)
            if not table:
                return ret_code, content

            bids = content.get("Bid") or []
            asks = content.get("Ask") or []

            # Q1: 主時間欄位用 svr_recv_time_bid，空值 fallback 到 received_at
            svr_time_str = _parse_hk_time(content.get("svr_recv_time_bid"))
            svr_recv_time_bid = svr_time_str if svr_time_str else received_at

            # 抽取 bid1 / ask1
            bid1_price = float(bids[0][0]) if bids else None
            bid1_volume = int(bids[0][1]) if bids else None
            ask1_price = float(asks[0][0]) if asks else None
            ask1_volume = int(asks[0][1]) if asks else None

            payload = {
                "code": code,
                "svr_recv_time_bid": svr_recv_time_bid,
                "bid1_price": bid1_price,
                "bid1_volume": bid1_volume,
                "ask1_price": ask1_price,
                "ask1_volume": ask1_volume,
                "bid_levels": len(bids),
                "ask_levels": len(asks),
                "received_at": received_at,
                # 完整 dict → raw_payload，tuple 轉 list 以利 JSONB 序列化
                "raw_payload": _json_safe(content),
            }
            _safe_put(buffers[table], payload, table)
        except Exception:
            logger.exception("處理 OrderBook 回調發生例外")
        return ret_code, content


# === 批次寫入 worker ================================================
class BatchWriter(threading.Thread):
    """每張表獨立的批次寫入背景 thread。"""

    def __init__(self, supabase: Client, table: str, buf: queue.Queue):
        super().__init__(name=f"writer-{table}", daemon=True)
        self.supabase = supabase
        self.table = table
        self.buf = buf

    def _drain(self) -> list[dict]:
        items: list[dict] = []
        while True:
            try:
                items.append(self.buf.get_nowait())
            except queue.Empty:
                break
        return items

    def _requeue(self, items: list[dict]) -> None:
        """寫入失敗時將資料放回佇列尾，下次重試。"""
        dropped = 0
        for item in items:
            try:
                self.buf.put_nowait(item)
            except queue.Full:
                dropped += 1
        if dropped:
            logger.error(
                "[%s] 重試入列失敗，佇列已滿，丟棄 %d 筆", self.table, dropped
            )

    def _flush_once(self) -> None:
        items = self._drain()
        if not items:
            return
        try:
            self.supabase.table(self.table).insert(items).execute()
            logger.info("[%s] 寫入成功 %d 筆", self.table, len(items))
        except Exception as exc:
            logger.error(
                "[%s] 寫入失敗 (%s)，保留 %d 筆等待重試",
                self.table,
                exc,
                len(items),
            )
            self._requeue(items)

    def run(self):
        logger.info("BatchWriter [%s] 啟動", self.table)
        while not shutdown_event.is_set():
            shutdown_event.wait(FLUSH_INTERVAL)
            if shutdown_event.is_set():
                break
            self._flush_once()

        # 關機前最後一次 flush
        logger.info("[%s] 關機前最終 flush...", self.table)
        try:
            self._flush_once()
        except Exception:
            logger.exception("[%s] 最終 flush 失敗", self.table)
        logger.info("BatchWriter [%s] 已退出", self.table)


# === 訂閱與健康檢查 =================================================
def subscribe_all(quote_ctx: OpenQuoteContext) -> bool:
    ret, data = quote_ctx.subscribe(
        CODES,
        SUBTYPES,
        is_first_push=True,
        is_detailed_orderbook=SUBSCRIBE_DETAILED_ORDERBOOK,
        session=SUBSCRIBE_SESSION,
    )
    if ret != RET_OK:
        logger.error("訂閱失敗: %s", data)
        return False
    logger.info(
        "訂閱成功: codes=%s, subtypes=%s, session=%s, detailed_ob=%s",
        CODES,
        [str(s) for s in SUBTYPES],
        SUBSCRIBE_SESSION,
        SUBSCRIBE_DETAILED_ORDERBOOK,
    )
    return True


def _build_ctx() -> OpenQuoteContext | None:
    """建立 OpenQuoteContext 並完成 handler 註冊與訂閱。"""
    try:
        ctx = OpenQuoteContext(host=OPEND_HOST, port=OPEND_PORT)
    except Exception as exc:
        logger.error("建立 OpenQuoteContext 失敗: %s", exc)
        return None
    ctx.set_handler(TickerHandler())
    ctx.set_handler(OrderBookHandler())
    if not subscribe_all(ctx):
        try:
            ctx.close()
        except Exception:
            pass
        return None
    return ctx


def health_check_loop(ctx_holder: dict[str, Any]) -> None:
    """
    定期 ping OpenD；失敗則關閉舊 ctx、重建並重新訂閱。
    """
    while not shutdown_event.is_set():
        shutdown_event.wait(HEALTH_CHECK_INTERVAL)
        if shutdown_event.is_set():
            break
        ctx = ctx_holder.get("ctx")
        ok = False
        try:
            if ctx is not None:
                ret, _ = ctx.get_global_state()
                ok = ret == RET_OK
        except Exception as exc:
            logger.warning("OpenD health-check 例外: %s", exc)

        if ok:
            continue

        logger.warning("OpenD 連線異常，嘗試重連 ...")
        try:
            if ctx is not None:
                ctx.close()
        except Exception:
            pass

        new_ctx = _build_ctx()
        if new_ctx is not None:
            ctx_holder["ctx"] = new_ctx
            logger.info("OpenD 重連並重新訂閱完成")
        else:
            logger.error("OpenD 重連失敗，下次再試")


# === Main ===========================================================
def main() -> None:
    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.error("缺少 SUPABASE_URL 或 SUPABASE_KEY 環境變數")
        sys.exit(1)

    logger.info("=== HSI/HHI LV2 採集守護進程 啟動 ===")
    logger.info(
        "OpenD=%s:%d, flush=%.1fs, max_queue=%d, health=%.1fs",
        OPEND_HOST,
        OPEND_PORT,
        FLUSH_INTERVAL,
        MAX_QUEUE_SIZE,
        HEALTH_CHECK_INTERVAL,
    )

    # 確保 SDK 內部所有 thread 為 daemon，避免 ctx.close() 卡住時主程序無法退出
    set_all_thread_daemon(True)

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    quote_ctx = _build_ctx()
    if quote_ctx is None:
        logger.error("初始 OpenD 連線/訂閱失敗，結束")
        sys.exit(1)
    ctx_holder: dict[str, Any] = {"ctx": quote_ctx}

    # 啟動 4 個 batch writer
    writers = [BatchWriter(supabase, table, buf) for table, buf in buffers.items()]
    for w in writers:
        w.start()

    # 啟動健康檢查
    health_thread = threading.Thread(
        target=health_check_loop,
        args=(ctx_holder,),
        name="health-check",
        daemon=True,
    )
    health_thread.start()

    # 訊號處理
    def _handle_signal(sig, _frame):
        logger.info("收到訊號 %s，準備關機...", sig)
        shutdown_event.set()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    # 主執行緒掛住等待
    while not shutdown_event.is_set():
        time_module.sleep(1)

    # 收尾：先關閉 OpenD，停止資料源；再讓 writer 把佇列 drain 完
    logger.info("關閉 OpenD 連線...")
    try:
        ctx_holder["ctx"].close()
    except Exception as exc:
        logger.warning("關閉 OpenD 失敗: %s", exc)

    logger.info("等待 writers flush 最後資料 ...")
    for w in writers:
        w.join(timeout=15)

    logger.info("=== 已優雅關機 ===")


if __name__ == "__main__":
    main()
