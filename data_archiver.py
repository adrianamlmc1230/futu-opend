"""
data_archiver.py — 港股 HSI/HHI LV2 行情冷熱數據分離

每日 04:30 (HKT) 把 Supabase 4 張表中「上一個交易日」的資料：
    1. 用 pandas 從 Postgres 拉出來（D 04:00 HKT ≤ ts < D+1 04:00 HKT）
       — 用 04:00 切日把日盤 + 夜期完整放進同一個 parquet
    2. 轉成 ZSTD 壓縮的 Parquet
    3. PUT 到 Cloudflare R2 (S3 相容)
    4. **驗證上傳成功才** DELETE 對應的 Postgres rows
    5. VACUUM FULL：把 dead tuple 空間還給 OS（需 5432 直連）

防禦設計
    - SELECT 時記下 (min_id, max_id)，DELETE 用 id 範圍鎖死
      → 即使期間有 late-arriving / out-of-order 資料，也不會誤刪
    - 上傳完成後做 head_object 驗證 + ContentLength 比對，**驗證失敗不刪 DB**
    - **R2 同 key 已存在時**：
        * 內容大小相同 → 視為 idempotent re-run，skip 上傳直接做 DELETE
        * 內容不同     → 自動 copy 舊檔到 `key.bak.<unix_ts>` 再覆寫，舊資料絕不遺失
    - 寫入失敗的暫存 Parquet 留在 /tmp，下次重跑會 overwrite
    - 整個流程 idempotent：當天重跑無副作用
    - raw_payload (JSONB) 在寫 Parquet 前 json.dumps() 轉字串，
      避免 pyarrow 對 mixed-type dict 的 schema-infer 失敗

排程器（archiver_scheduler.py）每天 04:30 觸發一次此模組的 run_once()。
也可以本機用 `python data_archiver.py [--dry-run] [--date YYYY-MM-DD]` 手動跑。
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import boto3
import pandas as pd
from botocore.client import Config as BotoConfig
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

# === 設定 ===========================================================
HK_TZ = timezone(timedelta(hours=8))

SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL")
S3_ACCESS_KEY_ID = os.getenv("S3_ACCESS_KEY_ID")
S3_SECRET_ACCESS_KEY = os.getenv("S3_SECRET_ACCESS_KEY")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_REGION = os.getenv("S3_REGION", "auto")  # R2 用 'auto'

# table -> 用於切日範圍的時間欄位
TABLES: dict[str, str] = {
    "hsi_ticker": "trade_time",
    "hhi_ticker": "trade_time",
    "hsi_order_book": "svr_recv_time_bid",
    "hhi_order_book": "svr_recv_time_bid",
}

# === Logging ========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("archiver")


# === 資料結構 =======================================================
@dataclass
class ArchiveResult:
    table: str
    rows: int
    bytes_uploaded: int
    s3_key: str
    deleted: int


# === 工具函數 =======================================================
def trading_day_hk_range(target_date: date | None = None) -> tuple[datetime, datetime, date]:
    """
    回傳「交易日切日」的 (start, end, date_obj)：

      交易日 D = D 04:00 HKT  ≤ ts <  D+1 04:00 HKT

    為什麼是 04:00 切：
      - 港股期指夜期 17:15 HKT → 次日 03:00 HKT
      - 用 04:00 切日可以把同一個交易日的「日盤 + 夜期」完整放進同一個 parquet
      - calendar-day 切會把夜期切成兩半，分析時很麻煩

    target_date 預設為「現在 HKT 之前已經收盤完成的最近交易日」：
      - 如果現在 HKT >= 04:00：那麼「昨天」剛收盤完成（昨天日盤 + 昨晚夜期），target = 昨天
      - 如果現在 HKT <  04:00：那麼前天才剛收盤完成，target = 前天
    """
    if target_date is None:
        now_hk = datetime.now(HK_TZ)
        # archiver 排在 04:30 觸發，正常情況 hour >= 4，target = yesterday
        # 若提早跑（< 04:00），代表夜期還沒收盤，要往前推一天
        if now_hk.hour >= 4:
            target_date = (now_hk - timedelta(days=1)).date()
        else:
            target_date = (now_hk - timedelta(days=2)).date()
    start = datetime.combine(target_date, datetime.min.time(), tzinfo=HK_TZ) + timedelta(hours=4)
    end = start + timedelta(days=1)
    return start, end, target_date


def make_s3_client():
    """建立 boto3 S3 client，相容 Cloudflare R2。"""
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=S3_ACCESS_KEY_ID,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
        region_name=S3_REGION,
        config=BotoConfig(
            signature_version="s3v4",
            retries={"max_attempts": 3, "mode": "standard"},
        ),
    )


@contextmanager
def temp_parquet_path(table: str, date_str: str):
    """產生一個 /tmp 暫存路徑，with 結束後刪除。"""
    fd, path = tempfile.mkstemp(suffix=".parquet", prefix=f"{table}_{date_str}_")
    os.close(fd)
    try:
        yield Path(path)
    finally:
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass


def _vacuum_full(tables: list[str]) -> None:
    """
    對指定表執行 VACUUM FULL，把 dead tuple 占用的實體空間還給 OS。

    必須用 5432 直連（SUPABASE_DB_DIRECT_URL），pgbouncer pool 不支援。
    若沒設 SUPABASE_DB_DIRECT_URL 則跳過（log 警告）。

    autocommit 模式：VACUUM 不能在 transaction 內執行。
    """
    direct_url = os.getenv("SUPABASE_DB_DIRECT_URL")
    if not direct_url:
        logger.warning("未設定 SUPABASE_DB_DIRECT_URL，跳過 VACUUM FULL（autovacuum 會處理 dead tuple，但體積要等下次重寫才會縮）")
        return

    logger.info("=" * 60)
    logger.info("VACUUM FULL：把 dead tuple 空間還給 OS")
    logger.info("=" * 60)
    engine = create_engine(direct_url, isolation_level="AUTOCOMMIT", pool_pre_ping=True)
    try:
        with engine.connect() as conn:
            for tbl in tables:
                t0 = datetime.now()
                try:
                    conn.execute(text(f"VACUUM FULL {tbl}"))
                    elapsed = (datetime.now() - t0).total_seconds()
                    # 取新的體積
                    size = conn.execute(
                        text(f"SELECT pg_size_pretty(pg_total_relation_size('{tbl}'::regclass))")
                    ).scalar()
                    logger.info("  - %-18s 完成（%.1fs）→ size %s", tbl, elapsed, size)
                except Exception as exc:
                    logger.error("  - %-18s 失敗: %s", tbl, exc)
    finally:
        engine.dispose()
    logger.info("=" * 60)


# === 主流程 =========================================================
def archive_one_table(
    engine,
    s3,
    table: str,
    time_col: str,
    start: datetime,
    end: datetime,
    target_date: date,
    dry_run: bool = False,
) -> ArchiveResult | None:
    """
    處理單張表：extract → transform → upload → verify → delete。
    上傳驗證失敗會 raise，不會跑到 DELETE。
    """
    log = logger.getChild(table)
    date_str = target_date.isoformat()

    # ----- Step 1：extract + 記錄 id 邊界 -----
    log.info("Step 1/4 撈取 %s where %s in [%s, %s)", table, time_col, start, end)
    select_sql = text(
        f"SELECT * FROM {table} "
        f"WHERE {time_col} >= :start AND {time_col} < :end "
        f"ORDER BY id"
    )
    with engine.connect() as conn:
        df = pd.read_sql(select_sql, conn, params={"start": start, "end": end})

    if df.empty:
        log.info("無資料，跳過")
        return None

    min_id = int(df["id"].min())
    max_id = int(df["id"].max())
    log.info("  撈到 %d 筆，id 範圍 [%d, %d]", len(df), min_id, max_id)

    # JSONB 欄位 → JSON 字串
    # pandas 從 Postgres 讀 JSONB 會解析為 Python dict/list，pyarrow 對
    # 混型別（如 order book 中 {} 與 {orderid: vol} 並存）的 schema-infer 會失敗。
    # 統一在這裡 json.dumps()，Parquet 存字串；分析時用 json.loads() 或
    # DuckDB read_json_auto() 還原。
    if "raw_payload" in df.columns:
        df["raw_payload"] = df["raw_payload"].apply(
            lambda v: None if v is None else json.dumps(v, separators=(",", ":"), ensure_ascii=False)
        )

    # ----- Step 2：transform → Parquet -----
    with temp_parquet_path(table, date_str) as pq_path:
        log.info("Step 2/4 寫入 Parquet (zstd) → %s", pq_path)
        df.to_parquet(pq_path, compression="zstd", index=False)
        size = pq_path.stat().st_size
        log.info("  檔案大小 %.2f MB", size / 1024 / 1024)

        # 立刻釋放記憶體（後面只需要 size + min_id + max_id）
        del df

        # ----- Step 3：upload to R2 -----
        s3_key = f"{table}/{target_date.year:04d}/{target_date.month:02d}/{target_date.day:02d}.parquet"
        log.info("Step 3/4 上傳 s3://%s/%s", S3_BUCKET_NAME, s3_key)

        if dry_run:
            log.warning("  [DRY-RUN] 跳過上傳與刪除")
            return ArchiveResult(table, max_id - min_id + 1, size, s3_key, 0)

        # 防護：若 key 已存在
        #   - 內容大小相同 → 視為 idempotent re-run，skip 上傳直接做 DELETE（無副作用）
        #   - 內容不同     → 自動備份舊檔到 .bak.<unix_ts>，再覆寫；舊資料絕不遺失
        existing_size = None
        try:
            existing = s3.head_object(Bucket=S3_BUCKET_NAME, Key=s3_key)
            existing_size = existing["ContentLength"]
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") not in ("404", "NoSuchKey"):
                raise

        skip_upload = False
        if existing_size is not None:
            if existing_size == size:
                log.info("  R2 已有同尺寸物件 (%d bytes)，視為重跑，skip 上傳",
                         existing_size)
                skip_upload = True
            else:
                ts = int(datetime.now(timezone.utc).timestamp())
                bak_key = f"{s3_key}.bak.{ts}"
                log.warning(
                    "  R2 已有 %d bytes 但本次要上傳 %d bytes，先備份舊檔到 %s",
                    existing_size, size, bak_key,
                )
                s3.copy_object(
                    Bucket=S3_BUCKET_NAME,
                    Key=bak_key,
                    CopySource={"Bucket": S3_BUCKET_NAME, "Key": s3_key},
                )

        if not skip_upload:
            try:
                s3.upload_file(
                    Filename=str(pq_path),
                    Bucket=S3_BUCKET_NAME,
                    Key=s3_key,
                    ExtraArgs={"ContentType": "application/vnd.apache.parquet"},
                )
            except (BotoCoreError, ClientError) as exc:
                log.error("上傳失敗，**不刪除 DB**：%s", exc)
                raise

            # ----- Step 3.5：上傳後驗證 -----
            log.info("Step 3.5 驗證 head_object")
            try:
                head = s3.head_object(Bucket=S3_BUCKET_NAME, Key=s3_key)
            except ClientError as exc:
                log.error("head_object 失敗，**不刪除 DB**：%s", exc)
                raise

            remote_size = head["ContentLength"]
            if remote_size != size:
                log.error(
                    "Size 不一致 (local=%d, remote=%d)，**不刪除 DB**",
                    size,
                    remote_size,
                )
                raise RuntimeError(
                    f"upload size mismatch local={size} remote={remote_size}"
                )
            log.info("  驗證通過 (size=%d, etag=%s)", remote_size, head.get("ETag"))

    # ----- Step 4：delete from Postgres -----
    log.info("Step 4/4 DELETE rows id BETWEEN %d AND %d", min_id, max_id)
    delete_sql = text(
        f"DELETE FROM {table} "
        f"WHERE id BETWEEN :min_id AND :max_id "
        f"  AND {time_col} >= :start AND {time_col} < :end"
    )
    with engine.begin() as conn:  # 自動 transaction
        result = conn.execute(
            delete_sql,
            {"min_id": min_id, "max_id": max_id, "start": start, "end": end},
        )
        deleted = result.rowcount or 0
    log.info("  已刪除 %d 筆", deleted)

    return ArchiveResult(table, max_id - min_id + 1, size, s3_key, deleted)


def run_once(target_date: date | None = None, dry_run: bool = False) -> list[ArchiveResult]:
    """
    跑完整一次封存。回傳每張表的結果。
    """
    _check_env()
    start, end, target_date = trading_day_hk_range(target_date)
    logger.info("=" * 60)
    logger.info("封存交易日: %s (HKT 04:00 → 次日 04:00)", target_date)
    logger.info("時間區間: %s ~ %s", start, end)
    logger.info("=" * 60)

    engine = create_engine(SUPABASE_DB_URL, pool_pre_ping=True)
    s3 = make_s3_client()
    results: list[ArchiveResult] = []

    try:
        for table, time_col in TABLES.items():
            try:
                r = archive_one_table(
                    engine, s3, table, time_col, start, end, target_date, dry_run
                )
                if r:
                    results.append(r)
            except Exception:
                # 單一表失敗不應拖累其他表（資料已備份在 R2 也沒事）
                logger.exception("[%s] 封存失敗", table)
    finally:
        engine.dispose()

    # 摘要
    logger.info("=" * 60)
    logger.info("封存完成")
    total_rows = sum(r.rows for r in results)
    total_mb = sum(r.bytes_uploaded for r in results) / 1024 / 1024
    total_deleted = sum(r.deleted for r in results)
    logger.info("  共 %d 張表、%d 筆、%.2f MB 上傳 R2，刪除 %d 筆",
                len(results), total_rows, total_mb, total_deleted)
    for r in results:
        logger.info("  - %s: %d rows / %.2f MB → %s (deleted=%d)",
                    r.table, r.rows, r.bytes_uploaded / 1024 / 1024,
                    r.s3_key, r.deleted)
    logger.info("=" * 60)

    # ---- VACUUM FULL：把 dead tuple 占用的實體空間還給 OS ----
    # 必須走「直連 5432」，pgbouncer transaction-mode (port 6543) 不支援
    # VACUUM FULL 取 ACCESS EXCLUSIVE LOCK，會 block collector 寫入幾秒
    # 但 collector 有 requeue 機制，超時會自動重試，不會丟資料
    if not dry_run and total_deleted > 0:
        _vacuum_full(list(TABLES.keys()))

    return results


def _check_env():
    missing = [
        k for k, v in {
            "SUPABASE_DB_URL": SUPABASE_DB_URL,
            "S3_ENDPOINT_URL": S3_ENDPOINT_URL,
            "S3_ACCESS_KEY_ID": S3_ACCESS_KEY_ID,
            "S3_SECRET_ACCESS_KEY": S3_SECRET_ACCESS_KEY,
            "S3_BUCKET_NAME": S3_BUCKET_NAME,
        }.items()
        if not v
    ]
    if missing:
        raise SystemExit(f"缺少環境變數: {', '.join(missing)}")


# === CLI ============================================================
def _parse_args():
    p = argparse.ArgumentParser(description="HSI/HHI 行情冷熱分離封存")
    p.add_argument(
        "--date",
        help="指定要封存的日期 (YYYY-MM-DD)，預設為 HKT 昨天",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="只跑 SELECT + Parquet 寫檔，不上傳也不刪除",
    )
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    try:
        run_once(target_date=args.date, dry_run=args.dry_run)
    except SystemExit:
        raise
    except Exception:
        logger.exception("封存流程發生未捕獲例外")
        sys.exit(1)
