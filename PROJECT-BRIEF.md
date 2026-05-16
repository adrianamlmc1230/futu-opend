# 香港期指 LV2 採集守護進程

## 這個專案在做什麼？
24/5 不間斷地把 Futu OpenD 推送過來的 HSI / HHI 主連的逐筆成交與 10 檔擺盤資料寫進 Supabase，供盤後量化分析使用。
無策略、無交易、無查詢 API，純 ETL 的 EL 部分。

**儲存策略**：混合式儲存（Hybrid Storage）— 核心檢索欄位獨立成 column 並建 index，完整原始資料保存在 `raw_payload` JSONB 欄位。

## 目前狀態
- ✅ 4 張表 DDL（V2 混合式：核心欄位 + raw_payload JSONB）
- ✅ 主程序：訂閱、回調、佇列、批次寫入、健康檢查、訊號處理
- ✅ Docker 化（host network + restart:always）
- ✅ **已上線運行**：阿里雲香港 VPS (`47.76.134.145`) + Futu OpenD 10.5.6508 + container 自動重啟
- ✅ Futu HK Futures LV2 權限正常，4 張表持續寫入（每秒數十筆）
- ✅ Phase 1 分析部存取：`analyst` role + 4 個 VIEW
- ✅ 冷熱分離（V1）：`data_archiver.py` + `archiver_scheduler.py`，每天 HKT 04:00 把昨天資料封存到 Cloudflare R2 並刪除 DB
- 📊 實測消耗：~600 MB / 日（~750k rows，擺盤占 88%）
- ⏸ 尚未做舊資料清理（依需求暫不做）

## 關鍵決策紀錄
| 日期 | 決策 | 原因 |
| --- | --- | --- |
| 初版 | 擺盤用 JSONB 而非 40 欄位 | 寫入精簡、pandas 端 `json_normalize` 一行展開、未來可擴 20 檔免 migration |
| 初版 | 用 thread + queue，不用 asyncio | Futu 回調與 supabase-py 都是同步，asyncio 反而要橋接 |
| 初版 | 每張表獨立 worker | 故障隔離 |
| 初版 | 失敗整批 requeue | 暫時的 Supabase 不可用不丟資料 |
| 初版 | 不做告警 | 依靠 `restart: always` |
| 對齊官方文件後 | `subscribe(session=Session.ALL, is_detailed_orderbook=True)` | 涵蓋夜期 + 取得擺盤每檔逐筆委託明細 |
| 對齊官方文件後 | Ticker 改單筆 try/except + `.get()` | 避免單欄位缺失打掛整批 |
| futu-api v10.5+ | 移除 `set_all_thread_daemon` 呼叫 | SDK v10.5+ 已刪除該 API，import 失敗會卡 restart 迴圈；改靠 `shutdown_event` + `daemon=True` 自管 thread lifecycle |
| V2 重構 | 改混合式儲存（核心欄位 + raw_payload JSONB） | 查詢效能 + 完整資料追溯兩者兼顧 |
| V2 重構 | OrderBook 主時間用 `svr_recv_time_bid`，空值 fallback `received_at` | 保留交易所側時間語義，且 NOT NULL |
| V2 重構 | 加 `bid_levels` / `ask_levels` 核心欄位 | 監控擺盤完整性（是否每次都 10 檔）|
| V2 重構 | Ticker `sequence` 不抽出、不做唯一約束 | 接受重連補推可能造成的重複，後段去重 |
| 部署設定 | 4 張表全部開 RLS，但暫不寫 policy | service_role 會 bypass 不影響採集；同時擋住 anon key 萬一外洩造成的資料裸奔；未來要做查詢前端時再針對特定 role 加 SELECT policy |

## 邏輯偽代碼

### Ticker 流程
```
on_recv_rsp(ticker_df):
    received_at = utcnow()
    for row in ticker_df.iterrows():
        try:                                        # 單筆獨立 try
            code  = row.get('code')
            table = TICKER_TABLES[code]             # 路由
            price, volume = row.get('price'), row.get('volume')
            if price is None or volume is None: skip + WARN
            trade_time = parse_hk_time(row.get('time'))
            if trade_time is None: skip + WARN
            raw_payload = json_safe(row.to_dict())  # 完整 row → JSONB
            payload = {
                code, trade_time, price, volume,
                ticker_direction,                   # 核心檢索欄位
                received_at,
                raw_payload                         # 含 sequence/turnover/type/...
            }
            safe_put(buffers[table], payload)        # 滿則丟最舊 + WARN
        except: log + skip 該筆 (不影響其他筆)
```

### Order Book 流程
```
on_recv_rsp(content_dict):
    received_at = utcnow()
    code  = content_dict.code
    table = ORDER_BOOK_TABLES[code]
    bids  = content_dict.get('Bid') or []
    asks  = content_dict.get('Ask') or []

    svr_time = parse_hk_time(content_dict.svr_recv_time_bid)
    if svr_time is None or empty:                   # Q1: 空值 fallback
        svr_time = received_at

    payload = {
        code,
        svr_recv_time_bid = svr_time,               # 主時間索引
        bid1_price, bid1_volume,                    # 抽 best bid
        ask1_price, ask1_volume,                    # 抽 best ask
        bid_levels = len(bids), ask_levels = len(asks),  # Q2: 完整性監控
        received_at,
        raw_payload = json_safe(content_dict)       # 完整 dict → JSONB
                                                    # 含 Bid/Ask 全 10 檔、
                                                    # svr_recv_time_ask、name、委託明細
    }
    safe_put(buffers[table], payload)
```

### Batch Writer 流程（每張表獨立一個 thread）
```
loop until shutdown:
    wait FLUSH_INTERVAL seconds
    items = drain_all(buffer)                    # 把佇列清空
    if items is empty: continue
    try:
        supabase.table(table).insert(items).execute()
        log "寫入成功 N 筆"
    except Exception as e:
        log "寫入失敗，requeue N 筆"
        for it in items: buffer.put_nowait(it)   # 放回佇列尾
on shutdown:
    final_flush()                                # 退出前最後一次嘗試
```

### Health Check 流程
```
loop until shutdown:
    wait HEALTH_CHECK_INTERVAL seconds
    try:
        ret, _ = ctx.get_global_state()
        ok = (ret == RET_OK)
    except: ok = False
    if not ok:
        log WARN "OpenD 連線異常，重連 ..."
        try ctx.close()
        new_ctx = build_ctx()                    # 重新 set_handler + subscribe
        if new_ctx: ctx_holder['ctx'] = new_ctx
```

### 啟動 / 關機順序
```
startup:
    create supabase client
    ctx = build_ctx()                            # subscribe(session=Session.ALL,
                                                 #           is_detailed_orderbook=True)
                                                 # 失敗即 sys.exit(1)
    start 4 BatchWriter threads (daemon=True)    # 自管 thread lifecycle
    start 1 health-check thread (daemon=True)
    register SIGINT / SIGTERM → shutdown_event.set()
    main thread sleep until shutdown

shutdown:
    ctx.close()                                  # 停止資料源（先關，避免新資料湧入）
    join all BatchWriter (timeout=15s)           # 每個 worker 退出前 final flush
```

## 資料表速查 (V2 — 混合式儲存)

### Ticker 表 (`hsi_ticker` / `hhi_ticker`)
| 欄位 | 型別 | 來源 |
| --- | --- | --- |
| `id` | BIGSERIAL | PK |
| `code` | TEXT | DataFrame `code` |
| `trade_time` | TIMESTAMPTZ | DataFrame `time` parsed +08:00 |
| `price` | NUMERIC(12,4) | DataFrame `price` |
| `volume` | BIGINT | DataFrame `volume` |
| `ticker_direction` | TEXT | DataFrame `ticker_direction` |
| `received_at` | TIMESTAMPTZ | UTC |
| `raw_payload` | JSONB | 完整 `row.to_dict()`（含 sequence / turnover / type / push_data_type / name）|

Index: `trade_time DESC`、`received_at DESC`、`ticker_direction`

### Order Book 表 (`hsi_order_book` / `hhi_order_book`)
| 欄位 | 型別 | 來源 |
| --- | --- | --- |
| `id` | BIGSERIAL | PK |
| `code` | TEXT | dict `code` |
| `svr_recv_time_bid` | TIMESTAMPTZ NOT NULL | dict `svr_recv_time_bid` parsed +08:00；**空值 fallback 至 received_at** |
| `bid1_price` / `bid1_volume` | NUMERIC / BIGINT | `Bid[0][0]` / `Bid[0][1]` |
| `ask1_price` / `ask1_volume` | NUMERIC / BIGINT | `Ask[0][0]` / `Ask[0][1]` |
| `bid_levels` / `ask_levels` | SMALLINT | `len(Bid)` / `len(Ask)` |
| `received_at` | TIMESTAMPTZ | UTC |
| `raw_payload` | JSONB | 完整 dict（含 10 檔 Bid/Ask、svr_recv_time_ask、name、委託明細）|

Index: `svr_recv_time_bid DESC`、`received_at DESC`

### 從 raw_payload 取資料的常用語法
```sql
-- ticker 序號去重
SELECT DISTINCT ON ((raw_payload->>'sequence')::bigint) *
FROM hsi_ticker;

-- 取第 N 檔擺盤（0-indexed）
SELECT raw_payload->'Bid'->2 FROM hsi_order_book;
-- → [167.50, 200, 3, {}]

-- 取 spread
SELECT svr_recv_time_bid, ask1_price - bid1_price AS spread
FROM hsi_order_book WHERE bid_levels = 10 AND ask_levels = 10;
```

## 已知限制 / 待辦
- ⚠ 主機重啟、容器 OOM 時佇列內資料會遺失（trade-off：簡化設計）
- ⚠ Supabase 長時間不可用時佇列會滿，會丟棄最舊資料（保新棄舊）
- ⚠ Futu 帳號**必須具備 LV2 訂閱權限**，否則港股期指 TICKER 不會推送（官方限制：HK options/futures TICKER 在 LV1 下無法訂閱）
- ⚠ **Supabase 必須是 Pro tier 或更高**：本服務 LV2 推送一日寫入量約 1-3 GB，Free tier 500 MB 撐不過 24 小時。上線前必須先升級或安排 Phase 2 Parquet 轉存
- ℹ Supabase / PostgREST HTTP/2 連線跑滿 19999 stream 後會主動 reset（`ConnectionTerminated last_stream_id:19999`）：屬正常 lifecycle，由 BatchWriter 的 requeue 機制吸收、不會丟資料；19 小時內出現約 9 次屬正常頻率
- ⏳ 未做表體積監控與分區（rolling），長期跑需考慮
- ⏳ Windows / macOS 開發環境需自行調整 `network_mode`

## 分析部門存取策略
- **Phase 1（現階段）**：Read-only `analyst` role + 4 個 VIEW（`v_hsi_ticker` / `v_hhi_ticker` / `v_hsi_order_book` / `v_hhi_order_book`）
  - VIEW 把 raw_payload 扁平展開成普通欄位（含 1-10 檔擺盤、spread、mid_price）
  - VIEW 用預設 INVOKER 模式（避免 Supabase Security Advisor 警告）
  - analyst 同時授權底層 4 張實表 + 4 個 VIEW 的 SELECT；文件只教用 VIEW
  - SQL：`sql/analyst_role.sql`（首次完整建置）/ `sql/analyst_role_views_only.sql`（重建 VIEW 不重設密碼）
  - 使用說明：`docs/analyst_quickstart.md`
- **Phase 2（觸發條件：表體積 > 50GB / 查詢延遲 > 30s / 上線滿 30 天，任一）**：Parquet 增量轉存
  - 7 天熱資料留 Postgres、歷史走 Parquet (DuckDB / FDW)
  - VIEW 改成 `Postgres UNION ALL Parquet`，分析師 query 不用改
  - 計劃：`docs/phase2_parquet_plan.md`
