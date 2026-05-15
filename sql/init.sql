-- =====================================================================
-- HSI / HHI LV2 實時行情儲存 schema (V2 — 混合式儲存)
-- 部署目標：Supabase (PostgreSQL)
--
-- 混合式儲存策略
--   1. 「核心檢索欄位」：抽出最常用於 WHERE 篩選的欄位獨立成 column 並建 index
--   2. 「全數據封裝」：完整原始資料保存在 raw_payload JSONB 欄位，方便日後追溯
--   3. 不對 ticker.sequence 做唯一約束，重連補推資料容許重複寫入（後段去重）
--
-- 時區
--   - 所有時間欄位使用 TIMESTAMPTZ
--   - 交易所時間 / 伺服器時間在 Python 端以 HK +08:00 解析後寫入
--   - received_at 使用 UTC，用於延遲監控
-- =====================================================================

-- ---------------------------------------------------------------------
-- 1. 恆指主連 逐筆成交 (Ticker)
--    raw_payload 包含完整 row.to_dict()：name, sequence, time, price,
--    volume, turnover, ticker_direction, type, push_data_type
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hsi_ticker (
    id                BIGSERIAL PRIMARY KEY,
    code              TEXT          NOT NULL,
    trade_time        TIMESTAMPTZ   NOT NULL,         -- 交易所成交時間 (HK +08:00)
    price             NUMERIC(12,4) NOT NULL,
    volume            BIGINT        NOT NULL,
    ticker_direction  TEXT,                           -- BUY / SELL / NEUTRAL
    received_at       TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    raw_payload       JSONB         NOT NULL          -- 完整原始資料
);

CREATE INDEX IF NOT EXISTS idx_hsi_ticker_trade_time
    ON hsi_ticker (trade_time DESC);
CREATE INDEX IF NOT EXISTS idx_hsi_ticker_received_at
    ON hsi_ticker (received_at DESC);
CREATE INDEX IF NOT EXISTS idx_hsi_ticker_direction
    ON hsi_ticker (ticker_direction);

-- ---------------------------------------------------------------------
-- 2. 國指主連 逐筆成交 (Ticker)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hhi_ticker (
    id                BIGSERIAL PRIMARY KEY,
    code              TEXT          NOT NULL,
    trade_time        TIMESTAMPTZ   NOT NULL,
    price             NUMERIC(12,4) NOT NULL,
    volume            BIGINT        NOT NULL,
    ticker_direction  TEXT,
    received_at       TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    raw_payload       JSONB         NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_hhi_ticker_trade_time
    ON hhi_ticker (trade_time DESC);
CREATE INDEX IF NOT EXISTS idx_hhi_ticker_received_at
    ON hhi_ticker (received_at DESC);
CREATE INDEX IF NOT EXISTS idx_hhi_ticker_direction
    ON hhi_ticker (ticker_direction);

-- ---------------------------------------------------------------------
-- 3. 恆指主連 10 檔擺盤 (Order Book)
--    核心欄位設計：
--      - svr_recv_time_bid 為主要時間索引 (Q1 決策)
--        若 Futu 回傳為空字串，Python 端 fallback 至 received_at
--      - bid1/ask1 抽出方便算 spread / midprice
--      - bid_levels / ask_levels 監控擺盤完整性 (Q2 決策)
--      - 完整 10 檔留在 raw_payload.Bid / raw_payload.Ask
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hsi_order_book (
    id                  BIGSERIAL PRIMARY KEY,
    code                TEXT          NOT NULL,
    svr_recv_time_bid   TIMESTAMPTZ   NOT NULL,        -- 買盤伺服器時間 (Q1)
    bid1_price          NUMERIC(12,4),
    bid1_volume         BIGINT,
    ask1_price          NUMERIC(12,4),
    ask1_volume         BIGINT,
    bid_levels          SMALLINT      NOT NULL,        -- 實際檔位數 (Q2)
    ask_levels          SMALLINT      NOT NULL,
    received_at         TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    raw_payload         JSONB         NOT NULL         -- 完整 10 檔 + svr_recv_time_ask + 委託明細
);

CREATE INDEX IF NOT EXISTS idx_hsi_order_book_svr_recv_time
    ON hsi_order_book (svr_recv_time_bid DESC);
CREATE INDEX IF NOT EXISTS idx_hsi_order_book_received_at
    ON hsi_order_book (received_at DESC);

-- ---------------------------------------------------------------------
-- 4. 國指主連 10 檔擺盤 (Order Book)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hhi_order_book (
    id                  BIGSERIAL PRIMARY KEY,
    code                TEXT          NOT NULL,
    svr_recv_time_bid   TIMESTAMPTZ   NOT NULL,
    bid1_price          NUMERIC(12,4),
    bid1_volume         BIGINT,
    ask1_price          NUMERIC(12,4),
    ask1_volume         BIGINT,
    bid_levels          SMALLINT      NOT NULL,
    ask_levels          SMALLINT      NOT NULL,
    received_at         TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    raw_payload         JSONB         NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_hhi_order_book_svr_recv_time
    ON hhi_order_book (svr_recv_time_bid DESC);
CREATE INDEX IF NOT EXISTS idx_hhi_order_book_received_at
    ON hhi_order_book (received_at DESC);

-- =====================================================================
-- 常用查詢示例
-- =====================================================================
-- 1. 拉某個時段的 ticker：直接走 trade_time 索引
-- SELECT * FROM hsi_ticker
-- WHERE trade_time BETWEEN '2025-04-07 09:30+08' AND '2025-04-07 12:00+08'
-- ORDER BY trade_time;
--
-- 2. 從 raw_payload 取出 sequence 做去重（如果有需要）：
-- SELECT DISTINCT ON ((raw_payload->>'sequence')::bigint) *
-- FROM hsi_ticker WHERE trade_time > now() - interval '1 day';
--
-- 3. 取最佳買賣價/量做 spread 分析：
-- SELECT svr_recv_time_bid, ask1_price - bid1_price AS spread
-- FROM hsi_order_book WHERE bid_levels = 10 AND ask_levels = 10;
--
-- 4. 從 raw_payload 取出第二檔（0-indexed）：
-- SELECT raw_payload->'Bid'->1 FROM hsi_order_book LIMIT 1;
--   -> [167.50, 200, 3, {}]   (price, volume, order_num, detail)
