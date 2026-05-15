-- =====================================================================
-- Phase 1 Step 2：建立 VIEW + 授權（不含密碼，可重複執行）
--
-- 在 Supabase SQL Editor 執行
-- 前置條件：已先執行 CREATE ROLE analyst LOGIN PASSWORD '...';
--
-- 設計：
--   - VIEW 用預設 INVOKER 模式（不會觸發 Supabase Security Advisor 警告）
--   - 所以同時也要 GRANT SELECT 給 analyst 在 4 張實表上
--   - 文件只教 analyst 用 VIEW，不會主動去碰實表（也沒理由用，raw_payload 是 JSONB）
-- =====================================================================

-- ---------------------------------------------------------------------
-- 1. Ticker views (扁平化 raw_payload)
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW v_hsi_ticker AS
SELECT
    id, code, trade_time, price, volume, ticker_direction,
    (raw_payload->>'sequence')::bigint        AS sequence,
    (raw_payload->>'turnover')::numeric       AS turnover,
    raw_payload->>'type'                      AS ticker_type,
    raw_payload->>'push_data_type'            AS push_data_type,
    raw_payload->>'name'                      AS name,
    received_at
FROM hsi_ticker;

CREATE OR REPLACE VIEW v_hhi_ticker AS
SELECT
    id, code, trade_time, price, volume, ticker_direction,
    (raw_payload->>'sequence')::bigint        AS sequence,
    (raw_payload->>'turnover')::numeric       AS turnover,
    raw_payload->>'type'                      AS ticker_type,
    raw_payload->>'push_data_type'            AS push_data_type,
    raw_payload->>'name'                      AS name,
    received_at
FROM hhi_ticker;

-- ---------------------------------------------------------------------
-- 2. Order book views (1-10 檔展開 + spread + mid_price)
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW v_hsi_order_book AS
SELECT
    id, code, svr_recv_time_bid,
    bid1_price, bid1_volume, ask1_price, ask1_volume,
    (raw_payload->'Bid'->1->>0)::numeric  AS bid2_price,
    (raw_payload->'Bid'->1->>1)::numeric::bigint   AS bid2_volume,
    (raw_payload->'Bid'->2->>0)::numeric  AS bid3_price,
    (raw_payload->'Bid'->2->>1)::numeric::bigint   AS bid3_volume,
    (raw_payload->'Bid'->3->>0)::numeric  AS bid4_price,
    (raw_payload->'Bid'->3->>1)::numeric::bigint   AS bid4_volume,
    (raw_payload->'Bid'->4->>0)::numeric  AS bid5_price,
    (raw_payload->'Bid'->4->>1)::numeric::bigint   AS bid5_volume,
    (raw_payload->'Bid'->5->>0)::numeric  AS bid6_price,
    (raw_payload->'Bid'->5->>1)::numeric::bigint   AS bid6_volume,
    (raw_payload->'Bid'->6->>0)::numeric  AS bid7_price,
    (raw_payload->'Bid'->6->>1)::numeric::bigint   AS bid7_volume,
    (raw_payload->'Bid'->7->>0)::numeric  AS bid8_price,
    (raw_payload->'Bid'->7->>1)::numeric::bigint   AS bid8_volume,
    (raw_payload->'Bid'->8->>0)::numeric  AS bid9_price,
    (raw_payload->'Bid'->8->>1)::numeric::bigint   AS bid9_volume,
    (raw_payload->'Bid'->9->>0)::numeric  AS bid10_price,
    (raw_payload->'Bid'->9->>1)::numeric::bigint   AS bid10_volume,
    (raw_payload->'Ask'->1->>0)::numeric  AS ask2_price,
    (raw_payload->'Ask'->1->>1)::numeric::bigint   AS ask2_volume,
    (raw_payload->'Ask'->2->>0)::numeric  AS ask3_price,
    (raw_payload->'Ask'->2->>1)::numeric::bigint   AS ask3_volume,
    (raw_payload->'Ask'->3->>0)::numeric  AS ask4_price,
    (raw_payload->'Ask'->3->>1)::numeric::bigint   AS ask4_volume,
    (raw_payload->'Ask'->4->>0)::numeric  AS ask5_price,
    (raw_payload->'Ask'->4->>1)::numeric::bigint   AS ask5_volume,
    (raw_payload->'Ask'->5->>0)::numeric  AS ask6_price,
    (raw_payload->'Ask'->5->>1)::numeric::bigint   AS ask6_volume,
    (raw_payload->'Ask'->6->>0)::numeric  AS ask7_price,
    (raw_payload->'Ask'->6->>1)::numeric::bigint   AS ask7_volume,
    (raw_payload->'Ask'->7->>0)::numeric  AS ask8_price,
    (raw_payload->'Ask'->7->>1)::numeric::bigint   AS ask8_volume,
    (raw_payload->'Ask'->8->>0)::numeric  AS ask9_price,
    (raw_payload->'Ask'->8->>1)::numeric::bigint   AS ask9_volume,
    (raw_payload->'Ask'->9->>0)::numeric  AS ask10_price,
    (raw_payload->'Ask'->9->>1)::numeric::bigint   AS ask10_volume,
    bid_levels, ask_levels,
    (ask1_price - bid1_price)              AS spread,
    (ask1_price + bid1_price) / 2          AS mid_price,
    NULLIF(raw_payload->>'svr_recv_time_ask', '')::timestamptz AS svr_recv_time_ask,
    received_at
FROM hsi_order_book;

CREATE OR REPLACE VIEW v_hhi_order_book AS
SELECT
    id, code, svr_recv_time_bid,
    bid1_price, bid1_volume, ask1_price, ask1_volume,
    (raw_payload->'Bid'->1->>0)::numeric  AS bid2_price,
    (raw_payload->'Bid'->1->>1)::numeric::bigint   AS bid2_volume,
    (raw_payload->'Bid'->2->>0)::numeric  AS bid3_price,
    (raw_payload->'Bid'->2->>1)::numeric::bigint   AS bid3_volume,
    (raw_payload->'Bid'->3->>0)::numeric  AS bid4_price,
    (raw_payload->'Bid'->3->>1)::numeric::bigint   AS bid4_volume,
    (raw_payload->'Bid'->4->>0)::numeric  AS bid5_price,
    (raw_payload->'Bid'->4->>1)::numeric::bigint   AS bid5_volume,
    (raw_payload->'Bid'->5->>0)::numeric  AS bid6_price,
    (raw_payload->'Bid'->5->>1)::numeric::bigint   AS bid6_volume,
    (raw_payload->'Bid'->6->>0)::numeric  AS bid7_price,
    (raw_payload->'Bid'->6->>1)::numeric::bigint   AS bid7_volume,
    (raw_payload->'Bid'->7->>0)::numeric  AS bid8_price,
    (raw_payload->'Bid'->7->>1)::numeric::bigint   AS bid8_volume,
    (raw_payload->'Bid'->8->>0)::numeric  AS bid9_price,
    (raw_payload->'Bid'->8->>1)::numeric::bigint   AS bid9_volume,
    (raw_payload->'Bid'->9->>0)::numeric  AS bid10_price,
    (raw_payload->'Bid'->9->>1)::numeric::bigint   AS bid10_volume,
    (raw_payload->'Ask'->1->>0)::numeric  AS ask2_price,
    (raw_payload->'Ask'->1->>1)::numeric::bigint   AS ask2_volume,
    (raw_payload->'Ask'->2->>0)::numeric  AS ask3_price,
    (raw_payload->'Ask'->2->>1)::numeric::bigint   AS ask3_volume,
    (raw_payload->'Ask'->3->>0)::numeric  AS ask4_price,
    (raw_payload->'Ask'->3->>1)::numeric::bigint   AS ask4_volume,
    (raw_payload->'Ask'->4->>0)::numeric  AS ask5_price,
    (raw_payload->'Ask'->4->>1)::numeric::bigint   AS ask5_volume,
    (raw_payload->'Ask'->5->>0)::numeric  AS ask6_price,
    (raw_payload->'Ask'->5->>1)::numeric::bigint   AS ask6_volume,
    (raw_payload->'Ask'->6->>0)::numeric  AS ask7_price,
    (raw_payload->'Ask'->6->>1)::numeric::bigint   AS ask7_volume,
    (raw_payload->'Ask'->7->>0)::numeric  AS ask8_price,
    (raw_payload->'Ask'->7->>1)::numeric::bigint   AS ask8_volume,
    (raw_payload->'Ask'->8->>0)::numeric  AS ask9_price,
    (raw_payload->'Ask'->8->>1)::numeric::bigint   AS ask9_volume,
    (raw_payload->'Ask'->9->>0)::numeric  AS ask10_price,
    (raw_payload->'Ask'->9->>1)::numeric::bigint   AS ask10_volume,
    bid_levels, ask_levels,
    (ask1_price - bid1_price)              AS spread,
    (ask1_price + bid1_price) / 2          AS mid_price,
    NULLIF(raw_payload->>'svr_recv_time_ask', '')::timestamptz AS svr_recv_time_ask,
    received_at
FROM hhi_order_book;

-- ---------------------------------------------------------------------
-- 3. 切回 SECURITY INVOKER（消除 Supabase Security Advisor 警告）
--    若先前曾設過 SECURITY DEFINER 才需要這段，新建專案可省略
-- ---------------------------------------------------------------------
ALTER VIEW v_hsi_ticker      SET (security_invoker = true);
ALTER VIEW v_hhi_ticker      SET (security_invoker = true);
ALTER VIEW v_hsi_order_book  SET (security_invoker = true);
ALTER VIEW v_hhi_order_book  SET (security_invoker = true);

-- ---------------------------------------------------------------------
-- 4. 授權：實表 + VIEW 都給 SELECT
--    (VIEW 用 INVOKER 模式，必須有底層表的 SELECT 權才能讀)
-- ---------------------------------------------------------------------
GRANT SELECT ON
    hsi_ticker, hhi_ticker, hsi_order_book, hhi_order_book,
    v_hsi_ticker, v_hhi_ticker, v_hsi_order_book, v_hhi_order_book
TO analyst;

-- ---------------------------------------------------------------------
-- 5. 驗證
-- ---------------------------------------------------------------------
SELECT table_name, privilege_type
FROM information_schema.role_table_grants
WHERE grantee = 'analyst'
ORDER BY table_name;

-- 預期看到 8 條 SELECT 紀錄（4 張表 + 4 個 VIEW）
