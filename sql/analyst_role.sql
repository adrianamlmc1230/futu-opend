-- =====================================================================
-- Phase 1：分析部門存取 — Read-only User + VIEW 抽象層
--
-- 設計（V2 — 切回 SECURITY INVOKER 消除 Supabase Security Advisor 警告）
--   1. analyst role：read-only login user
--   2. 4 個 VIEW：把 raw_payload 扁平展開成普通欄位
--   3. VIEW 用預設 INVOKER 模式（查詢者本身的權限去讀底層）
--   4. 所以同時 GRANT SELECT 給 analyst 在 4 張實表上；
--      文件只教 analyst 用 VIEW，不會主動去碰 raw_payload 實表
--
-- 在 Supabase SQL Editor 按順序執行
-- =====================================================================

-- ---------------------------------------------------------------------
-- 1. 建立 read-only role
--    執行前：把 'CHANGE_ME_STRONG_PASSWORD' 換成 24+ 字元強密碼
--    跑完後：把這段從 SQL Editor 視窗清除，避免歷史殘留
-- ---------------------------------------------------------------------
CREATE ROLE analyst LOGIN PASSWORD 'CHANGE_ME_STRONG_PASSWORD';

GRANT CONNECT ON DATABASE postgres TO analyst;
GRANT USAGE ON SCHEMA public TO analyst;


-- ---------------------------------------------------------------------
-- 2. Ticker views（扁平化 raw_payload）
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
-- 3. Order book views（1-10 檔展開 + spread + mid_price）
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
-- 4. 確保 VIEW 是 SECURITY INVOKER（消 Supabase 警告）
-- ---------------------------------------------------------------------
ALTER VIEW v_hsi_ticker      SET (security_invoker = true);
ALTER VIEW v_hhi_ticker      SET (security_invoker = true);
ALTER VIEW v_hsi_order_book  SET (security_invoker = true);
ALTER VIEW v_hhi_order_book  SET (security_invoker = true);


-- ---------------------------------------------------------------------
-- 5. 授權：實表 + VIEW 都給 SELECT（INVOKER 模式必須要實表權限）
-- ---------------------------------------------------------------------
GRANT SELECT ON
    hsi_ticker, hhi_ticker, hsi_order_book, hhi_order_book,
    v_hsi_ticker, v_hhi_ticker, v_hsi_order_book, v_hhi_order_book
TO analyst;


-- ---------------------------------------------------------------------
-- 6. 驗證
-- ---------------------------------------------------------------------
SELECT table_name, privilege_type
FROM information_schema.role_table_grants
WHERE grantee = 'analyst'
ORDER BY table_name;
-- 預期：8 條 SELECT 紀錄（4 張表 + 4 個 VIEW）


-- =====================================================================
-- Connection 範本給分析部
-- =====================================================================
-- postgresql://analyst:<密碼>@db.<project-ref>.supabase.co:5432/postgres?sslmode=require
--
-- 從 Supabase Dashboard → Project Settings → Database → Connection String 取
-- 把預設 user `postgres` 換成 `analyst` + 你設的密碼即可

-- =====================================================================
-- 緊急回收
-- =====================================================================
-- ALTER ROLE analyst NOLOGIN;          -- 暫時禁用
-- DROP ROLE analyst;                   -- 完全移除（自動清掉所有授權）
