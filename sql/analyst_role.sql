-- =====================================================================
-- Phase 1：分析部門存取 — Read-only User + VIEW 抽象層
--
-- 設計目標
--   1. 分析師看到「扁平、乾淨」的欄位，不用學 JSONB 語法
--   2. 分析師只能透過 VIEW 查詢，無法直接 SELECT 實體表
--   3. Phase 2 切 Parquet/external table 時 VIEW 介面保留，分析師 query 不用改
--
-- 在 Supabase SQL Editor 執行（按順序）
-- =====================================================================

-- ---------------------------------------------------------------------
-- 1. 建立 read-only role
-- ---------------------------------------------------------------------
-- 執行前：把 'CHANGE_ME_STRONG_PASSWORD' 換成 24+ 字元強密碼
-- 跑完後：把這段從 SQL Editor 清除，避免歷史殘留
-- ---------------------------------------------------------------------

CREATE ROLE analyst LOGIN PASSWORD 'CHANGE_ME_STRONG_PASSWORD';

GRANT CONNECT ON DATABASE postgres TO analyst;
GRANT USAGE ON SCHEMA public TO analyst;

-- 不直接給實體表的 SELECT 權限，避免分析師繞過 VIEW
-- (如果之前已經 grant 過，這邊不影響)


-- ---------------------------------------------------------------------
-- 2. 建立 VIEW（扁平展開 raw_payload）
-- ---------------------------------------------------------------------

-- 2a. Ticker views：把 raw_payload 的 sequence / turnover / type / push_data_type / name 扁平展開
CREATE OR REPLACE VIEW v_hsi_ticker AS
SELECT
    id,
    code,
    trade_time,
    price,
    volume,
    ticker_direction,
    (raw_payload->>'sequence')::bigint        AS sequence,
    (raw_payload->>'turnover')::numeric       AS turnover,
    raw_payload->>'type'                      AS ticker_type,
    raw_payload->>'push_data_type'            AS push_data_type,
    raw_payload->>'name'                      AS name,
    received_at
FROM hsi_ticker;

CREATE OR REPLACE VIEW v_hhi_ticker AS
SELECT
    id,
    code,
    trade_time,
    price,
    volume,
    ticker_direction,
    (raw_payload->>'sequence')::bigint        AS sequence,
    (raw_payload->>'turnover')::numeric       AS turnover,
    raw_payload->>'type'                      AS ticker_type,
    raw_payload->>'push_data_type'            AS push_data_type,
    raw_payload->>'name'                      AS name,
    received_at
FROM hhi_ticker;

-- 2b. Order book views：除了 best bid/ask 還展開 2-10 檔
--     注意：每檔在 raw_payload 是 [price, volume, order_num, detail_dict]
--     用 jsonb 路徑取值，型別轉換為 numeric / bigint
CREATE OR REPLACE VIEW v_hsi_order_book AS
SELECT
    id,
    code,
    svr_recv_time_bid,
    -- best bid/ask 已經有獨立欄位
    bid1_price, bid1_volume,
    ask1_price, ask1_volume,
    -- 第 2-10 檔從 raw_payload 取
    (raw_payload->'Bid'->1->>0)::numeric  AS bid2_price,
    (raw_payload->'Bid'->1->>1)::bigint   AS bid2_volume,
    (raw_payload->'Bid'->2->>0)::numeric  AS bid3_price,
    (raw_payload->'Bid'->2->>1)::bigint   AS bid3_volume,
    (raw_payload->'Bid'->3->>0)::numeric  AS bid4_price,
    (raw_payload->'Bid'->3->>1)::bigint   AS bid4_volume,
    (raw_payload->'Bid'->4->>0)::numeric  AS bid5_price,
    (raw_payload->'Bid'->4->>1)::bigint   AS bid5_volume,
    (raw_payload->'Bid'->5->>0)::numeric  AS bid6_price,
    (raw_payload->'Bid'->5->>1)::bigint   AS bid6_volume,
    (raw_payload->'Bid'->6->>0)::numeric  AS bid7_price,
    (raw_payload->'Bid'->6->>1)::bigint   AS bid7_volume,
    (raw_payload->'Bid'->7->>0)::numeric  AS bid8_price,
    (raw_payload->'Bid'->7->>1)::bigint   AS bid8_volume,
    (raw_payload->'Bid'->8->>0)::numeric  AS bid9_price,
    (raw_payload->'Bid'->8->>1)::bigint   AS bid9_volume,
    (raw_payload->'Bid'->9->>0)::numeric  AS bid10_price,
    (raw_payload->'Bid'->9->>1)::bigint   AS bid10_volume,
    (raw_payload->'Ask'->1->>0)::numeric  AS ask2_price,
    (raw_payload->'Ask'->1->>1)::bigint   AS ask2_volume,
    (raw_payload->'Ask'->2->>0)::numeric  AS ask3_price,
    (raw_payload->'Ask'->2->>1)::bigint   AS ask3_volume,
    (raw_payload->'Ask'->3->>0)::numeric  AS ask4_price,
    (raw_payload->'Ask'->3->>1)::bigint   AS ask4_volume,
    (raw_payload->'Ask'->4->>0)::numeric  AS ask5_price,
    (raw_payload->'Ask'->4->>1)::bigint   AS ask5_volume,
    (raw_payload->'Ask'->5->>0)::numeric  AS ask6_price,
    (raw_payload->'Ask'->5->>1)::bigint   AS ask6_volume,
    (raw_payload->'Ask'->6->>0)::numeric  AS ask7_price,
    (raw_payload->'Ask'->6->>1)::bigint   AS ask7_volume,
    (raw_payload->'Ask'->7->>0)::numeric  AS ask8_price,
    (raw_payload->'Ask'->7->>1)::bigint   AS ask8_volume,
    (raw_payload->'Ask'->8->>0)::numeric  AS ask9_price,
    (raw_payload->'Ask'->8->>1)::bigint   AS ask9_volume,
    (raw_payload->'Ask'->9->>0)::numeric  AS ask10_price,
    (raw_payload->'Ask'->9->>1)::bigint   AS ask10_volume,
    -- 完整性監控
    bid_levels,
    ask_levels,
    -- 衍生欄位（方便分析師直接用）
    (ask1_price - bid1_price)              AS spread,
    (ask1_price + bid1_price) / 2          AS mid_price,
    -- 伺服器收賣盤時間（從 raw_payload 取）
    NULLIF(raw_payload->>'svr_recv_time_ask', '')::timestamptz AS svr_recv_time_ask,
    received_at
FROM hsi_order_book;

CREATE OR REPLACE VIEW v_hhi_order_book AS
SELECT
    id,
    code,
    svr_recv_time_bid,
    bid1_price, bid1_volume,
    ask1_price, ask1_volume,
    (raw_payload->'Bid'->1->>0)::numeric  AS bid2_price,
    (raw_payload->'Bid'->1->>1)::bigint   AS bid2_volume,
    (raw_payload->'Bid'->2->>0)::numeric  AS bid3_price,
    (raw_payload->'Bid'->2->>1)::bigint   AS bid3_volume,
    (raw_payload->'Bid'->3->>0)::numeric  AS bid4_price,
    (raw_payload->'Bid'->3->>1)::bigint   AS bid4_volume,
    (raw_payload->'Bid'->4->>0)::numeric  AS bid5_price,
    (raw_payload->'Bid'->4->>1)::bigint   AS bid5_volume,
    (raw_payload->'Bid'->5->>0)::numeric  AS bid6_price,
    (raw_payload->'Bid'->5->>1)::bigint   AS bid6_volume,
    (raw_payload->'Bid'->6->>0)::numeric  AS bid7_price,
    (raw_payload->'Bid'->6->>1)::bigint   AS bid7_volume,
    (raw_payload->'Bid'->7->>0)::numeric  AS bid8_price,
    (raw_payload->'Bid'->7->>1)::bigint   AS bid8_volume,
    (raw_payload->'Bid'->8->>0)::numeric  AS bid9_price,
    (raw_payload->'Bid'->8->>1)::bigint   AS bid9_volume,
    (raw_payload->'Bid'->9->>0)::numeric  AS bid10_price,
    (raw_payload->'Bid'->9->>1)::bigint   AS bid10_volume,
    (raw_payload->'Ask'->1->>0)::numeric  AS ask2_price,
    (raw_payload->'Ask'->1->>1)::bigint   AS ask2_volume,
    (raw_payload->'Ask'->2->>0)::numeric  AS ask3_price,
    (raw_payload->'Ask'->2->>1)::bigint   AS ask3_volume,
    (raw_payload->'Ask'->3->>0)::numeric  AS ask4_price,
    (raw_payload->'Ask'->3->>1)::bigint   AS ask4_volume,
    (raw_payload->'Ask'->4->>0)::numeric  AS ask5_price,
    (raw_payload->'Ask'->4->>1)::bigint   AS ask5_volume,
    (raw_payload->'Ask'->5->>0)::numeric  AS ask6_price,
    (raw_payload->'Ask'->5->>1)::bigint   AS ask6_volume,
    (raw_payload->'Ask'->6->>0)::numeric  AS ask7_price,
    (raw_payload->'Ask'->6->>1)::bigint   AS ask7_volume,
    (raw_payload->'Ask'->7->>0)::numeric  AS ask8_price,
    (raw_payload->'Ask'->7->>1)::bigint   AS ask8_volume,
    (raw_payload->'Ask'->8->>0)::numeric  AS ask9_price,
    (raw_payload->'Ask'->8->>1)::bigint   AS ask9_volume,
    (raw_payload->'Ask'->9->>0)::numeric  AS ask10_price,
    (raw_payload->'Ask'->9->>1)::bigint   AS ask10_volume,
    bid_levels,
    ask_levels,
    (ask1_price - bid1_price)              AS spread,
    (ask1_price + bid1_price) / 2          AS mid_price,
    NULLIF(raw_payload->>'svr_recv_time_ask', '')::timestamptz AS svr_recv_time_ask,
    received_at
FROM hhi_order_book;


-- ---------------------------------------------------------------------
-- 3. 只 GRANT VIEW 的 SELECT 給 analyst
--    （analyst 沒有實體表的 SELECT 權限，無法繞過）
-- ---------------------------------------------------------------------
GRANT SELECT ON
    v_hsi_ticker,
    v_hhi_ticker,
    v_hsi_order_book,
    v_hhi_order_book
TO analyst;


-- ---------------------------------------------------------------------
-- 4. RLS：VIEW 預設 invoker 模式，會繼承當前 role 的權限
--    既然 analyst 沒有實體表 SELECT 權，VIEW 內部查實表會失敗
--    解法：把 VIEW 改成 SECURITY DEFINER（用建表者的權限執行）
-- ---------------------------------------------------------------------
ALTER VIEW v_hsi_ticker      SET (security_invoker = false);
ALTER VIEW v_hhi_ticker      SET (security_invoker = false);
ALTER VIEW v_hsi_order_book  SET (security_invoker = false);
ALTER VIEW v_hhi_order_book  SET (security_invoker = false);
-- security_invoker = false 即 SECURITY DEFINER 的等價語法 (PG15+)
-- 表示 VIEW 用 owner (postgres) 的權限去讀底層表，繞過 analyst 的限制


-- ---------------------------------------------------------------------
-- 5. 驗證
-- ---------------------------------------------------------------------
-- 列出 analyst 能看到什麼
SELECT table_schema, table_name, privilege_type
FROM information_schema.role_table_grants
WHERE grantee = 'analyst'
ORDER BY table_name;

-- 預期輸出：4 個 VIEW 各有 SELECT 權限，沒有實體表


-- =====================================================================
-- 給分析部門的 connection 範本（複製整段給他們）
-- =====================================================================
--
-- Host:     db.zxwpluhizkcgwinoftje.supabase.co  (從 Project Settings → Database 取)
-- Port:     5432   (直連) 或 6543 (pooler，推薦給分析腳本)
-- Database: postgres
-- User:     analyst
-- Password: <你剛設的密碼>
-- SSL:      required
--
-- pooler 用法：user 改成 analyst.<project-ref>，host 改成 aws-x-region.pooler.supabase.com
--
-- 可查詢的 VIEW：
--   v_hsi_ticker        恆指主連逐筆
--   v_hhi_ticker        國指主連逐筆
--   v_hsi_order_book    恆指主連 10 檔擺盤（已展平）
--   v_hhi_order_book    國指主連 10 檔擺盤（已展平）
--
-- =====================================================================
-- 緊急回收
-- =====================================================================
-- DROP ROLE analyst;
--
-- 或暫時禁用：
-- ALTER ROLE analyst NOLOGIN;
