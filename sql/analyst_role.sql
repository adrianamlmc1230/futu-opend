-- =====================================================================
-- 建立分析部門用的 read-only PostgreSQL 用戶
-- 在 Supabase SQL Editor 執行
--
-- 使用方法：
--   1. 把下方 'CHANGE_ME_STRONG_PASSWORD' 換成強密碼（24+ 字元）
--   2. 在 Supabase SQL Editor 執行整段
--   3. 把 connection string 給分析師（見最末段註解）
-- =====================================================================

-- 1. 建立 role
CREATE ROLE analyst LOGIN PASSWORD 'CHANGE_ME_STRONG_PASSWORD';

-- 2. 允許連線到資料庫
GRANT CONNECT ON DATABASE postgres TO analyst;

-- 3. 允許用 public schema
GRANT USAGE ON SCHEMA public TO analyst;

-- 4. 只給「我們的 4 張表」的 SELECT 權限（精準授權，不污染其他表）
GRANT SELECT ON
    public.hsi_ticker,
    public.hhi_ticker,
    public.hsi_order_book,
    public.hhi_order_book
TO analyst;

-- 5. 寫一條 RLS policy 讓 analyst 能繞過 RLS 讀（service_role 本來就 bypass，
--    我們現在多開一個白名單給 analyst）
DO $$
DECLARE
    t TEXT;
BEGIN
    FOREACH t IN ARRAY ARRAY['hsi_ticker', 'hhi_ticker', 'hsi_order_book', 'hhi_order_book']
    LOOP
        EXECUTE format(
            'CREATE POLICY analyst_read ON public.%I FOR SELECT TO analyst USING (true)',
            t
        );
    END LOOP;
END $$;

-- 驗證
SELECT
    schemaname, tablename, policyname, roles, cmd
FROM pg_policies
WHERE tablename IN ('hsi_ticker', 'hhi_ticker', 'hsi_order_book', 'hhi_order_book');

-- =====================================================================
-- 給分析部門的 connection 資訊（複製整段給他們）
-- =====================================================================
--
-- Host:     db.zxwpluhizkcgwinoftje.supabase.co
-- Port:     5432
-- Database: postgres
-- User:     analyst
-- Password: <你剛設的密碼>
--
-- 或用 pooler（避免長連線消耗 max_connections，分析腳本推薦走這個）：
-- Host:     aws-0-<region>.pooler.supabase.com
-- Port:     6543  (transaction mode) 或 5432 (session mode)
-- Database: postgres
-- User:     analyst.zxwpluhizkcgwinoftje
-- 規則：用戶名是 analyst.<project-ref>
--
-- 在 Supabase Dashboard → Project Settings → Database → Connection String
-- 找到實際的 host / pooler URL，把預設用戶 postgres 換成 analyst 即可
--
-- Python 範例：
--   import pandas as pd
--   conn = "postgresql://analyst:<password>@db.zxwpluhizkcgwinoftje.supabase.co:5432/postgres?sslmode=require"
--   df = pd.read_sql("SELECT * FROM hsi_ticker WHERE trade_time > now() - interval '1 hour'", conn)
--
-- =====================================================================
-- 緊急回收權限：
-- =====================================================================
-- DROP ROLE analyst;
-- (會自動移除所有授權；對應的 policies 自動消失)
