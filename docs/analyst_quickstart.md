# 港期 LV2 行情資料庫 — 分析部門使用手冊 (Phase 1)

> 給第一次接觸這個資料庫的同事。完整跟著走，10 分鐘內可以拉出第一筆資料。

---

## 1. 你會拿到什麼

IT 會給你三樣東西：

1. **Connection string**（一串網址，含密碼）
   ```
   postgresql://analyst:<密碼>@db.<project-ref>.supabase.co:5432/postgres?sslmode=require
   ```
2. 本文件
3. 一句話：「read-only，只能查 4 個 v_ 開頭的 view」

連線資訊請保管好——這串密碼會直接連到資料庫，不要貼到 chat、不要進 git。

---

## 2. 你能做什麼 vs 不能做什麼

| 動作 | 可以？ |
| --- | --- |
| `SELECT` 任何 4 個 VIEW | ✅ |
| `SELECT` 4 張底層實體表（`hsi_ticker` 等）| ✅（但不推薦，欄位混 JSONB 不好用）|
| `INSERT` / `UPDATE` / `DELETE` | ❌ |
| 建表 / 改 schema | ❌ |
| 看其他資料庫 / Supabase 設定 | ❌ |

---

## 3. 你能查的 4 個 VIEW

| VIEW | 內容 | 主時間欄位 |
| --- | --- | --- |
| `v_hsi_ticker` | 恆指主連逐筆成交 | `trade_time` |
| `v_hhi_ticker` | 國指主連逐筆成交 | `trade_time` |
| `v_hsi_order_book` | 恆指主連 10 檔擺盤（已展平）| `svr_recv_time_bid` |
| `v_hhi_order_book` | 國指主連 10 檔擺盤（已展平）| `svr_recv_time_bid` |

### Ticker VIEW 欄位（每筆成交）
| 欄位 | 型別 | 範例 | 說明 |
| --- | --- | --- | --- |
| `id` | bigint | `12345` | 主鍵 |
| `code` | text | `HK.HSImain` | 合約代碼 |
| `trade_time` | timestamptz | `2026-05-15 13:36:03+08` | 交易所成交時間 |
| `price` | numeric | `19500.0` | 成交價 |
| `volume` | bigint | `5` | 成交張數 |
| `ticker_direction` | text | `BUY` | `BUY` / `SELL` / `NEUTRAL` |
| `sequence` | bigint | `7460924…` | Futu 逐筆序號（去重用）|
| `turnover` | numeric | `97500.0` | 成交金額 |
| `ticker_type` | text | `AUTO_MATCH` | 撮合類型 |
| `push_data_type` | text | `REALTIME` | `REALTIME` / `CACHE`（補推用）|
| `name` | text | `恒生指数主连` | 合約名稱 |
| `received_at` | timestamptz | UTC | 系統收到並寫入的時間（量延遲用）|

### Order Book VIEW 欄位（每筆 10 檔快照）
| 欄位 | 型別 | 說明 |
| --- | --- | --- |
| `id` | bigint | 主鍵 |
| `code` | text | 合約代碼 |
| `svr_recv_time_bid` | timestamptz | Futu 伺服器收到買盤時間 |
| `svr_recv_time_ask` | timestamptz | 收到賣盤時間（可能 null）|
| `bid1_price` ~ `bid10_price` | numeric | 第 1 ~ 10 檔買價 |
| `bid1_volume` ~ `bid10_volume` | bigint | 第 1 ~ 10 檔買量 |
| `ask1_price` ~ `ask10_price` | numeric | 第 1 ~ 10 檔賣價 |
| `ask1_volume` ~ `ask10_volume` | bigint | 第 1 ~ 10 檔賣量 |
| `bid_levels` / `ask_levels` | smallint | 實際檔位數（理想都是 10） |
| `spread` | numeric | `ask1_price - bid1_price`（VIEW 已算好）|
| `mid_price` | numeric | `(ask1 + bid1) / 2`（VIEW 已算好）|
| `received_at` | timestamptz | UTC，量延遲 |

---

## 4. 怎麼連 — 三條路任選

### 路線 A：DBeaver / TablePlus / pgAdmin（圖形化，最簡單）

1. 下載 [DBeaver Community](https://dbeaver.io/download/)（免費）
2. 開啟 → `Database` → `New Database Connection` → 選 PostgreSQL
3. 填入：
   - **Host**：`db.<project-ref>.supabase.co`（從 connection string 找）
   - **Port**：`5432`
   - **Database**：`postgres`
   - **Username**：`analyst`
   - **Password**：你拿到的密碼
4. **Driver properties** 加一條：
   - `sslmode` = `require`
5. Test Connection → Finish
6. 左側 schema 樹找 `public` → `Views` → 雙擊 `v_hsi_ticker` → SQL Editor 寫查詢

### 路線 B：Python + pandas（程式化分析）

```bash
pip install psycopg2-binary pandas sqlalchemy
```

```python
import pandas as pd
from sqlalchemy import create_engine

# 把 <密碼> 和 <project-ref> 換成 IT 給你的值
DSN = "postgresql://analyst:<密碼>@db.<project-ref>.supabase.co:5432/postgres?sslmode=require"
engine = create_engine(DSN)

df = pd.read_sql("""
    SELECT trade_time, price, volume, ticker_direction
    FROM v_hsi_ticker
    WHERE trade_time > now() - interval '10 minutes'
    ORDER BY trade_time
""", engine)

print(df.head())
print(f"撈到 {len(df)} 筆")
```

### 路線 C：直接用 Supabase Dashboard SQL Editor

如果 IT 開了 Supabase 帳號給你，直接到 SQL Editor 寫 SQL 就好——不需要密碼，不需要安裝任何東西。

---

## 5. 上手範例（直接貼進去跑）

### 例 1：看最近 10 分鐘的 ticker 數量
```sql
SELECT code, count(*) AS ticks
FROM v_hsi_ticker
WHERE trade_time > now() - interval '10 minutes'
GROUP BY code;
```

### 例 2：算 1 分鐘 OHLC（Python）
```python
df = pd.read_sql("""
    SELECT trade_time, price, volume
    FROM v_hsi_ticker
    WHERE trade_time > now() - interval '1 hour'
    ORDER BY trade_time
""", engine)

df["trade_time"] = pd.to_datetime(df["trade_time"]).dt.tz_convert("Asia/Hong_Kong")
df = df.set_index("trade_time")

ohlc = df["price"].resample("1min").ohlc()
vol  = df["volume"].resample("1min").sum().rename("volume")
result = ohlc.join(vol)
print(result.tail(10))
```

### 例 3：看擺盤 spread 的時間序列
```sql
SELECT
    svr_recv_time_bid AS ts,
    bid1_price,
    ask1_price,
    spread,
    mid_price
FROM v_hsi_order_book
WHERE bid_levels = 10 AND ask_levels = 10              -- 過濾不完整快照
  AND svr_recv_time_bid > now() - interval '5 minutes'
ORDER BY svr_recv_time_bid;
```

### 例 4：取出 10 檔擺盤展開成寬表
```sql
SELECT
    svr_recv_time_bid,
    -- bid 10 檔
    bid1_price, bid2_price, bid3_price, bid4_price, bid5_price,
    bid6_price, bid7_price, bid8_price, bid9_price, bid10_price,
    bid1_volume, bid2_volume, bid3_volume, bid4_volume, bid5_volume,
    bid6_volume, bid7_volume, bid8_volume, bid9_volume, bid10_volume,
    -- ask 10 檔
    ask1_price, ask2_price, ask3_price, ask4_price, ask5_price,
    ask6_price, ask7_price, ask8_price, ask9_price, ask10_price,
    ask1_volume, ask2_volume, ask3_volume, ask4_volume, ask5_volume,
    ask6_volume, ask7_volume, ask8_volume, ask9_volume, ask10_volume
FROM v_hsi_order_book
WHERE svr_recv_time_bid > now() - interval '1 minute'
ORDER BY svr_recv_time_bid DESC
LIMIT 100;
```

### 例 5：去重 ticker（避免連線重連時的補推資料造成重複）
```sql
SELECT DISTINCT ON (sequence) *
FROM v_hsi_ticker
WHERE trade_time > now() - interval '1 day'
ORDER BY sequence, received_at;
```

### 例 6：算每分鐘平均 spread
```sql
SELECT
    date_trunc('minute', svr_recv_time_bid) AS minute,
    avg(spread) AS avg_spread,
    avg(mid_price) AS avg_mid,
    count(*) AS snapshots
FROM v_hsi_order_book
WHERE bid_levels = 10 AND ask_levels = 10
  AND svr_recv_time_bid > now() - interval '1 hour'
GROUP BY minute
ORDER BY minute;
```

---

## 6. 重要訣竅 / 常見坑

### 6.1 寫 WHERE 一定加時間條件
這是 LV2 即時資料，每天百萬筆，不加時間條件會炸：
```sql
-- ❌ 會跑很久
SELECT * FROM v_hsi_ticker;

-- ✅ 加時間窗
SELECT * FROM v_hsi_ticker WHERE trade_time > now() - interval '1 hour';
```

### 6.2 篩完整擺盤
擺盤偶爾會少於 10 檔（市場開盤前 / 流動性低）。算 spread 時記得過濾：
```sql
WHERE bid_levels = 10 AND ask_levels = 10
```

### 6.3 時區
- `trade_time` / `svr_recv_time_bid` 是 **HK +08:00**
- `received_at` 是 **UTC**（用來量採集延遲）
- pandas 讀進來預設無時區，比對前要 localize：
```python
df["trade_time"] = pd.to_datetime(df["trade_time"]).dt.tz_convert("Asia/Hong_Kong")
```

### 6.4 連線池
程式如果跑很久（例如 Jupyter），用完關掉：
```python
engine.dispose()
```
或把 port 改成 `6543`（Supabase pooler，自動管理連線）。

### 6.5 大查詢
百萬筆級別的 query 建議：
- 限制時間窗：`WHERE trade_time BETWEEN ... AND ...`
- 加 `LIMIT 10000` 先看樣本
- 用 `count(*)` 先估筆數再決定要不要全拉

---

## 7. 交易時段 / 沒資料的時段

港股期指有兩個交易時段：

| 時段 | HK 時間 |
| --- | --- |
| 日盤 | 09:15 – 12:00、13:00 – 16:30 |
| 夜期 | 17:15 – 次日 03:00 |

**非交易時段不會有新 row**——這不是壞掉，是市場休息。週末整天都不會有資料。

資料延遲：~1–3 秒（Futu 推送 → 我們的 VPS → Supabase）。

---

## 8. 故障排除

| 症狀 | 可能原因 | 處理 |
| --- | --- | --- |
| 連不上 | 網路問題 / VPN 影響 | 試試其他網路 |
| `password authentication failed` | 密碼錯了 | 找 IT 確認 |
| `permission denied for relation xxx` | 你查到沒授權的物件 | 只能查 4 個 v_ VIEW |
| 一直跑不完 | 沒加時間條件 | 加 `WHERE trade_time > ...` |
| 資料突然斷掉 | 可能是市場休息或 IT 在維護 | 看時間，或聯絡 IT |
| pandas 讀回來時間欄位看起來怪怪的 | 沒處理時區 | 用 `.dt.tz_convert("Asia/Hong_Kong")` |

---

## 9. 找誰

| 問題類別 | 找誰 |
| --- | --- |
| 連線 / 帳號 / 密碼忘了 | IT |
| 欄位語意 / 資料表結構 | 本文件 + GitHub repo 的 `PROJECT-BRIEF.md` |
| 缺資料 / 延遲異常 / 服務中斷 | IT 看 collector log |
| 想要的查詢效能太慢 | 跟 IT 反映；累積到一定量會啟動 Phase 2（Parquet）|

---

## 10. Phase 2 預告

當資料累積夠多、Phase 1 開始拖慢分析時，IT 會把歷史資料搬到 Parquet 檔案，但對你**完全透明**——你的 SQL 不用改，VIEW 後面會自動處理熱資料 + 冷資料的合併查詢。

詳情看 GitHub repo 的 `docs/phase2_parquet_plan.md`。
