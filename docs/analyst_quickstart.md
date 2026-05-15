# 分析部門 — 港期 LV2 行情資料庫快速上手 (Phase 1)

## 1. 連線資訊

從 IT 拿到（透過密碼管理器分享）：
- Host
- Port (5432 直連 / 6543 pooler，推薦 6543)
- Database: `postgres`
- User: `analyst`（read-only）
- Password
- SSL: required

完整 connection string 範例：
```
postgresql://analyst:<password>@db.<project-ref>.supabase.co:5432/postgres?sslmode=require
```

## 2. 你能查的 4 個 VIEW

| VIEW | 內容 | 主要時間欄位 |
| --- | --- | --- |
| `v_hsi_ticker` | 恆指主連逐筆成交 | `trade_time` |
| `v_hhi_ticker` | 國指主連逐筆成交 | `trade_time` |
| `v_hsi_order_book` | 恆指主連 10 檔擺盤（已展平）| `svr_recv_time_bid` |
| `v_hhi_order_book` | 國指主連 10 檔擺盤（已展平）| `svr_recv_time_bid` |

> 你**只能** SELECT VIEW，不能 INSERT / UPDATE / DELETE，也不能直查底層實體表。
> 這是設計上的安全層，不是限制。

### Ticker VIEW 欄位
| 欄位 | 型別 | 說明 |
| --- | --- | --- |
| `id` | bigint | 主鍵 |
| `code` | text | `HK.HSImain` / `HK.HHImain` |
| `trade_time` | timestamptz | 交易所成交時間 (HK +08:00) |
| `price` | numeric | 成交價 |
| `volume` | bigint | 成交量 |
| `ticker_direction` | text | `BUY` / `SELL` / `NEUTRAL` |
| `sequence` | bigint | Futu 逐筆序號（去重用）|
| `turnover` | numeric | 成交金額 |
| `ticker_type` | text | `AUTO_MATCH` / `ODD_LOT` / ... |
| `push_data_type` | text | `REALTIME` / `CACHE`（補推可區分）|
| `name` | text | 合約名稱 |
| `received_at` | timestamptz | 寫入時間 (UTC，可量延遲) |

### Order Book VIEW 欄位
| 欄位 | 型別 | 說明 |
| --- | --- | --- |
| `id` | bigint | 主鍵 |
| `code` | text | 合約代碼 |
| `svr_recv_time_bid` | timestamptz | Futu 伺服器收到買盤時間 |
| `svr_recv_time_ask` | timestamptz | 收到賣盤時間（可為 null） |
| `bid1_price..bid10_price` | numeric | 第 1-10 檔買價 |
| `bid1_volume..bid10_volume` | bigint | 第 1-10 檔買量 |
| `ask1_price..ask10_price` | numeric | 第 1-10 檔賣價 |
| `ask1_volume..ask10_volume` | bigint | 第 1-10 檔賣量 |
| `bid_levels` / `ask_levels` | smallint | 實際檔位數（理想 10）|
| `spread` | numeric | `ask1_price - bid1_price` （已算好）|
| `mid_price` | numeric | `(ask1 + bid1) / 2` （已算好）|
| `received_at` | timestamptz | 寫入時間 (UTC) |

## 3. Python pandas 範例

```python
import pandas as pd
from sqlalchemy import create_engine

DSN = "postgresql://analyst:<password>@db.<project-ref>.supabase.co:5432/postgres?sslmode=require"
engine = create_engine(DSN)

# 例 1：拉一小時的 ticker
df = pd.read_sql("""
    SELECT trade_time, price, volume, ticker_direction, sequence, turnover
    FROM v_hsi_ticker
    WHERE trade_time > now() - interval '1 hour'
    ORDER BY trade_time
""", engine)

# 例 2：算 1 分鐘 OHLC
df = df.set_index("trade_time")
ohlc = df["price"].resample("1min").ohlc()
vol  = df["volume"].resample("1min").sum()

# 例 3：擺盤 10 檔已經是欄位，直接拉
ob = pd.read_sql("""
    SELECT svr_recv_time_bid, bid1_price, bid1_volume, ask1_price, ask1_volume,
           bid2_price, bid3_price, ask2_price, ask3_price,
           spread, mid_price
    FROM v_hsi_order_book
    WHERE bid_levels = 10 AND ask_levels = 10
      AND svr_recv_time_bid > now() - interval '5 minutes'
    ORDER BY svr_recv_time_bid
""", engine)

# 例 4：直接拿 spread 序列（VIEW 已經算好）
spread = pd.read_sql("""
    SELECT svr_recv_time_bid AS ts, spread, mid_price
    FROM v_hsi_order_book
    WHERE bid_levels = 10 AND ask_levels = 10
      AND svr_recv_time_bid BETWEEN '2026-05-15 09:15+08' AND '2026-05-15 16:30+08'
""", engine)

# 例 5：Ticker 序號去重（連線重連會補推最近 50 筆）
clean = pd.read_sql("""
    SELECT DISTINCT ON (sequence) *
    FROM v_hsi_ticker
    WHERE trade_time > now() - interval '1 day'
    ORDER BY sequence, received_at
""", engine)
```

## 4. 重要訣竅

### 篩完整擺盤
擺盤偶爾少於 10 檔（市場開盤前 / 流動性低），分析時記得過濾：
```sql
WHERE bid_levels = 10 AND ask_levels = 10
```

### 時區
- `trade_time` / `svr_recv_time_*` 是 **HK +08:00**
- `received_at` 是 **UTC**
- pandas 預設讀進來無時區資訊，比較時要 localize：
```python
df["trade_time"] = pd.to_datetime(df["trade_time"]).dt.tz_convert("Asia/Hong_Kong")
```

### 連線數
免費版 Supabase 並發連線有限：
- 推薦走 pooler（port 6543）
- 不用後 `engine.dispose()` 釋放
- Jupyter 不要把 engine 開一堆 cell

### 大查詢
別整張表 `SELECT *`，務必加時間窗：
```sql
WHERE trade_time > now() - interval 'X hours'
WHERE trade_time BETWEEN '...' AND '...'
```

## 5. 交易時段 / 資料延遲

- HK 期指日盤：**09:15 – 16:30**
- HK 期指夜期：**17:15 – 次日 03:00**
- 非交易時段不會有新 row（不是斷線，是市場休息）
- 資料延遲：~1–3 秒（Futu 推送 → VPS → Supabase）

## 6. 限制

- `analyst` role：**read-only**，只能查 4 個 VIEW
- 不能查實體表、不能改 schema、不能寫資料
- Phase 1 的查詢效能受 Supabase free tier 限制；Phase 2 將會搬到 Parquet/外部存儲

## 7. 找誰

- 連線 / 帳號：IT
- 欄位語意：本文件 + `PROJECT-BRIEF.md`（在 git repo）
- 資料異常 / 缺資料：IT 看 collector log
- Phase 2 計劃：上線一個月後啟動，會切到 Parquet + 增量 sync
