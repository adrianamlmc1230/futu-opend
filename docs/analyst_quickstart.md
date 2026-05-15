# 分析部門 — 港期 LV2 行情資料庫快速上手

## 1. 連線資訊

從 IT 拿到：
- Host
- Port (5432 直連 / 6543 pooler)
- Database: `postgres`
- User: `analyst`（read-only）
- Password
- SSL: required

完整 connection string 範例：
```
postgresql://analyst:<password>@db.<project-ref>.supabase.co:5432/postgres?sslmode=require
```

## 2. 4 張資料表

| 表 | 內容 | 主時間欄位 |
| --- | --- | --- |
| `hsi_ticker` | 恆指主連逐筆成交 | `trade_time` |
| `hhi_ticker` | 國指主連逐筆成交 | `trade_time` |
| `hsi_order_book` | 恆指主連 10 檔擺盤 | `svr_recv_time_bid` |
| `hhi_order_book` | 國指主連 10 檔擺盤 | `svr_recv_time_bid` |

### 每張表的核心欄位
- `code`、`received_at`、`raw_payload (JSONB)` 為共用
- ticker 多了 `price`、`volume`、`ticker_direction`
- order_book 多了 `bid1_price/volume`、`ask1_price/volume`、`bid_levels`、`ask_levels`
- 完整欄位（如 ticker.sequence、ticker.turnover、order_book 的 10 檔資料）放在 `raw_payload`

## 3. Python pandas 範例

```python
import pandas as pd
from sqlalchemy import create_engine

DSN = "postgresql://analyst:<password>@db.<project-ref>.supabase.co:5432/postgres?sslmode=require"
engine = create_engine(DSN)

# 例 1：拉一小時的 ticker
df = pd.read_sql("""
    SELECT trade_time, price, volume, ticker_direction, raw_payload->>'sequence' AS seq
    FROM hsi_ticker
    WHERE trade_time > now() - interval '1 hour'
    ORDER BY trade_time
""", engine)

# 例 2：算 1 分鐘 K 線
df.set_index("trade_time", inplace=True)
ohlc = df["price"].resample("1min").ohlc()
vol = df["volume"].resample("1min").sum()

# 例 3：擺盤展開 10 檔（從 raw_payload 取）
ob = pd.read_sql("""
    SELECT svr_recv_time_bid,
           (raw_payload->'Bid'->0->>0)::numeric AS bid1_price,
           (raw_payload->'Bid'->0->>1)::numeric AS bid1_volume,
           (raw_payload->'Bid'->1->>0)::numeric AS bid2_price,
           (raw_payload->'Bid'->1->>1)::numeric AS bid2_volume,
           (raw_payload->'Ask'->0->>0)::numeric AS ask1_price,
           (raw_payload->'Ask'->0->>1)::numeric AS ask1_volume
    FROM hsi_order_book
    WHERE svr_recv_time_bid > now() - interval '5 minutes'
    ORDER BY svr_recv_time_bid
""", engine)

# 例 4：算 spread 與 mid price
spread_df = pd.read_sql("""
    SELECT svr_recv_time_bid AS ts,
           ask1_price - bid1_price AS spread,
           (ask1_price + bid1_price) / 2 AS mid
    FROM hsi_order_book
    WHERE bid_levels = 10 AND ask_levels = 10
      AND svr_recv_time_bid > now() - interval '1 hour'
""", engine)
```

## 4. 重要 SQL 訣竅

### 從 raw_payload 取欄位
```sql
-- ticker 的 sequence (bigint)
SELECT (raw_payload->>'sequence')::bigint FROM hsi_ticker;

-- ticker 的 turnover (numeric)
SELECT (raw_payload->>'turnover')::numeric FROM hsi_ticker;

-- 第 N 檔擺盤 (0-indexed)
SELECT raw_payload->'Bid'->2 FROM hsi_order_book;
-- → [167.50, 200, 3, {"orderid1": 100, ...}]
```

### 用 `DISTINCT ON` 去重 ticker
連線重連會補推最近 50 筆，可能有重複：
```sql
SELECT DISTINCT ON ((raw_payload->>'sequence')::bigint) *
FROM hsi_ticker
WHERE trade_time > now() - interval '1 day'
ORDER BY (raw_payload->>'sequence')::bigint, received_at;
```

### 篩完整快照
擺盤偶爾會少於 10 檔（市場開盤前 / 流動性低），分析時記得過濾：
```sql
WHERE bid_levels = 10 AND ask_levels = 10
```

## 5. 時區提醒

- `trade_time` / `svr_recv_time_bid` 是 **HK 時間 (+08:00)**
- `received_at` 是 **UTC**
- pandas 預設無時區，比較時要 localize 或全部轉同一時區

```python
df["trade_time"] = pd.to_datetime(df["trade_time"]).dt.tz_convert("Asia/Hong_Kong")
```

## 6. 限制與注意

- **read-only**：你只能 `SELECT`，不能 `INSERT` / `UPDATE` / `DELETE`
- **連線數有限**：請用 connection pooler（host port 改 6543）或 `engine.dispose()` 關連線
- **大查詢**：超過幾百萬筆的 query 建議加 `LIMIT` 或 `WHERE trade_time` 切時間窗
- **資料延遲**：~1–3 秒（Futu 推送 → VPS → Supabase）
- **交易時段才有資料**：HK 期指日盤 09:15–16:30、夜期 17:15–次日 03:00

## 7. 有問題找誰
- 帳號 / 連線：IT
- 資料品質 / 欄位語意：本文件 + `PROJECT-BRIEF.md`
- 缺資料 / 異常：IT 看 collector log
