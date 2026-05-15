# Phase 2 計劃 — Parquet 增量轉存

## 觸發條件（任一達到即啟動 Phase 2）
1. Supabase 表體積 > 50 GB（接近 Pro tier 預設）
2. 分析部門查詢延遲 > 30 秒成為常態
3. 上線滿 30 天（time-based 強制檢視）

## 目標
- 把 7 天前的歷史資料從 PostgreSQL 搬到 Parquet
- Supabase 留近 7 天熱資料；歷史走 Parquet
- 對分析部門透明：他們的 SQL 不用改

## 架構草圖
```
寫入端（不變）
   collector → Supabase  (4 張實體表)

每日 03:30 (港期收盤後)
   增量 export job:
     SELECT ... WHERE trade_time IN [yesterday, yesterday+1day)
     → Parquet partitioned by date / code
     → 上傳 R2 / S3 / Supabase Storage
   完成後 delete 對應實體表 row

分析端
   v_hsi_ticker, v_hhi_ticker, v_hsi_order_book, v_hhi_order_book
   改成 UNION ALL：
     SELECT ... FROM hsi_ticker      WHERE trade_time > now() - interval '7 days'
     UNION ALL
     SELECT ... FROM read_parquet('s3://.../hsi_ticker/dt=*/data.parquet')

   分析師端 query 不需要改（VIEW 介面保持不變）
```

## 候選技術
- **Storage**：Cloudflare R2（free egress）/ S3 / Supabase Storage
- **Query Engine**：DuckDB 直接讀 Parquet / Postgres FDW
- **Orchestration**：cron in container / GitHub Actions / Supabase Scheduled Function
- **Format**：Parquet + ZSTD 壓縮，partition by `code, date`

## 待決定項
- [ ] 7 天保留期是否合適（看實際查詢模式）
- [ ] DuckDB vs PostgreSQL FDW（前者效能好、後者管理簡單）
- [ ] 分析部門能否接受冷資料 / 熱資料 query 速度差異
- [ ] 異常情況（轉存失敗、partition 不全）的告警

## 不在 Phase 2 範圍
- 即時資料管道改造（仍然是 Supabase 直寫）
- 分析部門查詢 API 化（如果他們需要才考慮 Phase 3）
