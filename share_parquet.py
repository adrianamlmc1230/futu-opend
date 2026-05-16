"""
share_parquet.py — 產生 R2 Parquet 的預簽名下載 URL

用法：
    python share_parquet.py --date 2026-05-15
    python share_parquet.py --date 2026-05-15 --table hsi_ticker
    python share_parquet.py --list   # 列出目前 R2 上所有檔
    python share_parquet.py --date 2026-05-15 --days 30   # URL 有效 30 天

預設 7 天過期。輸出可以直接 email / IM 給分析師。
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime

import boto3
from botocore.client import Config
from dotenv import load_dotenv

load_dotenv()

S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL")
S3_ACCESS_KEY_ID = os.getenv("S3_ACCESS_KEY_ID")
S3_SECRET_ACCESS_KEY = os.getenv("S3_SECRET_ACCESS_KEY")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

TABLES = ["hsi_ticker", "hhi_ticker", "hsi_order_book", "hhi_order_book"]


def make_client():
    if not all([S3_ENDPOINT_URL, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY, S3_BUCKET_NAME]):
        sys.exit("缺少 S3_* 環境變數")
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=S3_ACCESS_KEY_ID,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
        region_name="auto",
        config=Config(signature_version="s3v4"),
    )


def list_objects(s3):
    """列出 bucket 內所有 parquet。"""
    paginator = s3.get_paginator("list_objects_v2")
    items = []
    for page in paginator.paginate(Bucket=S3_BUCKET_NAME):
        for obj in page.get("Contents", []):
            items.append(obj)
    items.sort(key=lambda x: x["Key"])
    return items


def cmd_list(s3):
    items = list_objects(s3)
    if not items:
        print("(empty)")
        return
    total = 0
    for obj in items:
        size_mb = obj["Size"] / 1024 / 1024
        total += obj["Size"]
        print(f"  {obj['Key']:50s}  {size_mb:>8.2f} MB  {obj['LastModified']:%Y-%m-%d %H:%M}")
    print(f"\n  TOTAL: {len(items)} 個檔案 / {total/1024/1024:.2f} MB")


def cmd_share(s3, date_str: str, table: str | None, expires_days: int):
    """產生預簽名 URL（單張表或當天所有表）。"""
    expires_in = expires_days * 24 * 3600
    expire_at = datetime.fromtimestamp(
        datetime.now().timestamp() + expires_in
    )

    yyyy, mm, dd = date_str.split("-")
    targets = [table] if table else TABLES

    print(f"=== Parquet 下載連結（{expires_days} 天有效，{expire_at:%Y-%m-%d %H:%M} 過期）===\n")

    for t in targets:
        key = f"{t}/{yyyy}/{int(mm):02d}/{int(dd):02d}.parquet"
        try:
            head = s3.head_object(Bucket=S3_BUCKET_NAME, Key=key)
            size_mb = head["ContentLength"] / 1024 / 1024
        except Exception:
            print(f"## {t} ({date_str})\n   ❌ 檔案不存在於 R2\n")
            continue

        url = s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": S3_BUCKET_NAME, "Key": key},
            ExpiresIn=expires_in,
        )
        print(f"## {t}  {date_str}  ({size_mb:.2f} MB)")
        print(f"   wget -O '{t}_{date_str}.parquet' '{url}'\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", help="日期 YYYY-MM-DD（產 URL 模式）")
    ap.add_argument("--table", help="只產某張表的 URL（不指定則 4 張都產）",
                    choices=TABLES)
    ap.add_argument("--days", type=int, default=7, help="URL 有效天數（預設 7 天）")
    ap.add_argument("--list", action="store_true", help="列出 R2 上所有檔")
    args = ap.parse_args()

    s3 = make_client()
    if args.list:
        cmd_list(s3)
    elif args.date:
        cmd_share(s3, args.date, args.table, args.days)
    else:
        ap.print_help()


if __name__ == "__main__":
    main()
