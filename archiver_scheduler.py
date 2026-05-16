"""
archiver_scheduler.py — 每天 04:00 HKT 觸發 data_archiver.run_once()

容器需設定 TZ=Asia/Hong_Kong（已在 docker-compose.yml 處理），
schedule 套件依當下 process local time 解讀 .at("04:00")。

防禦：
    - run_once() 失敗不會讓排程器掛掉
    - 啟動時不立即執行（除非 ARCHIVER_RUN_AT_STARTUP=1）
    - SIGINT / SIGTERM 優雅退出
"""
from __future__ import annotations

import logging
import os
import signal
import sys
import threading
import time
from datetime import datetime

import schedule
from dotenv import load_dotenv

import data_archiver

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("scheduler")

ARCHIVE_TIME = os.getenv("ARCHIVE_TIME", "04:00")  # HKT
RUN_AT_STARTUP = os.getenv("ARCHIVER_RUN_AT_STARTUP", "0") == "1"

shutdown_event = threading.Event()


def safe_run():
    """包一層 try，避免單次失敗讓 scheduler 掛掉。"""
    logger.info("觸發每日封存 (now=%s)", datetime.now())
    try:
        data_archiver.run_once()
    except Exception:
        logger.exception("封存流程例外（已捕獲，下個週期照常執行）")


def main():
    logger.info("=== Archiver Scheduler 啟動 ===")
    logger.info("排程：每天 %s 執行 data_archiver.run_once()", ARCHIVE_TIME)
    logger.info("時區：%s（依容器 TZ env，建議 Asia/Hong_Kong）",
                time.tzname)

    schedule.every().day.at(ARCHIVE_TIME).do(safe_run)

    if RUN_AT_STARTUP:
        logger.info("ARCHIVER_RUN_AT_STARTUP=1，啟動時立即執行一次")
        safe_run()

    def _handle_sig(sig, _f):
        logger.info("收到訊號 %s，準備退出", sig)
        shutdown_event.set()

    signal.signal(signal.SIGINT, _handle_sig)
    signal.signal(signal.SIGTERM, _handle_sig)

    # 主迴圈：每 30 秒檢查一次是否該觸發
    while not shutdown_event.is_set():
        schedule.run_pending()
        shutdown_event.wait(30)

    logger.info("=== Scheduler 已退出 ===")


if __name__ == "__main__":
    main()
