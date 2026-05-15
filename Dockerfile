# =====================================================================
# HSI/HHI LV2 採集守護進程
# 部署環境：與 Futu OpenD 同主機 (Linux + host network)
# =====================================================================
FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    TZ=Asia/Hong_Kong

# 時區資料 (供 logging 顯示為 HKT)
RUN apt-get update \
    && apt-get install -y --no-install-recommends tzdata \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

CMD ["python", "main.py"]
