import os
import json
import random
import time
import requests
from kafka import KafkaProducer
from datetime import datetime, timedelta
from typing import List

# ================= 配置区域 =================
BINANCE_URL = "https://fapi.binance.com"
KAFKA_BOOTSTRAP_SERVERS = ["43.159.56.125:9092", "43.163.1.156:9092", "43.156.2.129:9092"]
KAFKA_TOPIC = "Funding_Rate_Binance"

# 限流参数（Binance：500次/5分钟 ≈ 每秒 1.6 次）
MAX_REQUESTS_PER_MIN = 80
REQUEST_INTERVAL = 60 / MAX_REQUESTS_PER_MIN
MAX_RETRY = 5  # 最大重试次数

# 保存进度的文件
PROGRESS_FILE = "funding_rate_progress.json"

# ============================================

producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        max_request_size=5 * 1024 * 1024,  # 5MB 限制单个消息大小
        batch_size=100000,  # 批次大小 100KB，适用于中等吞吐
        linger_ms=300,  # 适当延迟批量发送，提高吞吐
    )


def ms_timestamp(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def safe_request(url, params=None, max_retry=MAX_RETRY):
    """带有限流和退避机制的请求"""
    retry = 0
    while retry < max_retry:
        try:
            resp = requests.get(url, params=params, timeout=10)

            # 如果触发限流
            if resp.status_code in (418, 429):
                wait_time = (2 ** retry) + random.uniform(0, 1)  # 指数退避 + 抖动
                print(f"[WARN] Rate limit hit. Sleeping {wait_time:.2f}s...")
                time.sleep(wait_time)
                retry += 1
                continue

            resp.raise_for_status()
            return resp.json()

        except requests.RequestException as e:
            wait_time = (2 ** retry) + random.uniform(0, 1)
            print(f"[ERROR] Request failed: {e}, retrying in {wait_time:.2f}s")
            time.sleep(wait_time)
            retry += 1

    raise Exception(f"Max retries exceeded for URL: {url}")


def fetch_funding_rate(symbol, start_time, end_time, limit=1000):
    """获取某个币种的资金费率数据"""
    url = "https://fapi.binance.com/fapi/v1/fundingRate"
    params = {
        "symbol": symbol,
        "startTime": start_time,
        "endTime": end_time,
        "limit": limit
    }
    data = safe_request(url, params=params)

    # 控制请求频率
    time.sleep(REQUEST_INTERVAL)
    return data

def load_progress():
    """加载上次的时间进度"""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            return json.load(f)
    return {}


def save_progress(progress):
    """保存进度到文件"""
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2)


def push_to_kafka(results, symbol: str):
    """推送数据到 Kafka"""
    for item in results:
        msg = {
            "symbol": item["symbol"],
            "time": item["fundingTime"],
            "rate": item["fundingRate"] if item["fundingRate"] else 0,
            "mark_price": item["markPrice"] if item["markPrice"] else 0
        }
        producer.send(KAFKA_TOPIC, msg)

def fetch_historical_data(symbols: List[str]) -> None:
    """阶段一：历史数据补齐"""
    progress = load_progress()

    for symbol in symbols:
        print(f"开始获取 {symbol} 历史数据...")
        now_ms = ms_timestamp(datetime.utcnow())
        start_ms = progress.get(symbol, ms_timestamp(datetime(2022, 1, 1)))

        while start_ms < now_ms:
            results = fetch_funding_rate(symbol, start_ms, now_ms, limit=1000)
            # print(results)
            if not results:
                # 没有数据 → 跳过 8 小时
                start_ms += 8 * 60 * 60 * 1000
                progress[symbol] = start_ms
                save_progress(progress)
                print(f"{symbol} 在 {datetime.utcfromtimestamp(start_ms/1000)} UTC 没有数据，跳过")
                continue

            push_to_kafka(results, symbol)

            last_time = results[-1]["fundingTime"]
            progress[symbol] = last_time + 1
            save_progress(progress)
            # 下一次循环用更新后的 start_ms
            start_ms = progress[symbol]
            print(f"{symbol} 已获取至 {datetime.utcfromtimestamp(last_time/1000)} UTC")

            if last_time >= now_ms:
                break

        producer.flush()
        print(f"{symbol}币对的历史数据已推送完成 ✅")
    print("所有币对的历史数据已推送完成 ✅")


def run(symbols: List[str]):
    # 阶段一：历史补齐
    fetch_historical_data(symbols)

if __name__ == "__main__":
    symbols = ["SOLUSDC", "XRPUSDC", "DOGEUSDC", "1000PEPEUSDC", "SUIUSDC", "BNBUSDC", "ENAUSDC",
                          "TRUMPUSDC", "ETHFIUSDC", "PNUTUSDC", "BOMEUSDC", "KAITOUSDC", "ADAUSDC", "1000BONKUSDC",
                          "ORDIUSDC", "AVAXUSDC", "LINKUSDC", "LTCUSDC", "WLDUSDC", "FILUSDC", "ARBUSDC",
                          "1000SHIBUSDC", "CRVUSDC", "TIAUSDC", "NEARUSDC", "BCHUSDC", "HBARUSDC", "NEOUSDC",
               "IPUSDC",'BTCUSDT', "ETHUSDT", "XRPUSDT", "BNBUSDT", "SOLUSDT", "TRUMPUSDT", "DOGEUSDT", "ADAUSDT",
               "1000PEPEUSDT", 'LTCUSDT', 'TRXUSDT', 'SUIUSDT', 'XLMUSDT', 'HBARUSDT', 'AAVEUSDT', 'AVAXUSDT',
               'LINKUSDT', 'BCHUSDT', 'NEARUSDT', 'LAYERUSDT', 'PNUTUSDT', 'WIFUSDT', 'ENAUSDT', 'SUSDT', 'KAITOUSDT',
             'SOLVUSDT', 'AUCTIONUSDT', '1000SHIBUSDT', 'DOTUSDT', 'RUNEUSDT', 'TONUSDT']  # 可扩展
    # symbols = ["SOLUSDC"]
    run(symbols)
