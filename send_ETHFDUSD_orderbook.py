import json
import time
import threading
from datetime import datetime, timedelta
from decimal import Decimal

import pytz
import pandas as pd
import numpy
from kafka import KafkaProducer
from binance import ThreadedWebsocketManager
import signal
import sys

# ========== 配置区域 ==========
KAFKA_TOPIC = "ETHFDUSD_OrderBook"
KAFKA_SERVERS = ["43.159.56.125:9092", "43.163.1.156:9092", "43.156.2.129:9092"]
symbol = "ethfdusd"

api_key = ""
api_secret = ""

shanghai_tz = pytz.timezone("Asia/Shanghai")

RECONNECT_INTERVAL = 5   # 看门狗检测间隔（秒）
TIMEOUT_THRESHOLD = 30   # 超过该秒数未收到消息则判定掉线

# ========== Kafka Producer 配置 ==========
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    linger_ms=100,  # 批处理延迟
    batch_size=32768,
    acks='all',
    retries=10,
    retry_backoff_ms=1000,
    request_timeout_ms=60000,
    delivery_timeout_ms=120000,
    max_block_ms=60000,
)

# ========== 全局变量 ==========
twm = None
twm_lock = threading.Lock()
last_msg_time = time.time()
stop_event = threading.Event()


# ===== 时间解析 =====
def parse_timestamp(ts):
    if isinstance(ts, str):
        ts = int(ts)

    length = len(str(ts))
    epoch = datetime(1970, 1, 1)

    if length == 13:
        dt = epoch + timedelta(milliseconds=ts)
        timestamp_ms = ts
    elif length == 16:
        dt = epoch + timedelta(microseconds=ts)
        timestamp_ms = ts // 1000
    elif length == 19:
        dt = epoch + timedelta(microseconds=ts // 1000)
        timestamp_ms = ts // 1_000_000
    else:
        dt = epoch + timedelta(seconds=ts)
        timestamp_ms = ts * 1000

    dt_utc = dt.replace(tzinfo=pytz.utc)
    dt_shanghai = dt_utc.astimezone(shanghai_tz)

    return {
        "UTC": dt_utc.strftime('%Y-%m-%d %H:%M:%S.%f %Z'),
        "Shanghai": dt_shanghai.strftime("%Y-%m-%d"),
        "timestamp_ms": timestamp_ms
    }


# ===== 数据类型转换 =====
def convert_value(value):
    """转换数据类型：数值转换为 float，布尔值转换为 bool，字符串保持 str"""
    if isinstance(value, str):
        # 处理布尔值
        if value.lower() in ["true", "false"]:
            return value.lower() == "true"
        # 处理数值
        try:
            if "." in value or 'e' in value.lower():
                val = numpy.float64(value)
                return f"{val:.12f}".rstrip('0').rstrip('.')
            else:
                return int(value)
        except ValueError:
            return value  # 如果转换失败，保持原始字符串
    elif isinstance(value, Decimal):
        return numpy.float64(value)
    return value  # 其他类型保持不变


# ===== WebSocket 消息处理 =====
def handle_depth_socket(msg):
    global last_msg_time
    try:
        last_msg_time = time.time()

        ts = msg.get("E", int(time.time() * 1000))
        time_dict = parse_timestamp(ts)

        bids = pd.DataFrame(msg.get('bids', []), columns=["bid_price", "bid_qty"])
        asks = pd.DataFrame(msg.get('asks', []), columns=["ask_price", "ask_qty"])

        max_len = max(len(bids), len(asks))
        bids = bids.reindex(range(max_len)).fillna(0)
        asks = asks.reindex(range(max_len)).fillna(0)

        merged = pd.concat([bids, asks], axis=1)
        merged.insert(0, "index", range(len(merged)))
        merged.insert(0, "timestamp", ts)
        merged.insert(0, "dt", time_dict['Shanghai'])

        for _, row in merged.iterrows():
            record = {
                "dt": row["dt"],
                "timestamp": int(row["timestamp"]),
                "index": int(row["index"]),
                "bid_price": float(row["bid_price"]),
                "bid_qty": float(row["bid_qty"]),
                "ask_price": float(row["ask_price"]),
                "ask_qty": float(row["ask_qty"])
            }
            record = {k: convert_value(v) for k, v in record.items()}
            producer.send(KAFKA_TOPIC, value=record).add_errback(
                lambda e: print(f"Kafka 发送失败: {e}")
            )

        producer.flush()  # 确保数据及时落盘

    except Exception as e:
        print(f"处理消息出错: {e}")


# ===== 启动 WebSocket =====
def start_socket():
    global twm
    with twm_lock:
        try:
            if twm is not None:
                print("尝试停止旧的 WebSocket...")
                twm.stop()
                time.sleep(3)
                twm = None

            print("启动新的 WebSocket 连接...")
            twm = ThreadedWebsocketManager(api_key=api_key, api_secret=api_secret)
            twm.start()
            twm.start_depth_socket(callback=handle_depth_socket, symbol=symbol, depth='20', interval=100)
            print("WebSocket 启动成功")

        except Exception as e:
            print(f"启动 WebSocket 出错: {e}")


# ===== 看门狗检测连接 =====
def watchdog():
    global last_msg_time
    while not stop_event.is_set():
        now = time.time()
        if now - last_msg_time > TIMEOUT_THRESHOLD:
            print("WebSocket 超过 30 秒未收到消息，尝试重连...")
            start_socket()
        time.sleep(RECONNECT_INTERVAL)


# ===== 优雅退出 =====
def signal_handler(sig, frame):
    print("捕获到退出信号，准备关闭程序...")
    stop_event.set()
    if twm:
        twm.stop()
    producer.flush()
    producer.close()
    sys.exit(0)


# ===== 主函数 =====
def main():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    start_socket()
    threading.Thread(target=watchdog, daemon=True).start()

    while not stop_event.is_set():
        time.sleep(10)


if __name__ == "__main__":
    main()
