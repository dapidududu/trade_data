import time
import pandas as pd
from binance import ThreadedWebsocketManager
from kafka import KafkaProducer
import json
from datetime import datetime, timedelta
import pytz
import threading
import traceback

# 定义上海时区
shanghai_tz = pytz.timezone("Asia/Shanghai")

# Binance API（可为空）
api_key = ""
api_secret = ""

# Kafka 配置
KAFKA_TOPIC = "BTCFDUSD_OrderBook"
KAFKA_SERVERS = ["43.159.56.125:9092", "43.163.1.156:9092", "43.156.2.129:9092"]

# Kafka 生产者
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    linger_ms=100,  # 批处理延迟
    batch_size=32768
)

# 全局变量
twm = None
twm_lock = threading.Lock()  # 线程锁，防止同时start/stop冲突
symbol = "btcfdusd"
last_msg_time = time.time()
RECONNECT_INTERVAL = 10  # 秒
TIMEOUT_THRESHOLD = 30   # 超过 30 秒没消息视为掉线


def parse_timestamp(ts):
    if isinstance(ts, str):
        ts = int(ts)

    length = len(str(ts))
    epoch = datetime(1970, 1, 1)

    if length == 13:
        dt = epoch + timedelta(milliseconds=ts)
        unit = "毫秒"
        timestamp_ms = ts
    elif length == 16:
        dt = epoch + timedelta(microseconds=ts)
        unit = "微秒"
        timestamp_ms = ts // 1000
    elif length == 19:
        dt = epoch + timedelta(microseconds=ts // 1000)
        unit = "纳秒（已转为微秒）"
        timestamp_ms = ts // 1_000_000
    else:
        dt = epoch + timedelta(seconds=ts)
        unit = "秒"
        timestamp_ms = ts * 1000

    dt_utc = dt.replace(tzinfo=pytz.utc)
    dt_shanghai = dt_utc.astimezone(shanghai_tz)

    return {
        "unit": unit,
        "UTC": dt_utc.strftime('%Y-%m-%d %H:%M:%S.%f %Z'),
        "Shanghai": dt_shanghai.strftime("%Y-%m-%d"),
        "timestamp_ms": timestamp_ms
    }


def handle_depth_socket(msg):
    global last_msg_time
    try:
        last_msg_time = time.time()  # 更新时间戳

        ts = msg.get("E", int(time.time() * 1000))
        time_dict = parse_timestamp(ts)
        print(ts)
        bids = pd.DataFrame(msg.get('bids', []), columns=["bid_price", "bid_qty"])
        asks = pd.DataFrame(msg.get('asks', []), columns=["ask_price", "ask_qty"])

        bids.reset_index(drop=True, inplace=True)
        asks.reset_index(drop=True, inplace=True)

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
            producer.send(KAFKA_TOPIC, value=record).add_errback(lambda e: print("Kafka 发送失败：", e))

    except Exception as e:
        print(f"[Error] 处理消息出错: {e}")
        traceback.print_exc()


def start_socket():
    global twm
    with twm_lock:
        try:
            if twm is not None:
                print("[INFO] 尝试停止旧的 WebSocket...")
                twm.stop()
                time.sleep(3)  # 多等一会
                twm = None  # 显式清除旧实例

            print("[INFO] WebSocket 启动新的...")
            twm = ThreadedWebsocketManager(api_key=api_key, api_secret=api_secret)
            twm.start()
            twm.start_depth_socket(callback=handle_depth_socket, symbol=symbol, depth='20', interval=100)
            print("[INFO] WebSocket 启动成功")

        except Exception as e:
            print(f"[Error] 启动 WebSocket 出错: {e}")
            traceback.print_exc()


def watchdog():
    global last_msg_time
    """每 RECONNECT_INTERVAL 秒检测一次连接状态"""
    while True:
        now = time.time()
        if now - last_msg_time > TIMEOUT_THRESHOLD:
            print("[WARNING] WebSocket 超过 30 秒未收到消息，尝试重连...")
            start_socket()  # 自动重启
        time.sleep(RECONNECT_INTERVAL)


def main():
    start_socket()

    # 启动看门狗线程
    threading.Thread(target=watchdog, daemon=True).start()

    # 保持主线程不退出
    while True:
        time.sleep(10)


if __name__ == "__main__":
    main()

