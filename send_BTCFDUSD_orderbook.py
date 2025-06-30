import time
import pandas as pd
from binance import ThreadedWebsocketManager
from kafka import KafkaProducer
import json
from datetime import datetime, timedelta
import pytz
# 定义上海时区
shanghai_tz = pytz.timezone("Asia/Shanghai")
# Binance API 可为空
api_key = ""
api_secret = ""

# Kafka 配置
KAFKA_TOPIC = "BTCFDUSD_OrderBook"
KAFKA_SERVERS = ["43.159.56.125:9092", "43.163.1.156:9092", "43.156.2.129:9092"]  # 请替换成你的 Kafka 地址

# 初始化 Kafka 生产者
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    linger_ms=100,  # 等待 100ms 批量发送
    batch_size=32768
)

def parse_timestamp(ts):
    """
    自动识别时间戳单位（秒、毫秒、微秒、纳秒），并转换为上海时间。

    :param ts: 整数或字符串形式的时间戳
    :return: dict，包含单位、UTC时间、上海时间
    """
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

def main():
    symbol = "btcfdusd"  # 注意全部小写，WebSocket 要求

    twm = ThreadedWebsocketManager(api_key=api_key, api_secret=api_secret)
    twm.start()

    def handle_depth_socket(msg):
        try:
            ts = int(time.time() * 1000)
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
                producer.send(KAFKA_TOPIC, value=record)
                # print(record)
        except Exception as e:
            print(f"[WebSocket Error] 处理深度消息失败: {e}，消息内容: {msg}")
    # 使用自定义 stream 监听 depth200
    twm.start_depth_socket(callback=handle_depth_socket, symbol=symbol, depth='20', interval=100)

    twm.join()

if __name__ == "__main__":
    main()
