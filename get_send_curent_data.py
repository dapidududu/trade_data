import json
import logging
import multiprocessing
import re
import time
from datetime import datetime, timedelta
from decimal import Decimal

import numpy
import pytz
from binance.client import Client
from kafka import KafkaProducer

shanghai_tz = pytz.timezone("Asia/Shanghai")
KAFKA_BROKER = ["43.134.2.101:9092", "43.156.229.202:9092", "43.134.66.240:9092"]


def time_res(client):
    time_res = client.get_server_time()
    print(time_res)


def fetch_all_historical_trades(api_key, api_secret, symbol, trade_type, batch_size=500):
    client = Client(api_key, api_secret)

    all_trades = []
    producer = init_kafka_producer()  # Kafka 生产者
    logger = setup_logger(f'log/{symbol}_recent.log' if trade_type == 'spot' else f'log/{symbol}USDT_recent.log')
    recent_trade_data = get_recent_trades_info(client, symbol, batch_size, trade_type)

    current_id = recent_trade_data[-1]["id"] + 1
    all_trades.extend(recent_trade_data)
    while True:
        try:
            trades_data = get_historical_trades_info(client, symbol, batch_size, current_id, trade_type)
            if not trades_data:
                time.sleep(0.5)
                continue

            all_trades.extend(trades_data)
            all_trades = convert_keys_to_snake_case(all_trades)
            current_id = trades_data[-1]["id"] + 1  # 下一轮起点 ID
            print(f"已抓取至 ID {current_id}，本轮获取 {len(all_trades)} 条:{all_trades}")

            logger.info(f"已抓取至 ID {current_id}，本轮获取 {len(all_trades)} 条:{all_trades}")
            send_to_kafka(all_trades, symbol, producer, logger, trade_type)
            all_trades = []
            # 防止限速，必要时加延迟
            time.sleep(0.5)
        except Exception as e:
            logger.error(f"错误：{e}")
            time.sleep(5)

    return all_trades


def get_recent_trades_info(client, symbol, batch_size, trade_type):
    if trade_type == 'spot':
        info = client.get_recent_trades(symbol=symbol, limit=batch_size)
    else:
        info = client.futures_recent_trades(symbol=f'{symbol}USDT', limit=batch_size)
    # print(len(info))
    # print(info)
    return info


def get_historical_trades_info(client, symbol, limit, fromId, trade_type):
    if trade_type == 'spot':
        info = client.get_historical_trades(symbol=symbol, limit=limit, fromId=fromId)
    else:
        info = client.futures_historical_trades(symbol=f'{symbol}USDT', limit=limit, fromId=fromId)
    # print(len(info))
    # print(info)
    return info


# **动态创建日志**
def setup_logger(log_file):
    """为当前进程创建独立日志"""
    logger = logging.getLogger(log_file)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    # 创建文件 handler
    file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
    file_handler.setFormatter(formatter)

    # 添加 handler
    logger.addHandler(file_handler)
    return logger


# **初始化 Kafka 生产者**
def init_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        max_request_size=5 * 1024 * 1024,  # 5MB 限制单个消息大小
        batch_size=100000,  # 批次大小 100KB，适用于中等吞吐
        linger_ms=300,  # 适当延迟批量发送，提高吞吐
    )


def camel_to_snake(name):
    """将驼峰命名转换为下划线命名"""
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

def convert_keys_to_snake_case(data):
    """递归将 dict 的所有 key 从驼峰转下划线"""
    if isinstance(data, dict):
        return {camel_to_snake(k): convert_keys_to_snake_case(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [convert_keys_to_snake_case(item) for item in data]
    else:
        return data

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
    elif length == 16:
        dt = epoch + timedelta(microseconds=ts)
        unit = "微秒"
    elif length == 19:
        dt = epoch + timedelta(microseconds=ts // 1000)
        unit = "纳秒（已转为微秒）"
    else:
        dt = epoch + timedelta(seconds=ts)
        unit = "秒"

    dt_utc = dt.replace(tzinfo=pytz.utc)
    dt_shanghai = dt_utc.astimezone(shanghai_tz)

    return {
        "UTC": dt_utc.strftime('%Y-%m-%d %H:%M:%S.%f %Z'),
        "Shanghai": dt_shanghai.strftime("%Y-%m-%d")
    }


# **发送 Kafka**
def send_to_kafka(trade_data, coin, producer, logger, trade_type):
    batch = []
    try:
        for row in trade_data:
            if trade_type == "spot":
                row.pop("is_best_match", None)
            row = {key: numpy.float64(value) if isinstance(value, Decimal) else value for key, value in row.items()}

            if isinstance(row['id'], str):  # 过滤非法数据
                continue
            row['dt'] = parse_timestamp(row['time'])['Shanghai']
            batch.append(row)

        for message in batch:
            topic = f'{coin}_SPOT' if trade_type == "spot" else coin
            producer.send(topic, value=message)
        logger.info(f"{coin} - {len(batch)} 条数据上传")
        producer.flush()
    except Exception as e:
        logger.error(f"错误：{e}")



def wrapper(args):
    api_key, api_secret, symbol, market_type = args
    return fetch_all_historical_trades(api_key, api_secret, symbol, market_type)


def main():
    api_key = "VilqADd2OseILGt8bcj8JMlO5hNgQmMQx6mZrRrrkex4czr5bjGUIFo4LVdSIlZE"
    api_secret = "zFUeZEU4HTf4vTCynKZxuQLVSVnxV1qymiHCYdRDQgHaL9pllu1LPj0XZSkWctPQ"
    # futures_symbol_list = ['BTC', "ETH", "XRP"]
    # spot_symbol_list = ['BTCFDUSD']
    futures_symbol_list = ['BTC']
    spot_symbol_list = []
    with multiprocessing.Pool(processes=(len(futures_symbol_list) + len(spot_symbol_list))) as pool:
        # pool.starmap(process_zip, [coin for coin in coin_list])
        tasks_args = [(api_key, api_secret, symbol, "futures") for symbol in futures_symbol_list] + \
                [(api_key, api_secret, symbol, "spot") for symbol in spot_symbol_list]

        pool.starmap(fetch_all_historical_trades, [tasks_arg for tasks_arg in tasks_args])




if __name__ == '__main__':
    main()
