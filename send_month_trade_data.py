import re

import numpy
import json
import os
import shutil
import zipfile
import logging
import multiprocessing
import pandas as pd
from decimal import Decimal
from kafka import KafkaProducer
from datetime import datetime, timedelta
import pytz
# 定义上海时区
shanghai_tz = pytz.timezone("Asia/Shanghai")
# Kafka 配置
KAFKA_BROKER = ["43.134.2.101:9092", "43.156.229.202:9092", "43.134.66.240:9092"]
BATCH_SIZE = 1000000  # 每次上传的行数
NUM_WORKERS = 4  # 进程数，提高并发度
# coin_list = ['BTC', "ETH", "XRP"]
# futures_coin_list = ["BNB", "SOL", "TRUMP", "DOGE", "ADA", "1000PEPE"]
# spot_coin_list = ['BTCFDUSD']
# USDC_futures_coin_list = ["SOLUSDC", "XRPUSDC", "DOGEUSDC", "1000PEPEUSDC", "SUIUSDC", "BNBUSDC", "ENAUSDC"]
USDC_futures_coin_list = ["1000PEPEUSDC"]
coin_list = []
futures_coin_list = []
spot_coin_list = []

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

def convert_value(value):
    """转换数据类型：数值转换为 float，布尔值转换为 bool，字符串保持 str"""
    if isinstance(value, str):
        # 处理布尔值
        if value.lower() in ["true", "false"]:
            return value.lower() == "true"
        # 处理数值
        try:
            return numpy.float64(value) if "." in value else int(value)
        except ValueError:
            return value  # 如果转换失败，保持原始字符串
    elif isinstance(value, Decimal):
        return numpy.float64(value)
    return value  # 其他类型保持不变

# **初始化 Kafka 生产者**
def init_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        max_request_size=5 * 1024 * 1024,  # 5MB 限制单个消息大小
        batch_size=500000,  # 批次大小 100KB，适用于中等吞吐
        linger_ms=500,  # 适当延迟批量发送，提高吞吐
    )


# **解压 ZIP 文件**
def extract_zip(zip_path, extract_to, logger):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)
    logger.info(f"Extracted: {zip_path} to {extract_to}")


# **CSV 解析 & 发送 Kafka**
def send_to_kafka(csv_path, coin, producer, logger, trade_type):
    logger.info(f"Processing: {csv_path}")

    if trade_type == "futures":
        column_names = ["id", "price", "qty", "quote_qty", "time", "is_buyer_maker"]
    else:
        column_names = ["id", "price", "qty", "quote_qty", "time", "is_buyer_maker", "None"]
    df_chunks = pd.read_csv(csv_path, names=column_names, chunksize=BATCH_SIZE)

    topic = re.sub(r'^\d+', '', coin)
    if trade_type == "spot":
        topic = f'{coin}_SPOT'
    else:
        if coin in futures_coin_list:
            topic = f'{coin}USDT_FUTURES'
        elif coin in USDC_futures_coin_list:
            topic = f'{topic}_FUTURES'
    for chunk_num, chunk in enumerate(df_chunks):
        messages = chunk.to_dict(orient="records")
        batch = []
        for row in messages:
            if trade_type == "spot":
                row.pop("None", None)
            row = {key: convert_value(value) for key, value in row.items()}
            if isinstance(row['id'], str):  # 过滤非法数据
                continue
            row['dt'] = parse_timestamp(row['time'])['Shanghai']
            batch.append(row)

        for message in batch:
            producer.send(topic, value=message)

        logger.info(f"{csv_path} - chunk_num: {chunk_num} - {len(batch)} 条数据上传")

    producer.flush()
    logger.info(f"Uploaded to Kafka: {csv_path}")


# **清理已处理的文件**
def cleanup_files(file_path, logger):
    if os.path.exists(file_path):
        if os.path.isdir(file_path):
            shutil.rmtree(file_path)
        else:
            os.remove(file_path)
        logger.info(f"Deleted: {file_path}")


# **处理 ZIP & 生成日志**
def process_zip(zip_path, coin, trade_type):
    """每个进程独立处理一个 ZIP 文件及其 CSV"""
    extract_folder = os.path.join(os.path.dirname(zip_path), f"extracted_{os.getpid()}")  # 进程独占文件夹
    os.makedirs(extract_folder, exist_ok=True)

    log_file = f"log_{os.path.basename(zip_path).replace('.zip', '.log')}"
    logger = setup_logger(log_file)

    extract_zip(zip_path, extract_folder, logger)  # 解压 ZIP

    producer = init_kafka_producer()  # Kafka 生产者

    for csv_file in os.listdir(extract_folder):
        csv_path = os.path.join(extract_folder, csv_file)
        send_to_kafka(csv_path, coin, producer, logger, trade_type)  # 发送数据
        cleanup_files(csv_path, logger)  # 删除 CSV

    cleanup_files(zip_path, logger)  # 删除 ZIP
    cleanup_files(extract_folder, logger)  # 删除解压目录
    producer.close()
    logger.info(f"Processing complete for {zip_path}")


# **多进程管理**
def main():
    base_dir = "."  # 数据存放目录
    total_processes = []
    coin_type_list = []
    for coin in coin_list:
        coin_type_list.append([coin, "futures"])
    for coin in futures_coin_list:
        coin_type_list.append([coin, "futures"])
    for coin in USDC_futures_coin_list:
        coin_type_list.append([coin, "futures"])
    for coin in spot_coin_list:
        coin_type_list.append([coin, "spot"])

    for coin in coin_type_list:
        coin_dir = os.path.join(base_dir, coin[0])

        # 获取 ZIP 文件列表
        zip_files = [os.path.join(coin_dir, f) for f in os.listdir(coin_dir) if f.endswith(".zip")]

        # 为每个币种启动一个进程池
        if zip_files:
            pool = multiprocessing.Pool(processes=NUM_WORKERS)
            process_args = [(zip_path, coin[0], coin[1]) for zip_path in zip_files]

            # 异步启动，不阻塞主循环
            p = pool.starmap_async(process_zip, process_args)
            total_processes.append((pool, p))

    # 等待所有币种上传完成
    for pool, p in total_processes:
        p.wait()  # 等待结果
        pool.close()
        pool.join()
    logging.info("所有 ZIP 处理完成")


if __name__ == "__main__":
    main()


