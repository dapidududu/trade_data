import numpy
import json
import os
from decimal import Decimal
import pandas as pd
import zipfile
import shutil
import time
from kafka import KafkaProducer

# Kafka 配置
KAFKA_BROKER = ["43.134.2.101:9092","43.156.229.202:9092","43.134.66.240:9092"]   # 你的 Kafka 服务器地址
BATCH_SIZE = 1000000  # 每次上传的行数

# 数据存放的根目录
base_dir = "."

import logging

# 配置日志
logging.basicConfig(
    filename="app_XRP.log",  # 日志文件名
    level=logging.INFO,   # 日志级别（DEBUG, INFO, WARNING, ERROR, CRITICAL）
    format="%(asctime)s - %(levelname)s - %(message)s",  # 日志格式
    datefmt="%Y-%m-%d %H:%M:%S"
)
logging.info("程序启动")


# 统一数据类型转换的函数
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


# 初始化 Kafka 生产者
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    batch_size=1048576,  # 1MB 批量大小
    linger_ms=10,  # 10ms 内尽量批量发送
    max_request_size=10485760  # 允许单次请求 10MB
)  # 序列化 JSON 数据



def extract_zip(zip_path, extract_to):
    """解压 ZIP 文件"""
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)
    logging.info(f"Extracted: {zip_path} to {extract_to}")


def send_to_kafka(file_path, coin):
    """读取 CSV 并按批次上传到 Kafka"""
    logging.info(f"Processing: {file_path}")
    column_names = ["id", "price", "qty", "quote_qty", "time", "is_buyer_maker"]  # 根据实际情况修改
    # 逐块读取 CSV 文件
    df = pd.read_csv(file_path, names=column_names, chunksize=BATCH_SIZE)
    chunk_num = 0
    for chunk in df:
        messages = chunk.to_json(orient='records', lines=True)
        for line in messages.split("\n"):
            if line:
                line_data = json.loads(line)
                line_data = {key: convert_value(value) for key, value in line_data.items()}
                if isinstance(line_data['id'], str):
                    continue
                logging.info(line_data)
                producer.send(coin, key=b"test", value=line_data)
                producer.flush()
            else:
                continue
        logging.info(f"{file_path}--------------chunk_num: {chunk_num}")
        chunk_num += 1
        time.sleep(1)
    logging.info(f"Uploaded to Kafka: {file_path}")


def cleanup_files(*file_paths):
    """删除已处理的文件"""
    for file_path in file_paths:
        if os.path.exists(file_path):
            if os.path.isdir(file_path):
                shutil.rmtree(file_path)  # 删除目录
            else:
                os.remove(file_path)  # 删除文件
            logging.info(f"Deleted: {file_path}")


# 遍历每个币种文件夹
# for coin in os.listdir(base_dir):
coin = "XRP"
coin_dir = os.path.join(base_dir, coin)

# if not os.path.isdir(coin_dir):  # 确保是文件夹
#     continue

for zip_file in os.listdir(coin_dir):
    if not zip_file.endswith(".zip"):
        continue

    zip_path = os.path.join(coin_dir, zip_file)
    extract_folder = os.path.join(coin_dir, "extracted")
    os.makedirs(extract_folder, exist_ok=True)

    # 解压 ZIP 文件
    extract_zip(zip_path, extract_folder)

    # 遍历解压出的 CSV 文件
    for csv_file in os.listdir(extract_folder):
        csv_path = os.path.join(extract_folder, csv_file)
        send_to_kafka(csv_path, coin)

        # 删除 CSV
        cleanup_files(csv_path)
        time.sleep(1)

    # 删除 ZIP 文件及解压目录
    cleanup_files(zip_path, extract_folder)

