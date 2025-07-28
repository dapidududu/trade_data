import json
from kafka import KafkaProducer
# #43.134.2.101 43.156.229.202 43.134.66.240
# # # Kafka 配置
KAFKA_BROKER = ["43.159.56.125:9092", "43.163.1.156:9092", "43.156.2.129:9092"]  # 你的 Kafka 服务器地址
KAFKA_TOPIC = 'ETH'  # Kafka 主题
BATCH_SIZE = 10  # 每次上传的行数

def send_message():
    # 初始化 Kafka 生产者
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,  # Kafka 集群地址
        value_serializer=lambda v: json.dumps(v).encode("utf-8")) # 序列化 JSON 数据

    # 构造 JSON 数据
    data = [{"id": 1, "name": "Alice", "price": 100.5}, {"id": 1, "name": "Alice", "price": 100.5}]
    for data in range (1,1000):
        # 发送数据
        producer.send(KAFKA_TOPIC, value={"id": data})
    producer.flush()

    print("消息发送成功")

from kafka import KafkaConsumer
def get_data():
    # Kafka 消费者配置
    consumer = KafkaConsumer(
        "BTCFDUSD_OrderBook",
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )

    # 监听 Kafka 主题并消费消息
    print("等待接收 JSON 消息...")
    for message in consumer:
        print("收到消息:", message.value)

if __name__ == "__main__":
    # send_message()
    get_data()