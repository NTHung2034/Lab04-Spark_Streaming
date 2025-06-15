import json
from kafka import KafkaConsumer
import argparse

# Config
#TOPIC_NAME = "btc-price"
#TOPIC_NAME = "btc-price-moving"
TOPIC_NAME = "btc-price-zscore"

BOOTSTRAP_SERVERS = "localhost:9092"

def main():
    # lấy tên topic từ commandline 
    parser = argparse.ArgumentParser(description='Kafka Consumer')
    parser.add_argument('topic_name', help='Tên topic mà bạn muốn consume')
    args = parser.parse_args()

    topic_name = args.topic_name

    # Tạo Kafka consumer
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',  # Đọc từ đầu topic
        enable_auto_commit=True,
        group_id='btc-price-consumer-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print(f"Listening to topic '{topic_name}'...")
    try:
        for message in consumer:
            data = message.value
            print(f"Received: {data}")
    except KeyboardInterrupt:
        print("\nShutting down consumer...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()