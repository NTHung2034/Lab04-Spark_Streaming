import threading
import time
from datetime import datetime, timezone
import requests
from kafka import KafkaProducer
import json
import socket

# Config
TOPIC_NAME = "btc-price"
SEND_INTERVAL = 0.1  # 100ms
NUM_FETCH_THREADS = 8  # 8 threads để fetch song song
REQUEST_TIMEOUT = 0.25  # 250ms timeout cho request HTTP
MAX_RETRIES = 3
BASE_URL = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"

# Giả lập DNS cache để tăng tốc độ resolve host
socket.setdefaulttimeout(REQUEST_TIMEOUT)

# Kafka producer với retries
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retries=3
)

# Shared data với timestamp chính xác
latest_data = {
    "symbol": "BTCUSDT",
    "price": None,
    "last_update": None,
    "counter": 0,
    "last_price": None
}
data_lock = threading.Lock()

def fetch_price():
    """Phiên bản fetch tối ưu với keep-alive và session reuse"""
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=10,
        pool_maxsize=10,
        max_retries=MAX_RETRIES
    )
    session.mount('https://', adapter)

    try:
        response = session.get(
            BASE_URL,
            timeout=REQUEST_TIMEOUT,
            headers={'Connection': 'keep-alive'}
        )
        if response.status_code == 200:
            data = response.json()
            if "symbol" in data and "price" in data:
                return float(data["price"])
    except Exception:
        pass
    return None

def fetch_worker():
    """Worker được tối ưu để liên tục fetch dữ liệu"""
    while True:
        start_time = time.time()
        price = fetch_price()

        if price is not None:
            with data_lock:
                update_needed = (
                    latest_data["price"] != price or
                    latest_data["counter"] >= 5 or
                    (datetime.now(timezone.utc) - latest_data["last_update"]).total_seconds() > 0.5
                )

                if update_needed:
                    latest_data["price"] = price
                    latest_data["last_update"] = datetime.now(timezone.utc)
                    latest_data["counter"] = 0
                    latest_data["last_price"] = price
                else:
                    latest_data["counter"] += 1

        elapsed = time.time() - start_time
        sleep_time = max(0, SEND_INTERVAL - elapsed)
        time.sleep(sleep_time)

def send_to_kafka():
    """Thread gửi dữ liệu lên Kafka theo tần suất định sẵn"""
    base_time = time.time()
    base_time = base_time - (base_time % SEND_INTERVAL)  # Làm tròn base_time theo SEND_INTERVAL
    count = 1

    while True:
        next_send = base_time + count * SEND_INTERVAL
        sleep_time = next_send - time.time()
        if sleep_time > 0:
            time.sleep(sleep_time)

        timestamp = datetime.fromtimestamp(next_send, tz=timezone.utc)
        # Làm tròn timestamp đến hàng trăm micro giây (100ms)
        rounded_time = timestamp.replace(microsecond=(timestamp.microsecond // 100000) * 100000)
        event_time = rounded_time.isoformat(timespec='milliseconds')

        with data_lock:
            current_data = {
                "symbol": latest_data["symbol"],
                "price": latest_data["price"] or latest_data["last_price"],
                "event_time": event_time,
                "data_age_ms": int((datetime.now(timezone.utc) - (latest_data["last_update"] or datetime.now(timezone.utc))).total_seconds() * 1000)
            }

        if current_data["price"] is not None:
            producer.send(TOPIC_NAME, current_data)
            print(f"Sending: {current_data}")
        else:
            print("Waiting for initial data...")

        count += 1

if __name__ == "__main__":
    # Khởi chạy worker threads
    for _ in range(NUM_FETCH_THREADS):
        threading.Thread(target=fetch_worker, daemon=True).start()

    # Thread gửi Kafka
    threading.Thread(target=send_to_kafka, daemon=True).start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down gracefully...")
