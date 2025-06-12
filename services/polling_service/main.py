import os
import time
import requests
import json
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ─── Configuration from Environment ─────────────────────────────
LOCAL_SERVER_URLS = os.getenv("LOCAL_SERVER_URLS", "").split(",")
AUTH_TOKEN = os.getenv("LOCAL_SERVER_AUTH_TOKEN", "")
POLLING_INTERVAL = int(os.getenv("POLLING_INTERVAL", "30"))
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
RAW_TOPIC = os.getenv("RAW_KAFKA_TOPIC", "raw_server_data")
LOG_TRIGGER_URL = os.getenv("TRACE_LOG_TRIGGER", "http://traceproxy:8765/v2/logs")

HEADERS = {"Authorization": f"Bearer {AUTH_TOKEN}"} if AUTH_TOKEN else {}

# ─── Kafka Setup ────────────────────────────────────────────────


def get_producer():
    for attempt in range(10):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=5,
                linger_ms=10,
                request_timeout_ms=10000,
                max_block_ms=10000
            )
            return producer
        except KafkaError as e:
            print(f"[KafkaProducer] Connection failed (attempt {attempt+1}/10): {e}")
            time.sleep(5)
    raise RuntimeError("KafkaProducer: Failed to connect after retries")

producer = get_producer()


def poll_and_push():
    for url in LOCAL_SERVER_URLS:
        try:
            response = requests.get(url.strip(), headers=HEADERS, timeout=5)
            response.raise_for_status()
            data = response.json()
            print(f"[INFO] Data from {url}: {data}")
            producer.send(RAW_TOPIC, data)
        except Exception as e:
            print(f"[ERROR] Failed to poll {url}: {e}")

def trigger_log_upload():
    try:
        response = requests.post(LOG_TRIGGER_URL, timeout=3)
        print(f"[TRACE UPLOAD] Triggered: {response.status_code}")
    except Exception as e:
        print(f"[TRACE UPLOAD ERROR] {e}")

if __name__ == "__main__":
    print("[Polling Service] Started")
    while True:
        poll_and_push()
        trigger_log_upload()
        time.sleep(POLLING_INTERVAL)
