import time
import os
import requests
from kafka import KafkaProducer
import json

KAFKA_TOPIC = "raw_server_data"
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
POLLING_INTERVAL = int(os.getenv("POLLING_INTERVAL", "10"))

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

while True:
    try:
        # Replace with real API call:
        # resp = requests.get("http://server/api/data")
        # data = resp.json()
        data = {"timestamp": time.time(), "source": "server1", "value": 42}
        producer.send(KAFKA_TOPIC, data)
        print(f"Published data: {data}")
    except Exception as e:
        print(f"Polling error: {e}")
    time.sleep(POLLING_INTERVAL)
