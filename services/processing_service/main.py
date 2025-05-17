from kafka import KafkaConsumer, KafkaProducer
import json
import os
from datetime import datetime

consumer = KafkaConsumer(
    "raw_server_data",
    bootstrap_servers=os.getenv("KAFKA_BROKER", "localhost:9092"),
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BROKER", "localhost:9092"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

for msg in consumer:
    data = msg.value
    print(f"Processing raw data: {data}")
    # TODO: Insert your business logic here.
    command = {
        "cmd": "START_MACHINE",
        "target": "Machine1",
        "timestamp": datetime.utcnow().isoformat()
    }
    producer.send("machine_commands", command)
    print(f"Issued command: {command}")
