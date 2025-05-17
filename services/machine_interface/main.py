from kafka import KafkaConsumer
import json
import os

consumer = KafkaConsumer(
    "machine_commands",
    bootstrap_servers=os.getenv("KAFKA_BROKER", "localhost:9092"),
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

for msg in consumer:
    command = msg.value
    print(f"Executing command: {command}")
    # TODO: Translate to Modbus/OPC UA call
    # e.g., pymodbus client write registers based on `command`
