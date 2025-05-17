import paho.mqtt.client as mqtt
from kafka import KafkaProducer
import json
import os

MQTT_BROKER_HOST = os.getenv("MQTT_BROKER_HOST", "localhost")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def on_message(client, userdata, msg):
    payload = msg.payload.decode()
    print(f"Trigger received: topic={msg.topic} payload={payload}")
    producer.send("trigger_events", {"topic": msg.topic, "payload": payload})

client = mqtt.Client()
client.on_message = on_message
client.connect(MQTT_BROKER_HOST, 1883, 60)
client.subscribe("sensor/trigger")
client.loop_forever()
