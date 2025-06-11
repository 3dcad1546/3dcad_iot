import os
import json
import time
import asyncio
import paho.mqtt.client as mqtt
from kafka import KafkaProducer
import psycopg2

# ─── Environment Variables ─────────────────────────────────────────────
MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "iot/sensor")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TRIGGER_KAFKA_TOPIC = os.getenv("TRIGGER_KAFKA_TOPIC", "trigger_events")
DB_URL = os.getenv("DB_URL")

# ─── PostgreSQL Setup ──────────────────────────────────────────────────
conn = psycopg2.connect(DB_URL)
conn.autocommit = True
cur = conn.cursor()

# ─── Kafka Producer Setup ──────────────────────────────────────────────
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# ─── MQTT Event Handler ────────────────────────────────────────────────
def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT Broker:", rc)
    client.subscribe(MQTT_TOPIC)

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")

        # Push to Kafka topic
        producer.send(TRIGGER_KAFKA_TOPIC, {**payload, "ts": timestamp})

        # Save to PostgreSQL
        cur.execute(
            "INSERT INTO mqtt_triggers(topic, payload, ts) VALUES (%s, %s::jsonb, NOW())",
            (msg.topic, json.dumps(payload))
        )

        print(f"Trigger processed and pushed to Kafka: {payload}")

    except Exception as e:
        print("Error processing trigger:", e)

# ─── Main ──────────────────────────────────────────────────────────────
def main():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(MQTT_BROKER, 1883, 60)
    client.loop_forever()

if __name__ == "__main__":
    main()
