import os
import json
import time
import asyncio
from aiokafka import AIOKafkaProducer
from asyncio_mqtt import Client as AsyncMQTTClient
import psycopg2

# ─── Environment Variables ─────────────────────────────────────────────
MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "iot/sensor")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TRIGGER_KAFKA_TOPIC = os.getenv("TRIGGER_KAFKA_TOPIC", "trigger_events")
DB_URL = os.getenv("DB_URL")

# ─── PostgreSQL Setup ──────────────────────────────────────────────────
DB_URL = os.getenv("DB_URL")
for _ in range(10):
    try:
        conn = psycopg2.connect(DB_URL)
        break
    except psycopg2.OperationalError:
        print("PostgreSQL not ready, retrying...")
        time.sleep(5)
else:
    raise RuntimeError("PostgreSQL not available")
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
async def main():
    prod = AIOKafkaProducer(bootstrap_servers=KAFKA_BROKER, ...)
    await prod.start()
    async with AsyncMQTTClient(MQTT_BROKER) as mqtt:
        async with mqtt.messages() as messages:
            await mqtt.subscribe(MQTT_TOPIC)
            async for msg in messages:
                payload = json.loads(msg.payload.decode())
                await prod.send_and_wait(TRIGGER_KAFKA_TOPIC, {..., "ts":time_str})
                cur.execute("INSERT ...", ...)
if __name__ == "__main__":
    main()
