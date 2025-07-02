# processing_service/main.py
import os
import asyncio
import json
import time
import signal

import psycopg2
from psycopg2.extras import RealDictCursor
from aiokafka import AIOKafkaProducer
from asyncio_mqtt import Client as AsyncMQTTClient, MqttError

# ─── Environment ────────────────────────────────────────────────────
MQTT_BROKER       = os.getenv("MQTT_BROKER", "mqtt-broker")
MQTT_TOPIC        = os.getenv("MQTT_TOPIC", "iot/sensor")
KAFKA_BROKER      = os.getenv("KAFKA_BROKER", "kafka:9092")
TRIGGER_KAFKA_TOPIC = os.getenv("TRIGGER_KAFKA_TOPIC", "trigger_events")
DB_URL            = os.getenv("DB_URL", "postgresql://edge:edgepass@postgres:5432/edgedb")

# ─── Postgres setup ─────────────────────────────────────────────────
pg_conn = psycopg2.connect(DB_URL)
pg_conn.autocommit = True
pg_cur  = pg_conn.cursor(cursor_factory=RealDictCursor)
# ensure the table exists
pg_cur.execute("""
CREATE TABLE IF NOT EXISTS mqtt_triggers (
    id SERIAL PRIMARY KEY,
    topic TEXT,
    payload JSONB,
    ts TIMESTAMP DEFAULT NOW()
);
""")

# ─── Kafka producer ─────────────────────────────────────────────────
kafka_producer: AIOKafkaProducer

async def init_kafka():
    global kafka_producer
    kafka_producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    await kafka_producer.start()
    print("[Processing] Kafka producer started.")

async def shutdown_kafka():
    await kafka_producer.stop()
    print("[Processing] Kafka producer stopped.")

# ─── MQTT → Kafka bridge ────────────────────────────────────────────
async def mqtt_to_kafka_loop():
    # wrap everything in a single connection so we reconnect automatically
    async with AsyncMQTTClient(MQTT_BROKER) as client:
        # subscribe once
        await client.subscribe(MQTT_TOPIC)
        print(f"[Processing] Subscribed to MQTT topic {MQTT_TOPIC}")
        async with client.unfiltered_messages() as messages:
            async for msg in messages:
                try:
                    data = json.loads(msg.payload.decode())
                except json.JSONDecodeError:
                    print("⛔ invalid JSON from MQTT:", msg.payload)
                    continue

                timestamp = time.strftime("%Y-%m-%dT%H:%M:%S")
                record = {**data, "ts": timestamp}

                # 1) publish to Kafka
                try:
                    await kafka_producer.send_and_wait(TRIGGER_KAFKA_TOPIC, record)
                except Exception as e:
                    print("⛔ failed to send to Kafka:", e)

                # 2) persist in Postgres
                try:
                    pg_cur.execute(
                        "INSERT INTO mqtt_triggers(topic,payload) VALUES (%s,%s::jsonb)",
                        (msg.topic, json.dumps(data))
                    )
                except Exception as e:
                    print("⛔ failed to write to Postgres:", e)

                print(f"[Bridge] {msg.topic} → Kafka {TRIGGER_KAFKA_TOPIC}: {record}")

# ─── Graceful shutdown support ──────────────────────────────────────
stop_event = asyncio.Event()

def _cancel_tasks():
    print("[Processing] Shutdown requested.")
    stop_event.set()

# ─── Entrypoint ────────────────────────────────────────────────────
async def main():
    # handle SIGTERM / SIGINT
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _cancel_tasks)

    await init_kafka()

    # run the mqtt→kafka bridge until cancelled
    bridge_task = asyncio.create_task(mqtt_to_kafka_loop())

    # wait for shutdown
    await stop_event.wait()
    bridge_task.cancel()
    try:
        await bridge_task
    except asyncio.CancelledError:
        pass

    await shutdown_kafka()
    pg_conn.close()
    print("[Processing] Exited cleanly.")

if __name__ == "__main__":
    asyncio.run(main())
