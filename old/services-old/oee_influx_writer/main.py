import asyncio
import os
import json
from aiokafka import AIOKafkaConsumer
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# ─── Environment Configuration ─────────────────────────────────────────────
KAFKA_TOPIC = os.getenv("OEE_STATUS_TOPIC", "oee_status")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BROKER", "kafka:9092")

INFLUX_URL = os.getenv("INFLUXDB_URL", "http://influxdb:8086")
INFLUX_TOKEN = os.getenv("INFLUXDB_TOKEN", "edgetoken")
INFLUX_ORG = os.getenv("INFLUXDB_ORG", "EdgeOrg")
INFLUX_BUCKET = os.getenv("INFLUXDB_BUCKET", "EdgeBucket")

# ─── InfluxDB Setup ───────────────────────────────────────────────────────
influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)

# ─── Kafka Consumer Logic ─────────────────────────────────────────────────
async def consume_oee():
    print(f"[OEE Writer] Connecting to Kafka: {KAFKA_BOOTSTRAP}, topic: {KAFKA_TOPIC}")
    
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="oee-influx-writer-group",
        auto_offset_reset="latest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )
    await consumer.start()

    try:
        async for msg in consumer:
            data = msg.value
            if not isinstance(data, dict):
                print("[WARN] Skipping non-dict payload")
                continue

            ts = data.get("ts")
            if not ts:
                print("[WARN] Missing timestamp in OEE payload")
                continue

            point = Point("oee").time(ts, WritePrecision.S)

            for key, value in data.items():
                if key == "ts":
                    continue
                if isinstance(value, (int, float)):
                    point.field(key, value)
                elif isinstance(value, str):
                    point.tag(key, value)  # optional: store non-numeric as tags
                else:
                    print(f"[WARN] Skipping unsupported type for key: {key}")

            write_api.write(bucket=INFLUX_BUCKET, record=point)
            print(f"[OEE Writer] Wrote to InfluxDB: {data}")
    except Exception as e:
        print(f"[ERROR] OEE consumer error: {e}")
    finally:
        await consumer.stop()
        influx_client.close()
        print("[OEE Writer] Stopped cleanly.")

# ─── Main Runner ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    try:
        asyncio.run(consume_oee())
    except KeyboardInterrupt:
        print("Interrupted by user")
