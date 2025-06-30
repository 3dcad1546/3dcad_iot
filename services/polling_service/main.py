import os
import asyncio
import json
from datetime import datetime
import aiohttp
import psycopg2
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

DB_URL            = os.getenv("DB_URL")
KAFKA_BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
POLL_TOPIC        = os.getenv("RAW_DATA_TOPIC", "raw_server_data")
REQUEST_TOPIC     = os.getenv("POLL_REQUEST_TOPIC", "poll_requests")
MACHINE_API_1     = os.getenv("POLL_API_1", "http://server1/api/data")
MACHINE_API_2     = os.getenv("POLL_API_2", "http://server2/api/data")

async def fetch_and_publish(session, url, producer):
    async with session.get(url, timeout=10) as resp:
        resp.raise_for_status()
        payload = await resp.json()
    msg = {
        "timestamp": datetime.utcnow().isoformat(),
        "source_url": url,
        "data": payload
    }
    await producer.send_and_wait(POLL_TOPIC, msg)

async def load_interval(conn):
    cur = conn.cursor()
    cur.execute("SELECT value FROM config WHERE key='POLL_INTERVAL_SEC'")
    row = cur.fetchone()
    return int(row[0]) if row else 60  # default 60s

async def poll_loop(producer, conn):
    interval = await load_interval(conn)
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                await asyncio.gather(
                    fetch_and_publish(session, MACHINE_API_1, producer),
                    fetch_and_publish(session, MACHINE_API_2, producer),
                )
            except Exception as e:
                print("Polling error:", e)
            await asyncio.sleep(interval)

async def handle_requests(consumer, session, producer):
    async for msg in consumer:
        try:
            url = msg.value.decode()  # expect the URL to fetch
            await fetch_and_publish(session, url, producer)
        except Exception as e:
            print("On-demand poll error:", e)

async def main():
    # Postgres
    pg = psycopg2.connect(DB_URL)
    pg.autocommit = True

    # Kafka Producer
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP,
                                value_serializer=lambda v: json.dumps(v).encode())
    await producer.start()

    # Kafka Consumer for on-demand
    consumer = AIOKafkaConsumer(REQUEST_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP, auto_offset_reset="latest")
    await consumer.start()

    async with aiohttp.ClientSession() as session:
        # start background tasks
        await asyncio.gather(
            poll_loop(producer, pg),
            handle_requests(consumer, session, producer)
        )

    await producer.stop()
    await consumer.stop()
    pg.close()

if __name__=="__main__":
    asyncio.run(main())
