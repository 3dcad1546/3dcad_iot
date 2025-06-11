import asyncio
import json
import os
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from aiokafka import AIOKafkaConsumer
from typing import Dict, Set

app = FastAPI()

# ─── Configuration from .env or default ────────────────────────────
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
WS_TOPICS = {
    "sensors": "raw_server_data",
    "triggers": "trigger_events",
    "commands": "machine_commands",
    "mes-logs": "mes_uploads",
    "trace-logs": "trace_logs",
    "scan-results": "scan_results",
    "machine-status": "machine_status"
}

# ─── Connection Manager ────────────────────────────────────────────
class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, Set[WebSocket]] = {k: set() for k in WS_TOPICS}

    async def connect(self, name: str, websocket: WebSocket):
        await websocket.accept()
        self.active_connections[name].add(websocket)

    def disconnect(self, name: str, websocket: WebSocket):
        self.active_connections[name].discard(websocket)

    async def broadcast(self, name: str, message: str):
        alive = set()
        for connection in self.active_connections[name]:
            try:
                await connection.send_text(message)
                alive.add(connection)
            except:
                continue
        self.active_connections[name] = alive

manager = ConnectionManager()

# ─── Kafka to WebSocket Worker ─────────────────────────────────────
async def kafka_consumer_task(ws_name: str, kafka_topic: str):
    consumer = AIOKafkaConsumer(
        kafka_topic,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id=f"{ws_name}_group"
    )
    await consumer.start()
    try:
        async for msg in consumer:
            value = msg.value.decode("utf-8") if isinstance(msg.value, bytes) else str(msg.value)
            await manager.broadcast(ws_name, value)
    except Exception as e:
        print(f"Kafka consumer for topic {kafka_topic} failed: {e}")
    finally:
        await consumer.stop()

# ─── Startup: Create Consumer Tasks ────────────────────────────────
@app.on_event("startup")
async def startup_event():
    for ws_name, kafka_topic in WS_TOPICS.items():
        asyncio.create_task(kafka_consumer_task(ws_name, kafka_topic))

# ─── WebSocket Route ───────────────────────────────────────────────
@app.websocket("/ws/{stream}")
async def websocket_endpoint(stream: str, websocket: WebSocket):
    if stream not in WS_TOPICS:
        await websocket.close(code=1008)
        return
    await manager.connect(stream, websocket)
    try:
        while True:
            await websocket.receive_text()  # To keep connection alive
    except WebSocketDisconnect:
        manager.disconnect(stream, websocket)
