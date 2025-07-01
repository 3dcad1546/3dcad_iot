# processing_service/main.py
# ----------------------
# Microservice responsible for consuming raw_server_data and trigger_events,
# applying business logic, and emitting machine_commands for actuation.

import os
import asyncio
import json
import signal
import requests
import uuid
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import NoBrokersAvailable
import psycopg2
from psycopg2.extras import RealDictCursor

# ─── MES/Trace endpoints ─────────────────────────────

MES_PC_URL      = os.getenv("MES_PROCESS_CONTROL_URL")
TRACE_PROXY     = os.getenv("TRACE_PROXY_HOST", "trace-proxy:8765")

# ─── Configuration from environment ─────────────────────────────
KAFKA_BROKER   = os.getenv("KAFKA_BROKER", "kafka:9092")
RAW_TOPIC      = os.getenv("RAW_KAFKA_TOPIC", "raw_server_data")
TRIGGER_TOPIC  = os.getenv("TRIGGER_KAFKA_TOPIC", "trigger_events")
PLC_WRITE_TOPIC = os.getenv("PLC_WRITE_COMMANDS_TOPIC", "plc_write_commands")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "processing_service")
DB_URL         = os.getenv("DB_URL")  # e.g. postgresql://user:pass@postgres:5432/db

# ─── Initialize Postgres ─────────────────────────────────────────
pg_conn = psycopg2.connect(DB_URL)
pg_conn.autocommit = True
pg_cur  = pg_conn.cursor(cursor_factory=RealDictCursor)

# Ensure audit table exists
pg_cur.execute("""
CREATE TABLE IF NOT EXISTS mes_trace_history (
    id SERIAL PRIMARY KEY,
    serial TEXT,
    step TEXT NOT NULL,
    response_json JSONB NOT NULL,
    ts TIMESTAMP DEFAULT NOW()
);
""")

def load_machine_config(machine_id: str):
    pg_cur.execute("""
      SELECT mes_process_control_url AS pc_url,
             mes_upload_url      AS upload_url,
             is_mes_enabled
      FROM machine_config
      WHERE machine_id = %s
    """, (machine_id,))
    row = pg_cur.fetchone()
    if not row:
        raise RuntimeError(f"No machine_config for {machine_id}")
    return row

# ─── Kafka clients ───────────────────────────────────────────────
kafka_consumer = None
kafka_producer = None
MACHINE_ID = os.getenv("MACHINE_ID")
machine_cfg = load_machine_config(MACHINE_ID)

async def init_kafka():
    global kafka_consumer, kafka_producer
   
    # Producer: send machine_commands
    for _ in range(5):
        try:
            kafka_producer = AIOKafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode()
            )
            await kafka_producer.start()
            print("[Processing] Kafka producer started.")
            break
        except NoBrokersAvailable:
            print("[Processing] Kafka broker not available, retrying...")
            await asyncio.sleep(3)
    else:
        raise RuntimeError("Cannot connect to Kafka producer")

    # Consumer: subscribe to raw and trigger topics
    kafka_consumer = AIOKafkaConsumer(
        RAW_TOPIC,
        TRIGGER_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=CONSUMER_GROUP,
        value_deserializer=lambda b: json.loads(b.decode())
    )
    await kafka_consumer.start()
    print(f"[Processing] Subscribed to topics: {RAW_TOPIC}, {TRIGGER_TOPIC}")

async def shutdown():
    if kafka_consumer:
        await kafka_consumer.stop()
        print("[Processing] Kafka consumer stopped.")
    if kafka_producer:
        await kafka_producer.stop()
        print("[Processing] Kafka producer stopped.")
    pg_conn.close()
    print("[Processing] Postgres connection closed.")

# ─── Business logic stub ─────────────────────────────────────────
import uuid, requests

def process_event(topic: str, msg: dict) -> dict:
    if topic != TRIGGER_TOPIC:
        return {"emit":False}

    barcode = msg["barcode"]
    is_auto = msg.get("mode_auto", False)
    result  = {"Trace":0,"Process":0,"MES":0}

    # 1) Who’s logged in?
    pg_cur.execute("""
      SELECT username,shift_id FROM sessions
       WHERE logout_ts IS NULL AND role='operator'
    ORDER BY login_ts DESC LIMIT 1
    """)
    row = pg_cur.fetchone()
    operator = row["username"] if row else None

    # 2) Get machine_config
    pg_cur.execute("""
      SELECT mes_process_control_url,mes_upload_url,
             trace_process_control_url,trace_interlock_url,
             is_mes_enabled,is_trace_enabled
        FROM machine_config WHERE machine_id=%s
    """, (os.getenv("MACHINE_ID"),))
    cfg = pg_cur.fetchone()

    # If manual mode, just write zeros
    if not is_auto or operator is None:
        return {"emit":True,"command":{
          "section":"status_bits","tags":result,
          "request_id":str(uuid.uuid4())
        }}

    # 3) MES PC
    if cfg["is_mes_enabled"]:
      try:
        r = requests.post(cfg["mes_process_control_url"],json={
          "UniqueId":   barcode,
          "MachineId":  os.getenv("MACHINE_ID"),
          "OperatorId": operator,
          "Tools":[], "RawMaterials":[],
          "CbsStreamName": os.getenv("CBS_STREAM","")
        },timeout=5)
        if r.status_code==200 and r.json().get("IsSuccessful"):
          result["MES"]=1
      except: pass

    # 4) Trace PC
    if cfg["is_trace_enabled"]:
      try:
        r = requests.get(cfg["trace_process_control_url"],
                         params={"serial":barcode,"serial_type":"band"},
                         timeout=3)
        if r.status_code==200 and r.json().get("pass"):
          result["Process"]=1
      except: pass

    # 5) Trace Interlock
      try:
        r = requests.post(cfg["trace_interlock_url"],
                          params={"serial":barcode,"serial_type":"band"},
                          timeout=3)
        if r.status_code==200 and r.json().get("pass"):
          result["Trace"]=1
      except: pass

    # 6) return PLC write cmd
    return {"emit":True,"command":{
      "section":"status_bits",
      "tags":result,
      "request_id":str(uuid.uuid4())
    }}



# ─── Main loop ───────────────────────────────────────────────────
async def run():
    await init_kafka()
    print("[Processing Service] Started consuming... Press Ctrl+C to exit.")
    try:
        async for msg in kafka_consumer:
            topic   = msg.topic
            payload = msg.value
            print(f"[Received] topic={topic} payload={payload}")

            # 1) Audit the incoming
            pg_cur.execute(
                "INSERT INTO mes_trace_history(serial,step,response_json) VALUES(%s,%s,%s)",
                (payload.get("serial",""), topic, json.dumps(payload))
            )

            # 2) Business logic
            decision = process_event(topic, payload)
            if decision.get("emit"):
                cmd = decision["command"]

                # 3) Send to PLC write queue
                await kafka_producer.send_and_wait(PLC_WRITE_TOPIC, cmd)
                print(f"[Emitted→PLC] {cmd}")

                # 4) Log that command too
                pg_cur.execute(
                    "INSERT INTO mes_trace_history(serial,step,response_json) VALUES(%s,%s,%s)",
                    (payload.get("serial",""), "machine_commands", json.dumps(cmd))
                )

    except Exception as e:
        print(f"[Processing] Error in main loop: {e}")
    finally:
        await shutdown()

# ─── Entrypoint ───────────────────────────────────────────────────
if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    # Handle graceful shutdown
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.ensure_future(shutdown()))
    loop.run_until_complete(run())
