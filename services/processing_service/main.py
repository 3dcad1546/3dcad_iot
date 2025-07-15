import os
import asyncio
import json
import signal
import uuid
import pathlib
import logging

import requests
import httpx
from datetime import datetime

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import NoBrokersAvailable

import psycopg2
from psycopg2.extras import RealDictCursor


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger("processing-service")


# load just the bits map
map_path = os.path.join(os.path.dirname(__file__), "register_map.json")
with open(map_path, "r") as f:
    REGISTER_MAP = json.load(f)

# ─── Configuration ────────────────────────────────────────────────
KAFKA_BROKER            = os.getenv("KAFKA_BROKER", "kafka:9092")
RAW_TOPIC               = os.getenv("RAW_KAFKA_TOPIC", "raw_server_data")
TRIGGER_TOPIC           = os.getenv("TRIGGER_KAFKA_TOPIC", "trigger_events")
PLC_WRITE_TOPIC         = os.getenv("PLC_WRITE_COMMANDS_TOPIC", "plc_write_commands")
CYCLE_EVENT_TOPIC       = os.getenv("CYCLE_EVENT_TOPIC", "cycle_event")
CONSUMER_GROUP          = os.getenv("CONSUMER_GROUP", "processing_service")

DB_URL                  = os.getenv("DB_URL")
USER_LOGIN_URL          = os.getenv("USER_LOGIN_URL")



# ─── Postgres setup ───────────────────────────────────────────────
pg_conn = psycopg2.connect(DB_URL)
print(pg_conn,"pg_conn")
pg_conn.autocommit = True
pg_cur  = pg_conn.cursor(cursor_factory=RealDictCursor)


# ensure audit + cycle tables exist
pg_cur.execute("""
CREATE TABLE IF NOT EXISTS mes_trace_history (
  id SERIAL PRIMARY KEY,
  serial TEXT,
  step   TEXT NOT NULL,
  response_json JSONB NOT NULL,
  ts TIMESTAMP DEFAULT NOW()
);
""")
pg_cur.execute("""
CREATE TABLE IF NOT EXISTS cycle_master (
  cycle_id UUID PRIMARY KEY,
  operator TEXT NOT NULL,
  shift_id INTEGER NOT NULL REFERENCES shift_master(id),
  variant TEXT NOT NULL,
  barcode TEXT NOT NULL,
  start_ts TIMESTAMP NOT NULL DEFAULT NOW(),
  end_ts TIMESTAMP,
  unload_status BOOLEAN NOT NULL DEFAULT FALSE,
  unload_ts TIMESTAMP
);
""")
pg_cur.execute("""
CREATE TABLE IF NOT EXISTS cycle_event (
  id SERIAL PRIMARY KEY,
  cycle_id UUID NOT NULL REFERENCES cycle_master(cycle_id),
  stage TEXT NOT NULL,
  ts TIMESTAMP NOT NULL DEFAULT NOW()
);
""")

# ─── Load machine_config ──────────────────────────────────────────
def load_machine_config(machine_id: str):
    pg_cur.execute("""
      SELECT
        mes_process_control_url    AS pc_url,
        mes_upload_url,
        trace_process_control_url  AS trace_pc_url,
        trace_interlock_url        AS trace_il_url,
        is_mes_enabled,
        is_trace_enabled
      FROM machine_config
      WHERE machine_id=%s
    """, (machine_id,))
    row = pg_cur.fetchone()
    if not row:
        raise RuntimeError(f"No machine_config for {machine_id}")
    return row

MACHINE_ID   = os.getenv("MACHINE_ID")
machine_cfg  = load_machine_config(MACHINE_ID)

# ─── Kafka clients ─────────────────────────────────────────────────
kafka_consumer = None
kafka_producer = None

async def init_kafka():
    global kafka_consumer, kafka_producer
    # producer
    for _ in range(5):
        try:
            kafka_producer = AIOKafkaProducer(
              bootstrap_servers=KAFKA_BROKER,
              value_serializer=lambda v: json.dumps(v).encode()
            )
            await kafka_producer.start()
            logger.info("Kafka producer started.")
            break
        except NoBrokersAvailable:
            logger.warning("Kafka unavailable, retrying…")
            await asyncio.sleep(3)
    else:
        raise RuntimeError("Cannot connect to Kafka producer")

    # consumer
    kafka_consumer = AIOKafkaConsumer(
      RAW_TOPIC, TRIGGER_TOPIC,
      bootstrap_servers=KAFKA_BROKER,
      group_id=CONSUMER_GROUP,
      value_deserializer=lambda b: json.loads(b.decode())
    )
    await kafka_consumer.start()
    logger.info(f"Subscribed to {RAW_TOPIC}, {TRIGGER_TOPIC}")

async def shutdown():
    if kafka_consumer: await kafka_consumer.stop()
    if kafka_producer: await kafka_producer.stop()
    pg_conn.close()
    logger.info("Shut down cleanly")

# ─── Helpers ───────────────────────────────────────────────────────
async def get_current_operator(token: str) -> str:
    url = f"{USER_LOGIN_URL}/api/verify"
    headers = {"X-Auth-Token": token}
    async with httpx.AsyncClient() as client:
        r = await client.get(url, headers=headers, timeout=2)
        r.raise_for_status()
        return r.json()["username"]

def now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"

async def publish_cycle_event(cycle_id: str, stage: str):
    evt = {"cycle_id": cycle_id, "stage": stage, "ts": now_iso()}
    # 1) Kafka
    await kafka_producer.send_and_wait(CYCLE_EVENT_TOPIC, evt)
    logger.info("Kafka event sent — topic: %s, payload: %s", CYCLE_EVENT_TOPIC, evt)
    # 2) Postgres
    pg_cur.execute(
      "INSERT INTO cycle_event(cycle_id,stage) VALUES(%s,%s)",
      (cycle_id, stage)
    )
    pg_conn.commit()
    logger.info("cycle_event inserted and committed — cycle_id: %s, stage: %s", cycle_id, stage)

# ─── Business logic ────────────────────────────────────────────────
async def process_event(topic: str, msg: dict) -> dict:
    if topic != TRIGGER_TOPIC:
        return {"emit": False}

    barcode  = msg["barcode"]
    # Remove null bytes from barcode
    if barcode:
        original = barcode
        barcode = barcode.replace("\x00", "")
        if original != barcode:
            logger.info(f"Removed null bytes from barcode: '{original}' -> '{barcode}'")
        
    is_auto  = msg.get("mode_auto", False)
    token    = msg.get("token")
    operator = await get_current_operator(token) if token else None

    # 1) start cycle
    cycle_id = str(uuid.uuid4())
    # lookup shift
    pg_cur.execute("""
      SELECT id FROM shift_master
       WHERE (start_time < end_time AND start_time <= NOW()::time AND NOW()::time < end_time)
          OR (start_time > end_time AND (NOW()::time >= start_time OR NOW()::time < end_time))
      LIMIT 1
    """)
    
    row = pg_cur.fetchone()
    shift_id = row["id"] if row else None

    pg_cur.execute("""
      INSERT INTO cycle_master(cycle_id,operator,shift_id,variant,barcode)
      VALUES (%s,%s,%s,%s,%s)
    """, (cycle_id, operator, shift_id, "", barcode))
    pg_conn.commit()
    await publish_cycle_event(cycle_id, "InputStation")
    
    cmds = []
    # 2) manual override
    if not is_auto or operator is None:
        await publish_cycle_event(cycle_id, "ManualMode")
        cmds.append({
          "section":  "manual_station",
          "tag_name": "ManualMode",
          "value":    1,
          "request_id": cycle_id
        })
        return {"commands": cmds}

    # 3) MES PC
    if machine_cfg["is_mes_enabled"]:
        r = requests.post(machine_cfg["pc_url"], json={}, timeout=5)
        if r.status_code == 200 and r.json().get("IsSuccessful"):
            await publish_cycle_event(cycle_id, "MES")
            cmds.append({
              "section":  "mes_upload",
              "tag_name": "MESUpload",
              "value":    1,
              "request_id": cycle_id
            })

    # 4) Trace PC
    if machine_cfg["is_trace_enabled"]:
        r = requests.get(machine_cfg["trace_pc_url"],
                         params={"serial":barcode,"serial_type":"band"}, timeout=3)
        if r.status_code == 200 and r.json().get("pass"):
            await publish_cycle_event(cycle_id, "Process")
            cmds.append({
              "section":  "process_station",
              "tag_name": "ProcessPassing",
              "value":    1,
              "request_id": cycle_id
            })

        # 5) Interlock
        r = requests.post(machine_cfg["trace_il_url"],
                          params={"serial":barcode,"serial_type":"band"}, timeout=3)
        if r.status_code == 200 and r.json().get("pass"):
            await publish_cycle_event(cycle_id, "Trace")
            cmds.append({
              "section":  "trace_upload",
              "tag_name": "TraceUpload",
              "value":    1,
              "request_id": cycle_id
            })

    


    # 6) PLC write — now using register_map.json for addresses & bits
    status_map = REGISTER_MAP["status_bits"]["write"]
    plc_cmd = {
        "section": "status_bits",
        "tags": {
            tag_name: {
                "address": addr,
                "bit":      bit,
                "value":    1 if msg.get(tag_name) else 0
            }
            for tag_name, (addr, bit) in status_map.items()
        },
        "request_id": cycle_id
    }
    await publish_cycle_event(cycle_id, "PLCWrite")
    return {"commands": cmds}

# ─── Main loop ─────────────────────────────────────────────────────
async def run():
    await init_kafka()
    logger.info("Running… Ctrl+C to quit")
    try:
        async for msg in kafka_consumer:
            topic   = msg.topic
            payload = msg.value
            # audit input
            pg_cur.execute(
              "INSERT INTO mes_trace_history(serial,step,response_json) VALUES(%s,%s,%s)",
              (payload.get("serial",""), topic, json.dumps(payload))
            )
            pg_conn.commit()
            out = await process_event(topic, payload)
            for cmd in out.get("commands", []):
                await kafka_producer.send_and_wait(PLC_WRITE_TOPIC, cmd)
                # audit PLC command
                pg_cur.execute(
                "INSERT INTO mes_trace_history(serial,step,response_json) VALUES(%s,%s,%s)",
                (payload.get("barcode",""), "machine_commands", json.dumps(cmd))
                )
                pg_conn.commit()

    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        await shutdown()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.ensure_future(shutdown()))
    loop.run_until_complete(run())
