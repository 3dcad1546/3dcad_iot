import os, asyncio, json, signal, uuid, requests, httpx
from datetime import datetime
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import NoBrokersAvailable
import psycopg2
from psycopg2.extras import RealDictCursor

# ─── Configuration ────────────────────────────────────────────────
KAFKA_BROKER       = os.getenv("KAFKA_BROKER", "kafka:9092")
RAW_TOPIC          = os.getenv("RAW_KAFKA_TOPIC", "raw_server_data")
TRIGGER_TOPIC      = os.getenv("TRIGGER_KAFKA_TOPIC", "trigger_events")
PLC_WRITE_TOPIC    = os.getenv("PLC_WRITE_COMMANDS_TOPIC", "plc_write_commands")
CYCLE_EVENT_TOPIC  = os.getenv("CYCLE_EVENT_TOPIC", "cycle_event")
CONSUMER_GROUP     = os.getenv("CONSUMER_GROUP", "processing_service")
DB_URL             = os.getenv("DB_URL")
USER_LOGIN_URL     = os.getenv("USER_LOGIN_URL")

# ─── Postgres setup ───────────────────────────────────────────────
pg_conn = psycopg2.connect(DB_URL)
pg_conn.autocommit = True
pg_cur  = pg_conn.cursor(cursor_factory=RealDictCursor)

# ensure audit + cycle tables exist
pg_cur.execute("""
CREATE TABLE IF NOT EXISTS mes_trace_history (
  id SERIAL PRIMARY KEY,
  serial TEXT, step TEXT NOT NULL, response_json JSONB NOT NULL, ts TIMESTAMP DEFAULT NOW()
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
        mes_process_control_url AS pc_url,
        mes_upload_url,
        trace_process_control_url AS trace_pc_url,
        trace_interlock_url       AS trace_il_url,
        is_mes_enabled,
        is_trace_enabled
      FROM machine_config
      WHERE machine_id=%s
    """, (machine_id,))
    row = pg_cur.fetchone()
    if not row:
        raise RuntimeError(f"No machine_config for {machine_id}")
    return row

MACHINE_ID  = os.getenv("MACHINE_ID")
machine_cfg = load_machine_config(MACHINE_ID)

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
            print("[Processing] Kafka producer started.")
            break
        except NoBrokersAvailable:
            print("[Processing] Kafka unavailable, retrying…")
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
    print(f"[Processing] subscribed to {RAW_TOPIC}, {TRIGGER_TOPIC}")

async def shutdown():
    if kafka_consumer: await kafka_consumer.stop()
    if kafka_producer: await kafka_producer.stop()
    pg_conn.close()
    print("[Processing] shut down cleanly")


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
    # 2) Postgres
    pg_cur.execute(
      "INSERT INTO cycle_event(cycle_id,stage) VALUES(%s,%s)",
      (cycle_id, stage)
    )


# ─── Business logic ────────────────────────────────────────────────
async def process_event(topic: str, msg: dict) -> dict:
    if topic != TRIGGER_TOPIC:
        return {"emit": False}

    barcode = msg["barcode"]
    is_auto = msg.get("mode_auto", False)
    token   = msg.get("token")
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
    await publish_cycle_event(cycle_id, "InputStation")

    # 2) manual override
    if not is_auto or operator is None:
        await publish_cycle_event(cycle_id, "ManualMode")
        return {"emit":True, "command":{
          "section":"manual",
          "tag_name":"ManualMode",
          "value":1,
          "request_id":cycle_id
        }}

    # 3) MES PC
    if machine_cfg["is_mes_enabled"]:
        try:
            r = requests.post(machine_cfg["pc_url"], json={
              "UniqueId":barcode,
              "MachineId":MACHINE_ID,
              "OperatorId":operator,
              "Tools":[], "RawMaterials":[],
              "CbsStreamName":os.getenv("CBS_STREAM_NAME","")
            }, timeout=5)
            if r.status_code==200 and r.json().get("IsSuccessful"):
                await publish_cycle_event(cycle_id, "MES")
        except: pass

    # 4) Trace PC
    if machine_cfg["is_trace_enabled"]:
        try:
            r = requests.get(machine_cfg["trace_pc_url"],
                             params={"serial":barcode,"serial_type":"band"},
                             timeout=3)
            if r.status_code==200 and r.json().get("pass"):
                await publish_cycle_event(cycle_id, "Process")
        except: pass

        # 5) Trace Interlock
        try:
            r = requests.post(machine_cfg["trace_il_url"],
                              params={"serial":barcode,"serial_type":"band"},
                              timeout=3)
            if r.status_code==200 and r.json().get("pass"):
                await publish_cycle_event(cycle_id, "Trace")
        except: pass

    # 6) PLC write
    plc_cmd = {
      "section":"status_bits",
      "tags":{
        "MES":     1 if msg.get("MES") else 0,
        "Process": 1 if msg.get("Process") else 0,
        "Trace":   1 if msg.get("Trace") else 0
      },
      "request_id": cycle_id
    }
    await publish_cycle_event(cycle_id, "PLCWrite")
    return {"emit":True, "command":plc_cmd}


# ─── Main loop ─────────────────────────────────────────────────────
async def run():
    await init_kafka()
    print("[Processing] running… Ctrl+C to quit")
    try:
        async for msg in kafka_consumer:
            topic   = msg.topic
            payload = msg.value
            # audit input
            pg_cur.execute(
              "INSERT INTO mes_trace_history(serial,step,response_json) VALUES(%s,%s,%s)",
              (payload.get("serial",""), topic, json.dumps(payload))
            )
            out = await process_event(topic, payload)
            if out.get("emit"):
                cmd = out["command"]
                await kafka_producer.send_and_wait(PLC_WRITE_TOPIC, cmd)
                # audit PLC cmd
                pg_cur.execute(
                  "INSERT INTO mes_trace_history(serial,step,response_json) VALUES(%s,%s,%s)",
                  (payload.get("serial",""), "machine_commands", json.dumps(cmd))
                )
    except Exception as e:
        print("[Processing] Error:", e)
    finally:
        await shutdown()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.ensure_future(shutdown()))
    loop.run_until_complete(run())
