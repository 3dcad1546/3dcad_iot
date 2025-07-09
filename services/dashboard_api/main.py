import os
import json
import time
import threading
import select
import asyncio
import requests
import psycopg2
import psycopg2.extensions
import uuid
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException, Depends, WebSocket, WebSocketDisconnect, Query, Header
from pydantic import BaseModel, Field
from typing import Optional, List, Dict
from influxdb_client import InfluxDBClient, WriteOptions
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError, NoBrokersAvailable as AIOKafkaNoBrokersAvailable
from fastapi.middleware.cors import CORSMiddleware 

# ─── Configuration ──────────────────────────────────────────────
PLC_WRITE_COMMANDS_TOPIC   = os.getenv("PLC_WRITE_COMMANDS_TOPIC",   "plc_write_commands")
PLC_WRITE_RESPONSES_TOPIC  = os.getenv("PLC_WRITE_RESPONSES_TOPIC",  "plc_write_responses")
MACHINE_STATUS_TOPIC       = os.getenv("MACHINE_STATUS",             "machine_status")
STARTUP_STATUS             = os.getenv("STARTUP_STATUS",             "startup_status")
MANUAL_STATUS              = os.getenv("MANUAL_STATUS",              "manual_status")
AUTO_STATUS                = os.getenv("AUTO_STATUS",                "auto_status")
ROBO_STATUS                = os.getenv("ROBO_STATUS",                "robo_status")
IO_STATUS                  = os.getenv("IO_STATUS",                  "io_status")
OEE_STATUS                 = os.getenv("OEE_STATUS",                 "oee_status")
ON_OFF_STATUS              = os.getenv("ON_OFF_STATUS",              "on_off_status")

# ─── FastAPI App ─────────────────────────────────────────────────
app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ─── PostgreSQL Setup ──────────────────────────────────────────────
DB_URL = os.getenv("DB_URL")
for _ in range(10):
    try:
        conn = psycopg2.connect(DB_URL)
        break
    except psycopg2.OperationalError:
        print("PostgreSQL not ready, retrying in 5s…")
        time.sleep(5)
else:
    raise RuntimeError("PostgreSQL not available")
conn.autocommit = True
cur = conn.cursor(cursor_factory=RealDictCursor)

# ─── Hot-Reloadable Config ────────────────────────────────────────
_config_cache: Dict[str,str] = {}

def load_config() -> Dict[str,str]:
    cur.execute("SELECT key, value FROM config")
    return {k:v for k,v in cur.fetchall()}

def listen_for_config_updates():
    lc_conn = psycopg2.connect(DB_URL)
    lc_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    lc = lc_conn.cursor()
    lc.execute("LISTEN config_update;")
    while True:
        if select.select([lc_conn],[],[],5) == ([],[],[]):
            continue
        lc_conn.poll()
        while lc_conn.notifies:
            note = lc_conn.notifies.pop(0)
            key = note.payload
            cur.execute("SELECT value FROM config WHERE key=%s", (key,))
            row = cur.fetchone()
            if row:
                _config_cache[key] = row[0]

_config_cache = load_config()
threading.Thread(target=listen_for_config_updates, daemon=True).start()

def get_cfg(key: str, default=None):
    return _config_cache.get(key, default)

# ─── Kafka Bootstrap Setting ──────────────────────────────────────
KAFKA_BOOTSTRAP = get_cfg("KAFKA_BROKER", "kafka:9092")

# ─── InfluxDB Setup ────────────────────────────────────────────────
influx_client = InfluxDBClient(
    url=get_cfg("INFLUXDB_URL",   "http://influxdb:8086"),
    token=get_cfg("INFLUXDB_TOKEN","edgetoken"),
    org=get_cfg("INFLUXDB_ORG",   "EdgeOrg")
)
write_api = influx_client.write_api(write_options=WriteOptions(batch_size=1, flush_interval=1000))
query_api = influx_client.query_api()

# ─── Models ───────────────────────────────────────────────────────
class LoginRequest(BaseModel):
    Username: str
    Password: str
    Process: Optional[str] = ""

class ScanRequest(BaseModel):
    serial: str
    result: str  # "pass" | "fail" | "scrap"

class PlcWriteCommand(BaseModel):
    section: str
    tag_name: str
    value:  float | int | str
    request_id: Optional[str] = None

class CycleEvent(BaseModel):
    stage: str
    ts: datetime

class CycleReportItem(BaseModel):
    cycle_id: str
    operator: str
    shift_id: int
    variant: Optional[str]
    barcode: str
    start_ts: datetime
    end_ts: Optional[datetime]
    events: List[CycleEvent]

# ─── Auth Guard ───────────────────────────────────────────────────
USER_SVC_URL = os.getenv("USER_SVC_URL", "http://user_login:8001")

def require_login(token: str = Header(None, alias="X-Auth-Token")) -> str:
    if not token:
        raise HTTPException(401, "Missing auth token")
    resp = requests.get(f"{USER_SVC_URL}/api/verify",
                        headers={"X-Auth-Token": token}, timeout=3)
    if resp.status_code != 200:
        raise HTTPException(401, "Invalid or expired session")
    return resp.json()["username"]

async def websocket_auth(
    websocket: WebSocket,
    token: Optional[str] = Query(None),
    auth_header: Optional[str] = Header(None, alias="X-Auth-Token")
) -> str:
    tk = token or auth_header
    if not tk:
        raise HTTPException(401,"Missing auth token")
    resp = requests.get(f"{USER_SVC_URL}/api/verify",
                        headers={"X-Auth-Token": tk}, timeout=3)
    if resp.status_code != 200:
        raise HTTPException(401,"Invalid or expired session")
    return resp.json()["username"]

# ─── Config Endpoints ─────────────────────────────────────────────
@app.post("/api/config")
def update_config(item: Dict[str,str], user: str = Depends(require_login)):
    cur.execute("UPDATE config SET value=%s WHERE key=%s",
                (item["value"], item["key"]))
    cur.execute("NOTIFY config_update, %s", (item["key"],))
    return {"message": f"Config {item['key']} updated"}

@app.get("/api/config")
def get_config(user: str = Depends(require_login)):
    return _config_cache

# ─── Health ───────────────────────────────────────────────────────
@app.get("/api/health")
def health():
    return {"status":"ok"}

# ─── Scan Orchestration ────────────────────────────────────────────
@app.post("/api/scan")
def scan_part(req: ScanRequest, user: str = Depends(require_login)):
    serial = req.serial
    result = {}
    try:
        # MES Process Control
        pc = requests.post(
            get_cfg("MES_PROCESS_CONTROL_URL"),
            json={
                "UniqueId": serial,
                "MachineId": get_cfg("MACHINE_ID"),
                "OperatorId": user,
                "Tools": [], "RawMaterials": [],
                "CbsStreamName": get_cfg("CBS_STREAM_NAME")
            }, timeout=5
        )
        if pc.status_code!=200 or not pc.json().get("IsSuccessful"):
            raise HTTPException(400, f"MES PC failed: {pc.text}")
        result["mes_pc"] = pc.json()
        cur.execute(
            "INSERT INTO mes_trace_history(serial,step,response_json,ts) VALUES(%s,%s,%s::jsonb,NOW())",
            (serial, "mes_pc", json.dumps(result["mes_pc"]))
        )

        # Trace Process Control
        tp = requests.get(
            f"http://{get_cfg('TRACE_PROXY_HOST')}/v2/process_control",
            params={"serial":serial,"serial_type":"band"}, timeout=3
        )
        tp.raise_for_status()
        tpj = tp.json()
        if not tpj.get("pass"):
            raise HTTPException(400, f"Trace PC failed: {tpj}")
        result["trace_pc"] = tpj
        cur.execute(
            "INSERT INTO mes_trace_history(serial,step,response_json,ts) VALUES(%s,%s,%s::jsonb,NOW())",
            (serial,"trace_pc",json.dumps(tpj))
        )

        # Trace Interlock
        il = requests.post(
            f"http://{get_cfg('TRACE_PROXY_HOST')}/interlock",
            params={"serial":serial,"serial_type":"band"}, timeout=3
        )
        il.raise_for_status()
        ilj = il.json()
        if not ilj.get("pass"):
            raise HTTPException(400, f"Interlock failed: {ilj}")
        result["interlock"] = ilj
        cur.execute(
            "INSERT INTO mes_trace_history(serial,step,response_json,ts) VALUES(%s,%s,%s::jsonb,NOW())",
            (serial,"interlock",json.dumps(ilj))
        )

        # MES Upload
        payload = {
            "UniqueIds":   [serial],
            "MachineIds":  [get_cfg("MACHINE_ID")],
            "OperatorIds": [user],
            "Tools":[], "RawMaterials":[],
            "DateTimeStamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "CbsStreamName": get_cfg("CBS_STREAM_NAME")
        }
        if req.result=="pass":
            payload.update({"IsPass":True,"IsFail":False,"IsScrap":False,"Defects":[]})
        elif req.result=="fail":
            payload.update({
                "IsPass":False,"IsFail":True,"IsScrap":False,
                "FailureDefects":[{"FailureReasonCodes":get_cfg("FAILURE_REASON_CODES").split(","),"NcmReasonCodes":get_cfg("NCM_REASON_CODES").split(",")}]
            })
        else:  # scrap
            payload.update({"IsPass":False,"IsFail":False,"IsScrap":True,"Defects":[{"DefectCode":"Trace Scrap","Quantity":1}]})
        mu = requests.post(get_cfg("MES_UPLOAD_URL"),json=payload,timeout=5)
        if mu.status_code!=200 or not mu.json().get("IsSuccessful"):
            raise HTTPException(400, f"MES upload failed: {mu.text}")
        result["mes_upload"] = mu.json()
        cur.execute(
            "INSERT INTO mes_trace_history(serial,step,response_json,ts) VALUES(%s,%s,%s::jsonb,NOW())",
            (serial,"mes_upload",json.dumps(result["mes_upload"]))
        )

        # Trace Data Log
        td = requests.post(
            f"http://{get_cfg('TRACE_PROXY_HOST')}/v2/logs",
            json={"serials":{"part_id":serial}}, timeout=5
        )
        if td.status_code!=200:
            raise HTTPException(400, f"Trace log failed: {td.text}")
        result["trace_log"] = td.json()
        cur.execute(
            "INSERT INTO mes_trace_history(serial,step,response_json,ts) VALUES(%s,%s,%s::jsonb,NOW())",
            (serial,"trace_log",json.dumps(result["trace_log"]))
        )

        # Final audit
        cur.execute(
            "INSERT INTO scan_audit(serial,operator,result,ts) VALUES(%s,%s,%s,NOW())",
            (serial,user,req.result)
        )
        return result

    except HTTPException as he:
        cur.execute(
            "INSERT INTO error_logs(context,error_msg,details,ts) VALUES(%s,%s,%s::jsonb,NOW())",
            ("scan",str(he.detail),json.dumps({"serial":serial,"error":he.detail}))
        )
        raise
    except Exception as e:
        cur.execute(
            "INSERT INTO error_logs(context,error_msg,details,ts) VALUES(%s,%s,%s::jsonb,NOW())",
            ("scan",str(e),json.dumps({"serial":serial}))
        )
        raise HTTPException(500,"Internal server error")

# ─── Sensors Endpoints ─────────────────────────────────────────────
@app.get("/api/sensors/latest")
def sensors_latest(source: str, user: str = Depends(require_login)):
    flux = f'''
      from(bucket:"{get_cfg("INFLUXDB_BUCKET")}")
        |> range(start:-1m)
        |> filter(fn:(r)=>r._measurement=="sensor_data" and r.source=="{source}")
        |> last()
    '''
    tables = query_api.query(flux)
    if not tables or not tables[0].records:
        raise HTTPException(404,"No data")
    rec = tables[0].records[-1]
    return {"time":rec.get_time().isoformat(),"value":rec.get_value(),"source":rec.values.get("source")}

@app.get("/api/sensors/history")
def sensors_history(source: str, hours: int=1, user: str=Depends(require_login)):
    flux = f'''
      from(bucket:"{get_cfg("INFLUXDB_BUCKET")}")
        |> range(start:-{hours}h)
        |> filter(fn:(r)=>r._measurement=="sensor_data" and r.source=="{source}")
        |> keep(columns:["_time","_value","source"])
    '''
    tables = query_api.query(flux)
    data = [
      {"time":r.get_time().isoformat(),"value":r.get_value(),"source":r.values.get("source")}
      for tbl in tables for r in tbl.records
    ]
    if not data:
        raise HTTPException(404,"No history")
    return data

# ─── OEE Endpoints ───────────────────────────────────────────────────
@app.get("/api/oee/current")
def oee_current(user: str = Depends(require_login)):
    flux = f'''
      from(bucket:"{get_cfg("INFLUXDB_BUCKET")}")
        |> range(start:-5m)
        |> filter(fn:(r)=>r._measurement=="oee")
        |> last()
    '''
    tables = query_api.query(flux)
    if not tables or not tables[0].records:
        raise HTTPException(404,"No OEE data")
    rec = tables[0].records[-1]
    return {"time":rec.get_time().isoformat(), **{k:rec.values[k] for k in rec.values if not k.startswith("_")}}

@app.get("/api/oee/history")
def oee_history(
    hours: int = Query(1, ge=0),
    start: Optional[datetime]=Query(None),
    end:   Optional[datetime]=Query(None),
    user:  str=Depends(require_login)
):
    if start and end:
        t0, t1 = start, end
    else:
        t1 = datetime.utcnow()
        t0 = t1 - timedelta(hours=hours)
    start_ts = t0.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_ts   = t1.strftime("%Y-%m-%dT%H:%M:%SZ")
    flux = f'''
      from(bucket:"{get_cfg("INFLUXDB_BUCKET")}")
        |> range(start:{start_ts}, stop:{end_ts})
        |> filter(fn:(r)=>r._measurement=="oee")
        |> pivot(rowKey:["__time"],columnKey:["_field"],valueColumn:"_value")
    '''
    tables = query_api.query(flux)
    data = []
    for tbl in tables:
        for rec in tbl.records:
            e = {"time":rec.get_time().isoformat()}
            e.update({k:rec.values[k] for k in rec.values if not k.startswith("_")})
            data.append(e)
    if not data:
        raise HTTPException(404,f"No OEE in {start_ts}→{end_ts}")
    return data

# ─── Cycle Reporting Endpoint ──────────────────────────────────────
@app.get("/api/cycles", response_model=List[CycleReportItem])
def get_cycles(
    operator: Optional[str]      = None,
    shift_id: Optional[int]      = None,
    barcode: Optional[str]       = None,
    variant: Optional[str]       = None,
    from_ts: Optional[datetime]  = Query(None, alias="from"),
    to_ts:   Optional[datetime]  = Query(None, alias="to"),
    user: str = Depends(require_login)
):
    clauses, params = [], []
    if operator:
        clauses.append("cm.operator=%s");    params.append(operator)
    if shift_id is not None:
        clauses.append("cm.shift_id=%s");   params.append(shift_id)
    if barcode:
        clauses.append("cm.barcode=%s");    params.append(barcode)
    if variant:
        clauses.append("cm.variant=%s");    params.append(variant)
    if from_ts:
        clauses.append("cm.start_ts>=%s");  params.append(from_ts)
    if to_ts:
        clauses.append("cm.start_ts<=%s");  params.append(to_ts)
    where = " AND ".join(clauses) if clauses else "TRUE"

    sql = f"""
    SELECT
      cm.cycle_id, cm.operator, cm.shift_id, cm.variant, cm.barcode,
      cm.start_ts, cm.end_ts,
      jsonb_agg(jsonb_build_object('stage',ce.stage,'ts',ce.ts)
                ORDER BY ce.id) AS events
    FROM cycle_master cm
    LEFT JOIN cycle_event ce ON ce.cycle_id=cm.cycle_id
    WHERE {where}
    GROUP BY cm.cycle_id
    ORDER BY cm.start_ts DESC
    """
    cur.execute(sql, params)
    rows = cur.fetchall()

    result: List[CycleReportItem] = []
    for r in rows:
        item = CycleReportItem(
            cycle_id = r["cycle_id"],
            operator = r["operator"],
            shift_id = r["shift_id"],
            variant  = r["variant"],
            barcode  = r["barcode"],
            start_ts = r["start_ts"],
            end_ts   = r["end_ts"],
            events   = [CycleEvent(**e) for e in (r["events"] or [])]
        )
        result.append(item)
    return result

# ─── WebSocket Manager ────────────────────────────────────────────
WS_TOPICS = {
    "machine-status": MACHINE_STATUS_TOPIC,
    "startup-status": STARTUP_STATUS,
    "manual-status":  MANUAL_STATUS,
    "auto-status":    AUTO_STATUS,
    "robo-status":    ROBO_STATUS,
    "io-status":      IO_STATUS,
    "oee-status":     OEE_STATUS,
    "plc-write-responses": PLC_WRITE_RESPONSES_TOPIC
}

class ConnectionManager:
    def __init__(self):
        self.active: Dict[str, set[WebSocket]] = {s:set() for s in WS_TOPICS}
        self.pending: Dict[str, WebSocket]   = {}

    async def connect(self, stream: str, ws: WebSocket):
        await ws.accept()
        self.active.setdefault(stream,set()).add(ws)

    def disconnect(self, stream: str, ws: WebSocket):
        self.active.get(stream,set()).discard(ws)

    async def broadcast(self, stream: str, msg: str):
        conns = self.active.get(stream, set())
        dead = []
        for ws in conns:
            try:
                await ws.send_text(msg)
            except:
                dead.append(ws)
        for ws in dead:
            conns.remove(ws)

    async def send_write_response(self, request_id: str, payload: dict):
        ws = self.pending.pop(request_id, None)
        if ws:
            try:
                await ws.send_json({"type":"plc_write_response","data":payload})
            except:
                pass

mgr = ConnectionManager()

# ─── Kafka→WebSocket ──────────────────────────────────────────────
async def kafka_to_ws(name: str, topic: str):
    for i in range(10):
        try:
            consumer = AIOKafkaConsumer(
                topic,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                auto_offset_reset="latest",
                value_deserializer=lambda b: b.decode()
            )
            await consumer.start()
            break
        except AIOKafkaNoBrokersAvailable:
            await asyncio.sleep(5)
    else:
        raise RuntimeError(f"Cannot connect consumer to {topic}")

    try:
        async for msg in consumer:
            await mgr.broadcast(name, msg.value)
    finally:
        await consumer.stop()

# ─── Kafka Producer for WebSocket PLC writes ───────────────────────
kafka_producer: Optional[AIOKafkaProducer] = None

async def init_kafka_producer():
    global kafka_producer
    for i in range(10):
        try:
            p = AIOKafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode()
            )
            await p.start()
            kafka_producer = p
            return
        except AIOKafkaNoBrokersAvailable:
            await asyncio.sleep(5)
    raise RuntimeError("Cannot start Kafka producer")

# ─── Listen for PLC write responses ────────────────────────────────
async def listen_for_plc_write_responses():
    for i in range(10):
        try:
            consumer = AIOKafkaConsumer(
                PLC_WRITE_RESPONSES_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                auto_offset_reset="latest",
                value_deserializer=lambda b: json.loads(b.decode())
            )
            await consumer.start()
            break
        except AIOKafkaNoBrokersAvailable:
            await asyncio.sleep(5)
    else:
        raise RuntimeError("Cannot start response consumer")

    try:
        async for msg in consumer:
            payload = msg.value
            request_id = payload.get("request_id")
            if request_id:
                await mgr.send_write_response(request_id, payload)
    finally:
        await consumer.stop()

# ─── WebSocket Endpoints ──────────────────────────────────────────
@app.websocket("/ws/plc-write")
async def ws_plc_write(ws: WebSocket, operator: str = Depends(websocket_auth)):
    await mgr.connect("plc-write-responses", ws)
    try:
        while True:
            data = await ws.receive_json()
            cmd = PlcWriteCommand(**data)
            rid = cmd.request_id or str(uuid.uuid4())
            cmd.request_id = rid
            mgr.pending[rid] = ws
            if not kafka_producer:
                raise RuntimeError("Kafka producer not initialized")
            await kafka_producer.send_and_wait(PLC_WRITE_COMMANDS_TOPIC, cmd.dict())
            await ws.send_json({"type":"ack","status":"pending","request_id":rid})
    except WebSocketDisconnect:
        mgr.disconnect("plc-write-responses", ws)

@app.websocket("/ws/{stream}")
async def ws_stream(stream: str, ws: WebSocket, operator: str = Depends(websocket_auth)):
    if stream not in WS_TOPICS:
        await ws.close(code=1008, reason="Unknown stream")
        return
    await mgr.connect(stream, ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        mgr.disconnect(stream, ws)

async def consume_machine_status_and_populate_db():
    # 1) Create consumer
    consumer = AIOKafkaConsumer(
        MACHINE_STATUS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        value_deserializer=lambda b: json.loads(b.decode())
    )
    await consumer.start()

    try:
        async for msg in consumer:
            payload = msg.value
            ts      = payload["ts"]
            sets    = payload.get("sets", [])

            for s in sets:
                cycle_id = s["set_id"]               # e.g. "BC1|BC2"
                bc1, bc2 = s["barcodes"]
                prog     = s["progress"]             # { station: {status_1, status_2, ts}, … }

                # 2) Upsert cycle_master
                cur.execute("""
                    INSERT INTO cycle_master(cycle_id, barcode, start_ts)
                         VALUES (%s, %s, %s)
                    ON CONFLICT (cycle_id) DO NOTHING
                """, (cycle_id, f"{bc1}|{bc2}", s["created_ts"]))

                # 3) For each station whose status_1 just turned ON and
                #    there is no existing cycle_event for (cycle_id, station):
                for stage, vals in prog.items():
                    if vals["status_1"] == 1:
                        # check if we've already logged this event
                        cur.execute("""
                            SELECT 1 FROM cycle_event
                             WHERE cycle_id=%s AND stage=%s
                        """, (cycle_id, stage))
                        if cur.fetchone() is None:
                            # insert the event
                            cur.execute("""
                                INSERT INTO cycle_event(cycle_id, stage, ts)
                                     VALUES(%s, %s, %s)
                            """, (cycle_id, stage, vals["ts"]))

                # 4) If unload_station.status_1 == 1 → finalize cycle
                unload = prog.get("unload_station", {})
                if unload.get("status_1") == 1:
                    cur.execute("""
                        UPDATE cycle_master
                           SET end_ts = %s
                         WHERE cycle_id = %s
                           AND end_ts IS NULL
                    """, (unload["ts"], cycle_id))

    finally:
        await consumer.stop()
# ─── Startup / Shutdown ───────────────────────────────────────────
@app.on_event("startup")
async def on_startup():
    await init_kafka_producer()
    for name, topic in WS_TOPICS.items():
        asyncio.create_task(kafka_to_ws(name, topic))
    asyncio.create_task(consume_machine_status_and_populate_db())
    asyncio.create_task(listen_for_plc_write_responses())

@app.on_event("shutdown")
async def on_shutdown():
    if kafka_producer:
        await kafka_producer.stop()
    if conn:
        conn.close()
    if influx_client:
        influx_client.close()
