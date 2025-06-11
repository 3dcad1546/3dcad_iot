import os
import json
import time
import threading
import select
import asyncio
import requests
import psycopg2
import psycopg2.extensions

from fastapi import FastAPI, HTTPException, Depends, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
from typing import Optional
from influxdb_client import InfluxDBClient, WritePrecision
from aiokafka import AIOKafkaConsumer

app = FastAPI()

# ─── PostgreSQL Setup ──────────────────────────────────────────────
DB_URL = os.getenv("DB_URL")
conn = psycopg2.connect(DB_URL)
conn.autocommit = True
cur = conn.cursor()

# ─── Hot Reloadable Config ─────────────────────────────────────────
_config_cache: dict[str, str] = {}

def load_config():
    cur.execute("SELECT key, value FROM config")
    return {k: v for k, v in cur.fetchall()}

def listen_for_config_updates():
    listen_conn = psycopg2.connect(DB_URL)
    listen_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    lc = listen_conn.cursor()
    lc.execute("LISTEN config_update;")
    while True:
        if select.select([listen_conn], [], [], 5) == ([], [], []):
            continue
        listen_conn.poll()
        while listen_conn.notifies:
            note = listen_conn.notifies.pop(0)
            key = note.payload
            cur.execute("SELECT value FROM config WHERE key = %s", (key,))
            row = cur.fetchone()
            if row:
                _config_cache[key] = row[0]

_config_cache = load_config()
threading.Thread(target=listen_for_config_updates, daemon=True).start()

def get_cfg(key: str, default=None):
    return _config_cache.get(key, default)

# ─── InfluxDB Setup ────────────────────────────────────────────────
influx_client = InfluxDBClient(
    url=get_cfg("INFLUXDB_URL", "http://influxdb:8086"),
    token=get_cfg("INFLUXDB_TOKEN", "edgetoken"),
    org=get_cfg("INFLUXDB_ORG", "EdgeOrg")
)
write_api = influx_client.write_api(write_options=WritePrecision.S)
query_api = influx_client.query_api()

# ─── Models & Auth Guard ───────────────────────────────────────────
class LoginRequest(BaseModel):
    Username: str
    Password: str
    Process: Optional[str] = ""

class ScanRequest(BaseModel):
    serial: str
    result: str  # "pass" | "fail" | "scrap"

_current_operator: Optional[str] = None
def require_login():
    if not _current_operator:
        raise HTTPException(401, "Operator not logged in")

# ─── Config API ─────────────────────────────────────────────────────
@app.post("/api/config")
def update_config(item: dict, _=Depends(require_login)):
    cur.execute("UPDATE config SET value = %s WHERE key = %s", (item["value"], item["key"]))
    cur.execute("NOTIFY config_update, %s", (item["key"],))
    return {"message": f"Config {item['key']} updated"}

@app.get("/api/config")
def get_config():
    return _config_cache

# ─── Login & Logout ─────────────────────────────────────────────────────────
@app.post("/api/login")
def login(req: LoginRequest):
    global _current_operator
    allowed_ops = get_cfg("OPERATOR_IDS", "").split(",")
    if req.Username not in allowed_ops:
        raise HTTPException(403, f"Operator '{req.Username}' not permitted")

    body = {
      "MachineId":     get_cfg("MACHINE_ID"),
      "Process":       req.Process,
      "Username":      req.Username,
      "Password":      req.Password,
      "CbsStreamName": get_cfg("CBS_STREAM_NAME")
    }
    resp = requests.post(get_cfg("MES_OPERATOR_LOGIN_URL"), json=body)
    if resp.status_code != 200 or not resp.json().get("IsSuccessful"):
        raise HTTPException(resp.status_code, "Login failed")

    _current_operator = req.Username
    cur.execute(
        "INSERT INTO operator_sessions(username, login_ts) VALUES(%s, NOW())",
        (req.Username,)
    )
    return {"message": "Login successful"}

@app.post("/api/logout")
def logout():
    global _current_operator
    _current_operator = None
    return {"message": "Logged out"}

# ─── Health ─────────────────────────────────────────────────────────
@app.get("/api/health")
def health():
    return {"status": "ok"}

# ─── Scan Orchestration ─────────────────────────────────────────────────────
@app.post("/api/scan")
def scan_part(req: ScanRequest, _=Depends(require_login)):
    serial = req.serial
    result = {}

    try:
        # 1) MES PC-based Process Control
        pc = requests.post(
            get_cfg("MES_PROCESS_CONTROL_URL"),
            json={
                "UniqueId":      serial,
                "MachineId":     get_cfg("MACHINE_ID"),
                "OperatorId":    _current_operator,
                "Tools":         [],
                "RawMaterials":  [],
                "CbsStreamName": get_cfg("CBS_STREAM_NAME")
            }
        )
        if pc.status_code != 200 or not pc.json().get("IsSuccessful"):
            raise HTTPException(400, f"MES PC failed: {pc.text}")
        result["mes_pc"] = pc.json()
        cur.execute(
            "INSERT INTO mes_trace_history(serial, step, response_json, ts) VALUES(%s,%s,%s::jsonb,NOW())",
            (serial, "mes_pc", json.dumps(result["mes_pc"]))
        )

        # 2) Trace Process Control
        tp = requests.get(
            f"http://{get_cfg('TRACE_PROXY_HOST')}/v2/process_control",
            params={"serial": serial, "serial_type": "band"}
        ); tp.raise_for_status()
        tpj = tp.json()
        if not tpj.get("pass"):
            raise HTTPException(400, f"Trace PC failed: {tpj}")
        result["trace_pc"] = tpj
        cur.execute(
            "INSERT INTO mes_trace_history(serial, step, response_json, ts) VALUES(%s,%s,%s::jsonb,NOW())",
            (serial, "trace_pc", json.dumps(tpj))
        )

        # 3) Trace Interlock
        inter = requests.post(
            f"http://{get_cfg('TRACE_PROXY_HOST')}/interlock",
            params={"serial": serial, "serial_type": "band"}
        ); inter.raise_for_status()
        inj = inter.json()
        if not inj.get("pass"):
            raise HTTPException(400, f"Interlock failed: {inj}")
        result["interlock"] = inj
        cur.execute(
            "INSERT INTO mes_trace_history(serial, step, response_json, ts) VALUES(%s,%s,%s::jsonb,NOW())",
            (serial, "interlock", json.dumps(inj))
        )

        # 4) MES Upload
        payload = {
          "UniqueIds":     [serial],
          "MachineIds":    [get_cfg("MACHINE_ID")],
          "OperatorIds":   [_current_operator],
          "Tools":         [],
          "RawMaterials":  [],
          "DateTimeStamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
          "CbsStreamName": get_cfg("CBS_STREAM_NAME")
        }
        if req.result == "pass":
            payload.update({"IsPass": True, "IsFail": False, "IsScrap": False, "Defects": []})
        elif req.result == "fail":
            payload.update({
              "IsPass": False, "IsFail": True, "IsScrap": False,
              "FailureDefects": [{
                 "FailureReasonCodes": get_cfg("FAILURE_REASON_CODES").split(","),
                 "NcmReasonCodes":     get_cfg("NCM_REASON_CODES").split(",")
              }]
            })
        elif req.result == "scrap":
            payload.update({
              "IsPass": False, "IsFail": False, "IsScrap": True,
              "Defects": [{"DefectCode":"Trace Scrap","Quantity":1}]
            })
        else:
            raise HTTPException(400, "Invalid result type")

        mes = requests.post(get_cfg("MES_UPLOAD_URL"), json=payload)
        if mes.status_code != 200 or not mes.json().get("IsSuccessful"):
            raise HTTPException(400, f"MES upload failed: {mes.text}")
        result["mes_upload"] = mes.json()
        cur.execute(
            "INSERT INTO mes_trace_history(serial, step, response_json, ts) VALUES(%s,%s,%s::jsonb,NOW())",
            (serial, "mes_upload", json.dumps(result["mes_upload"]))
        )

        # 5) Trace Data Log
        td = requests.post(
            f"http://{get_cfg('TRACE_PROXY_HOST')}/v2/logs",
            json={"serials":{"part_id":serial}}
        )
        if td.status_code != 200:
            raise HTTPException(400, f"Trace log failed: {td.text}")
        result["trace_log"] = td.json()
        cur.execute(
            "INSERT INTO mes_trace_history(serial, step, response_json, ts) VALUES(%s,%s,%s::jsonb,NOW())",
            (serial, "trace_log", json.dumps(result["trace_log"]))
        )

        # Final audit
        cur.execute(
            "INSERT INTO scan_audit(serial, operator, result, ts) VALUES(%s,%s,%s,NOW())",
            (serial, _current_operator, req.result)
        )

        return result

    except HTTPException as he:
        cur.execute(
            "INSERT INTO error_logs(context,error_msg,details,ts) VALUES(%s,%s,%s::jsonb,NOW())",
            ("scan", str(he.detail), json.dumps({"serial":serial, "error":he.detail}))
        )
        raise

    except Exception as e:
        cur.execute(
            "INSERT INTO error_logs(context,error_msg,details,ts) VALUES(%s,%s,%s::jsonb,NOW())",
            ("scan", str(e), json.dumps({"serial": serial}))
        )
        raise HTTPException(500, "Internal server error")

# ─── Sensors Endpoints ─────────────────────────────────────────────────────
@app.get("/api/sensors/latest")
def sensors_latest(source: str):
    flux = f'''
      from(bucket:"{get_cfg("INFLUXDB_BUCKET")}")
        |> range(start:-1m)
        |> filter(fn:(r)=>r._measurement=="sensor_data" and r.source=="{source}")
        |> last()
    '''
    tables = query_api.query(flux)
    if not tables or not tables[0].records:
        raise HTTPException(404, "No data")
    rec = tables[0].records[-1]
    return {"time":rec.get_time().isoformat(), "value":rec.get_value(), "source":rec.values.get("source")}

@app.get("/api/sensors/history")
def sensors_history(source: str, hours: int = 1):
    flux = f'''
      from(bucket:"{get_cfg("INFLUXDB_BUCKET")}")
        |> range(start:-{hours}h)
        |> filter(fn:(r)=>r._measurement=="sensor_data" and r.source=="{source}")
        |> keep(columns:["_time","_value","source"])
    '''
    tables = query_api.query(flux)
    data = [
      {"time":rec.get_time().isoformat(), "value":rec.get_value(), "source":rec.values.get("source")}
      for table in tables for rec in table.records
    ]
    if not data:
        raise HTTPException(404, "No history")
    return data

# ─── OEE Endpoints ─────────────────────────────────────────────────────────
@app.get("/api/oee/current")
def oee_current():
    flux = f'''
      from(bucket:"{get_cfg("INFLUXDB_BUCKET")}")
        |> range(start:-5m)
        |> filter(fn:(r)=>r._measurement=="oee")
        |> last()
    '''
    tables = query_api.query(flux)
    if not tables or not tables[0].records:
        raise HTTPException(404, "No OEE data")
    rec = tables[0].records[-1]
    return {"time":rec.get_time().isoformat(),
            **{k:rec.values[k] for k in rec.values if not k.startswith("_")}}

@app.get("/api/oee/history")
def oee_history(hours: int = 1):
    flux = f'''
      from(bucket:"{get_cfg("INFLUXDB_BUCKET")}")
        |> range(start:-{hours}h)
        |> filter(fn:(r)=>r._measurement=="oee")
        |> pivot(rowKey:["_time"], columnKey:["_field"], valueColumn:"_value")
    '''
    tables = query_api.query(flux)
    data = []
    for table in tables:
        for rec in table.records:
            entry = {"time":rec.get_time().isoformat()}
            entry.update({fld:rec.values[fld] for fld in rec.values if fld not in ("_measurement",)})
            data.append(entry)
    if not data:
        raise HTTPException(404, "No OEE history")
    return data


# ─── WebSocket Setup ───────────────────────────────────────────────
KAFKA_BOOTSTRAP = get_cfg("KAFKA_BROKER", "kafka:9092")
WS_TOPICS = {
    "sensors": "raw_server_data",
    "triggers": "trigger_events",
    "commands": "machine_commands",
    "mes-logs": "mes_uploads",
    "trace-logs": "trace_logs",
    "scan-results": "scan_results",
    "station-status": "station_flags",
    "startup-status": "startup_status",
    "manual-status": "manual_status",
    "auto-status": "auto_status",
    "robo-status": "robo_status",
    "io-status": "io_status"
}

class ConnectionManager:
    def __init__(self):
        self.active = {k: set() for k in WS_TOPICS}

    async def connect(self, name, ws: WebSocket):
        await ws.accept()
        self.active[name].add(ws)

    def disconnect(self, name, ws: WebSocket):
        self.active[name].discard(ws)

    async def broadcast(self, topic, msg):
        living = set()
        for ws in self.active[topic]:
            try:
                await ws.send_text(msg)
                living.add(ws)
            except:
                pass
        self.active[topic] = living

mgr = ConnectionManager()

async def kafka_to_ws(name, topic):
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="latest",
        loop=asyncio.get_event_loop()
    )
    await consumer.start()
    try:
        async for record in consumer:
            msg = record.value.decode() if isinstance(record.value, bytes) else str(record.value)
            await mgr.broadcast(name, msg)
    finally:
        await consumer.stop()

@app.on_event("startup")
async def start_ws_consumers():
    for name, topic in WS_TOPICS.items():
        asyncio.create_task(kafka_to_ws(name, topic))

async def kafka_to_ws(name, topic):
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="latest",
        loop=asyncio.get_event_loop()
    )
    await consumer.start()
    try:
        async for msg in consumer:
            await mgr.broadcast(name, msg.value.decode())
    finally:
        await consumer.stop()

@app.websocket("/ws/{stream}")
async def ws_endpoint(stream: str, ws: WebSocket):
    if stream not in WS_TOPICS:
        await ws.close(code=1008)
        return
    await mgr.connect(stream, ws)
    try:
        while True:
            await ws.receive_text()  # Ignore input; server-push only
    except WebSocketDisconnect:
        mgr.disconnect(stream, ws)

