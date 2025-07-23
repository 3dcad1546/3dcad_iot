import os,json,time,threading,select,asyncio,requests,psycopg2,psycopg2.extensions,uuid,logging,traceback,random 
from logging.handlers import RotatingFileHandler
from psycopg2.extras import RealDictCursor
from psycopg2 import errors
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException, Depends, WebSocket, WebSocketDisconnect, Query, Header,Request
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from influxdb_client import InfluxDBClient, WriteOptions
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError, NoBrokersAvailable as AIOKafkaNoBrokersAvailable
from fastapi.middleware.cors import CORSMiddleware 
from pytz import timezone
from dateutil.parser import isoparse

# Ensure log directory exists
log_dir = "/services/dashboard_api/logs"
os.makedirs(log_dir, exist_ok=True)
# Global Kafka producer
kafka_producer: AIOKafkaProducer = None
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        # Console handler
        logging.StreamHandler(),
        # File handler with rotation
        RotatingFileHandler(
            os.path.join(log_dir, "dashboard_api.log"),
            maxBytes=10485760,  # 10MB
            backupCount=5
        )
    ]
)
logger = logging.getLogger("dashboard-api")


# ─── Configuration ──────────────────────────────────────────────
PLC_WRITE_COMMANDS_TOPIC   = os.getenv("PLC_WRITE_COMMANDS_TOPIC",   "plc_write_commands")
PLC_WRITE_RESPONSES_TOPIC  = os.getenv("PLC_WRITE_RESPONSES_TOPIC",  "plc_write_responses")
MACHINE_STATUS_TOPIC       = os.getenv("MACHINE_STATUS",             "machine_status")
STARTUP_STATUS             = os.getenv("STARTUP_STATUS",             "startup_status")
MANUAL_STATUS              = os.getenv("MANUAL_STATUS",              "manual_status")
AUTO_STATUS                = os.getenv("AUTO_STATUS",                "auto_status")
ROBO_STATUS                = os.getenv("ROBO_STATUS",                "robo_status")
IO_STATUS                  = os.getenv("IO_STATUS",                  "io_status")
ALARM_STATUS               = os.getenv("ALARM_STATUS",               "alarm_status")
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
        logging.error("PostgreSQL not ready, retrying in 5s…")
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

# WebSocket Topics (remove machine-status from here since it's handled separately)
WS_TOPICS = {
    "machine-status": MACHINE_STATUS_TOPIC,
    "startup-status": STARTUP_STATUS,
    "manual-status":  MANUAL_STATUS,
    "auto-status":    AUTO_STATUS,
    "robo-status":    ROBO_STATUS,
    "io-status":      IO_STATUS,
    "alarm-status":   ALARM_STATUS,
    "oee-status":     OEE_STATUS,
    "plc-write-responses": PLC_WRITE_RESPONSES_TOPIC,
    "plc-write": PLC_WRITE_COMMANDS_TOPIC, 
    "analytics": "other_streams_if_any"
}

# WebSocket Connection Manager
class ConnectionManager:
    def __init__(self):
        self.active: Dict[str, set[WebSocket]] = {s:set() for s in WS_TOPICS}
        self.pending: Dict[str, WebSocket]   = {}

    async def connect(self, stream: str, websocket: WebSocket):
        await websocket.accept()
        if stream not in self.active:
            self.active[stream] = set()
        self.active[stream].add(websocket)
        logger.info(f"WebSocket connected to stream: {stream}")

    def disconnect(self, stream: str, websocket: WebSocket):
        if stream in self.active:
            self.active[stream].discard(websocket)
            logger.info(f"WebSocket disconnected from stream: {stream}")

    async def broadcast(self, stream: str, message: str):
        if stream in self.active:
            dead_connections = []
            for connection in self.active[stream]:
                try:
                    await connection.send_text(message)
                except Exception as e:
                    logger.debug(f"Failed to send to WebSocket in {stream}: {e}")
                    dead_connections.append(connection)
            
            # Clean up dead connections
            for connection in dead_connections:
                self.active[stream].discard(connection)

mgr = ConnectionManager()

# ─── variant_master  ───────────────────────────────────
cur.execute("""
CREATE TABLE IF NOT EXISTS variant_master (
  id   SERIAL      PRIMARY KEY,
  name TEXT UNIQUE NOT NULL
);
""")

cur.execute("""
ALTER TABLE cycle_master
  ADD COLUMN IF NOT EXISTS variant TEXT NULL;
""")

cur.execute("""
CREATE INDEX IF NOT EXISTS idx_cycle_master_variant
  ON cycle_master(variant);
""")



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
    variant: str
    barcode: str
    start_ts: datetime
    end_ts: Optional[datetime]
    events: List[CycleEvent]
    analytics: Optional[List[Dict[str, Any]]] = None  

class Variant(BaseModel):
    id:   int
    name: str

class VariantCreate(BaseModel):
    name: str = Field(..., description="Name of the variant")

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
        conn.commit()

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
        conn.commit()

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
        conn.commit()

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
        conn.commit()

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
        conn.commit()

        # Final audit
        cur.execute(
            "INSERT INTO scan_audit(serial,operator,result,ts) VALUES(%s,%s,%s,NOW())",
            (serial,user,req.result)
        )
        conn.commit()
        return result

    except HTTPException as he:
        cur.execute(
            "INSERT INTO error_logs(context,error_msg,details,ts) VALUES(%s,%s,%s::jsonb,NOW())",
            ("scan",str(he.detail),json.dumps({"serial":serial,"error":he.detail}))
        )
        conn.commit()
        raise
    except Exception as e:
        cur.execute(
            "INSERT INTO error_logs(context,error_msg,details,ts) VALUES(%s,%s,%s::jsonb,NOW())",
            ("scan",str(e),json.dumps({"serial":serial}))
        )
        conn.commit()
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

# ─── Variant CRUD ───────────────────────────────────────────────────

@app.post("/api/variants", response_model=Variant)
def create_variant(item: VariantCreate, user: str = Depends(require_login)):
    try:
        cur.execute(
            "INSERT INTO variant_master(name) VALUES (%s) RETURNING id,name",
            (item.name,)
        )
    except errors.UniqueViolation:
        raise HTTPException(409, "Variant already exists")
    row = cur.fetchone()
    return Variant(**row)

@app.get("/api/variants", response_model=List[Variant])
def list_variants(user: str = Depends(require_login)):
    cur.execute("SELECT id,name FROM variant_master ORDER BY name")
    rows = cur.fetchall()
    return [Variant(**r) for r in rows]

@app.put("/api/variants/{variant_id}", response_model=Variant)
def update_variant(variant_id: int, item: VariantCreate, user: str = Depends(require_login)):
    cur.execute(
        "UPDATE variant_master SET name=%s WHERE id=%s RETURNING id,name",
        (item.name, variant_id)
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(404, "Variant not found")
    return Variant(**row)

@app.delete("/api/variants/{variant_id}", status_code=204)
def delete_variant(variant_id: int, user: str = Depends(require_login)):
    cur.execute("DELETE FROM variant_master WHERE id=%s", (variant_id,))
    if cur.rowcount == 0:
        raise HTTPException(404, "Variant not found")
    return


# alarm api

@app.get("/api/alarms", response_model=List[dict])
def get_alarms(
    status: Optional[str] = Query(None, description="Filter by status: active, acknowledged, resolved"),
    limit: int = Query(100, ge=1, le=500, description="Maximum number of alarms to return"),
    user: str = Depends(require_login)
):
    """
    Returns alarms from the alarm_master table with optional filtering
    """
    try:
        where_clause = ""
        params = []
        
        if status:
            where_clause = "WHERE status = %s"
            params.append(status)
        
        query = f"""
            SELECT 
                id, alarm_date, alarm_time, alarm_code, message, status,
                acknowledged, acknowledged_by, acknowledged_at, created_at, resolved_at,
                EXTRACT(EPOCH FROM (acknowledged_at - created_at)) as ack_duration_seconds,
                EXTRACT(EPOCH FROM (COALESCE(resolved_at, NOW()) - created_at)) as total_duration_seconds
            FROM alarm_master 
            {where_clause}
            ORDER BY alarm_date DESC, alarm_time DESC 
            LIMIT %s
        """
        params.append(limit)
        
        cur.execute(query, params)
        alarms = cur.fetchall()
        
        # Convert to list of dicts with proper datetime formatting
        result = []
        for alarm in alarms:
            alarm_dict = dict(alarm)
            # Format timestamps to ISO format if they exist
            for ts_field in ['acknowledged_at', 'created_at', 'resolved_at']:
                if alarm_dict.get(ts_field):
                    alarm_dict[ts_field] = alarm_dict[ts_field].isoformat()
            result.append(alarm_dict)
        
        return result
        
    except Exception as e:
        logger.error(f"Error fetching alarms: {e}")
        raise HTTPException(500, f"Failed to fetch alarms: {str(e)}")


#receive webhook data
@app.post("/edge/api/v1/analytics")
async def receive_analytics(data: Dict):
    logger.info(f"Received analytics webhook with keys: {list(data.keys())}")
    
    # Try multiple possible field names for barcode
    barcode = None
    for field in ["barcode", "part_id", "metadata", "serial"]:
        if field in data and data[field]:
            barcode = data[field]
            logger.info(f"Found barcode identifier in field '{field}': {barcode}")
            break
    
    if not barcode:
        logger.warning(f"Analytics data received without any identifiable barcode fields: {data.keys()}")
        return {"status": "warning", "message": "No barcode found in data"}
    
    # Find the corresponding cycle_id for this barcode
    try:
        cur.execute("SELECT cycle_id FROM cycle_master WHERE barcode = %s ORDER BY start_ts DESC LIMIT 1", 
                   (barcode,))
        row = cur.fetchone()
        
        if not row:
            logger.warning(f"No cycle found for barcode {barcode} in analytics data")
            return {"status": "warning", "message": f"No cycle found for barcode {barcode}"}
        
        cycle_id = row["cycle_id"]
        
        logger.info(f"Found cycle_id {cycle_id} for barcode {barcode}")

        
        
        # Store the analytics data
        cur.execute(
            "INSERT INTO cycle_analytics(cycle_id, json_data) VALUES(%s, %s) RETURNING id",
            (cycle_id, json.dumps(data))
        )
        analytics_id = cur.fetchone()["id"]
        conn.commit()

         # After successful storage, broadcast to WebSocket
        await mgr.broadcast("analytics", json.dumps({
            "cycle_id": cycle_id,
            "barcode": barcode,
            "analytics_id": analytics_id,
            "analytics": data
        }))
        
       
        
        logger.info(f"Successfully stored analytics data with ID {analytics_id} for cycle {cycle_id}")
        return {"status": "success", "cycle_id": cycle_id, "analytics_id": analytics_id}
        
    except Exception as e:
        logger.error(f"Error storing analytics data: {e}")
        import traceback
        logger.error(traceback.format_exc())
        conn.rollback()
        return {"status": "error", "message": str(e)}

# ─── Cycle Reporting Endpoint ──────────────────────────────────────
class CycleVariantUpdate(BaseModel):
    variant_id: int

@app.put("/api/cycles/{cycle_id}/variant", response_model=Dict[str,str])
def assign_cycle_variant(
    cycle_id: str,
    upd: CycleVariantUpdate,
    user: str = Depends(require_login)
):
    # 1) look up the variant name
    cur.execute("SELECT name FROM variant_master WHERE id=%s", (upd.variant_id,))
    vr = cur.fetchone()
    if not vr:
        raise HTTPException(404, "Variant not found")
    name = vr["name"]

    # 2) update the cycle
    cur.execute(
        "UPDATE cycle_master SET variant=%s WHERE cycle_id=%s",
        (name, cycle_id)
    )
    if cur.rowcount == 0:
        raise HTTPException(404, "Cycle not found")

    return {"cycle_id": cycle_id, "variant": name}

# og report changes working condition with analytics
# @app.get("/api/cycles", response_model=List[CycleReportItem])
# def get_cycles(
#     operator: Optional[str] = None,
#     shift_id: Optional[int] = None,
#     barcode: Optional[str] = None,
#     variant: Optional[str] = None,
#     from_ts: Optional[datetime] = Query(None, alias="from"),
#     to_ts: Optional[datetime] = Query(None, alias="to"),
#     include_analytics: bool = Query(False),
#     limit: int = Query(60, ge=1, le=100),
#     user: str = Depends(require_login)
# ):
#     IST = timezone("Asia/Kolkata")
    
#     # Build WHERE clauses
#     # Build WHERE clauses
#     clauses, params = [], []
#     if operator:
#         clauses.append("cm.operator=%s"); params.append(operator)
#     if shift_id is not None:
#         clauses.append("cm.shift_id=%s"); params.append(shift_id)
#     if barcode:
#         clauses.append("cm.barcode=%s"); params.append(barcode)
#     if variant:
#         clauses.append("cm.variant=%s"); params.append(variant)
#     if from_ts:
#         from_ts_ist = from_ts.astimezone(IST)
#         clauses.append("cm.start_ts >= %s"); params.append(from_ts_ist)
#     if to_ts:
#         to_ts_ist = to_ts.astimezone(IST)
#         clauses.append("cm.start_ts <= %s"); params.append(to_ts_ist)
    
#     # Add explicit type casting for cycle_id comparison
#     if operator:
#         clauses.append("cm.operator=%s"); params.append(operator)
#     if shift_id is not None:
#         clauses.append("cm.shift_id=%s"); params.append(shift_id)
#     if barcode:
#         clauses.append("cm.barcode=%s"); params.append(barcode)
#     if variant:
#         clauses.append("cm.variant=%s"); params.append(variant)
#     if from_ts:
#         from_ts_ist = from_ts.astimezone(IST)
#         clauses.append("cm.start_ts >= %s"); params.append(from_ts_ist)
#     if to_ts:
#         to_ts_ist = to_ts.astimezone(IST)
#         clauses.append("cm.start_ts <= %s"); params.append(to_ts_ist)
    
#     # Add explicit type casting for cycle_id comparison
#     analytics_select = ""
#     analytics_join = ""
    
#     if include_analytics:
#         analytics_select = ", COALESCE(jsonb_agg(ca.json_data) FILTER (WHERE ca.id IS NOT NULL), '[]'::jsonb) as analytics"
#         analytics_join = "LEFT JOIN cycle_analytics ca ON ca.cycle_id = CAST(cm.cycle_id AS TEXT)"
        
    
#     # Your SQL query with additions for analytics
#     sql = f"""
#     SELECT 
#       cm.cycle_id, cm.operator, cm.shift_id, cm.variant, cm.barcode,
#       cm.start_ts, cm.end_ts,
#       jsonb_agg(jsonb_build_object('stage', ce.stage, 'ts', ce.ts)
#                 ORDER BY ce.id) AS events
#       {analytics_select}
#     FROM cycle_master cm
#     LEFT JOIN cycle_event ce ON ce.cycle_id = cm.cycle_id
#     {analytics_join}
#     WHERE {' AND '.join(clauses) if clauses else 'TRUE'}
#     GROUP BY cm.cycle_id, cm.operator, cm.shift_id, cm.variant, cm.barcode, cm.start_ts, cm.end_ts
#     ORDER BY cm.start_ts DESC
#     LIMIT %s
#     """
#     params.append(limit)
    
#     cur.execute(sql, params)
#     rows = cur.fetchall()
    
#     # Update your result processing to include analytics
#     result = []
#     for r in rows:
#         item = {
#             "cycle_id": r["cycle_id"],
#             "operator": r["operator"],
#             "shift_id": r["shift_id"],
#             "variant": r["variant"] or "",
#             "barcode": r["barcode"],
#             "start_ts": r["start_ts"].astimezone(IST),
#             "end_ts": r["end_ts"].astimezone(IST) if r["end_ts"] else None,
#             "events": [{"stage": e["stage"], "ts": e["ts"]} for e in (r["events"] or [])]

#         }
        
#         # Add analytics if included
#         if include_analytics and "analytics" in r and r["analytics"]:
#             item["analytics"] = r["analytics"]
        
#         result.append(CycleReportItem(**item))
    
#     return result


# this function added to include cycle_id in vision json_data also
@app.get("/api/cycles", response_model=List[CycleReportItem])
def get_cycles(
    operator: Optional[str] = None,
    shift_id: Optional[int] = None,
    barcode: Optional[str] = None,
    variant: Optional[str] = None,
    from_ts: Optional[datetime] = Query(None, alias="from"),
    to_ts: Optional[datetime] = Query(None, alias="to"),
    include_analytics: bool = Query(False),
    limit: int = Query(60, ge=1, le=100),
    user: str = Depends(require_login)
):
    IST = timezone("Asia/Kolkata")

    clauses, params = [], []
    if operator:
        clauses.append("cm.operator=%s"); params.append(operator)
    if shift_id is not None:
        clauses.append("cm.shift_id=%s"); params.append(shift_id)
    if barcode:
        clauses.append("cm.barcode=%s"); params.append(barcode)
    if variant:
        clauses.append("cm.variant=%s"); params.append(variant)
    if from_ts:
        from_ts_ist = from_ts.astimezone(IST)
        clauses.append("cm.start_ts >= %s"); params.append(from_ts_ist)
    if to_ts:
        to_ts_ist = to_ts.astimezone(IST)
        clauses.append("cm.start_ts <= %s"); params.append(to_ts_ist)

    # SQL parts
    analytics_cte = ""
    analytics_join = ""
    analytics_select = ""

    if include_analytics:
        analytics_cte = """
        WITH analytics_agg AS (
            SELECT
                ca.cycle_id,
                jsonb_agg(ca.json_data) AS data
            FROM cycle_analytics ca
            GROUP BY ca.cycle_id
        )
        """
        analytics_join = "LEFT JOIN analytics_agg aa ON aa.cycle_id = cm.cycle_id"
        analytics_select = """,
        jsonb_build_object(
            'cycle_id', cm.cycle_id,
            'data', COALESCE(aa.data, '[]'::jsonb)
        ) AS analytics
        """

    # Final SQL query
    sql = f"""
    {analytics_cte}
    SELECT 
      cm.cycle_id, cm.operator, cm.shift_id, cm.variant, cm.barcode,
      cm.start_ts, cm.end_ts,
      jsonb_agg(jsonb_build_object('stage', ce.stage, 'ts', ce.ts)
                ORDER BY ce.id) AS events
      {analytics_select}
    FROM cycle_master cm
    LEFT JOIN cycle_event ce ON ce.cycle_id = cm.cycle_id
    {analytics_join}
    WHERE {' AND '.join(clauses) if clauses else 'TRUE'}
    GROUP BY cm.cycle_id, cm.operator, cm.shift_id, cm.variant, cm.barcode, cm.start_ts, cm.end_ts
    {', aa.data' if include_analytics else ''}
    ORDER BY cm.start_ts DESC
    LIMIT %s
    """
    params.append(limit)

    cur.execute(sql, params)
    rows = cur.fetchall()

    # Build result
    result = []
    for r in rows:
        item = {
            "cycle_id": r["cycle_id"],
            "operator": r["operator"],
            "shift_id": r["shift_id"],
            "variant": r["variant"] or "",
            "barcode": r["barcode"],
            "start_ts": r["start_ts"].astimezone(IST),
            "end_ts": r["end_ts"].astimezone(IST) if r["end_ts"] else None,
            "events": [{"stage": e["stage"], "ts": e["ts"]} for e in (r["events"] or [])]
        }

        if include_analytics and r.get("analytics"):
            # Wrap in array to match expected List[Dict] structure
            item["analytics"] = [r["analytics"]]

        result.append(CycleReportItem(**item))

    return result


# Add alarm acknowledgment endpoint
@app.post("/api/alarms/{alarm_id}/acknowledge")
def acknowledge_alarm(alarm_id: int, user: str = Depends(require_login)):
    """
    Acknowledge an alarm
    """
    try:
        cur.execute("""
            UPDATE alarm_master 
            SET acknowledged = TRUE, 
                acknowledged_by = %s, 
                acknowledged_at = NOW(),
                status = 'acknowledged'
            WHERE id = %s AND acknowledged = FALSE
            RETURNING id, alarm_code, status
        """, (user, alarm_id))
        
        result = cur.fetchone()
        if not result:
            raise HTTPException(404, "Alarm not found or already acknowledged")
        
        conn.commit()
        logger.info(f"Alarm {alarm_id} acknowledged by {user}")
        
        return {
            "message": f"Alarm {result['alarm_code']} acknowledged successfully",
            "alarm_id": result["id"],
            "status": result["status"]
        }
        
    except Exception as e:
        logger.error(f"Error acknowledging alarm {alarm_id}: {e}")
        conn.rollback()
        raise HTTPException(500, f"Failed to acknowledge alarm: {str(e)}")

# Add alarm resolution endpoint
@app.post("/api/alarms/{alarm_id}/resolve")
def resolve_alarm(alarm_id: int, user: str = Depends(require_login)):
    """
    Mark an alarm as resolved
    """
    try:
        cur.execute("""
            UPDATE alarm_master 
            SET status = 'resolved',
                resolved_at = NOW()
            WHERE id = %s AND status != 'resolved'
            RETURNING id, alarm_code, status
        """, (alarm_id,))
        
        result = cur.fetchone()
        if not result:
            raise HTTPException(404, "Alarm not found or already resolved")
        
        conn.commit()
        logger.info(f"Alarm {alarm_id} resolved by {user}")
        
        return {
            "message": f"Alarm {result['alarm_code']} resolved successfully",
            "alarm_id": result["id"],
            "status": result["status"]
        }
        
    except Exception as e:
        logger.error(f"Error resolving alarm {alarm_id}: {e}")
        conn.rollback()
        raise HTTPException(500, f"Failed to resolve alarm: {str(e)}")

# Add this function to consume and process alarm data from Kafka
async def consume_alarm_status_and_populate_db():
    """
    Consume alarm status from Kafka and insert active alarms into database
    """
    for i in range(10):
        try:
            consumer = AIOKafkaConsumer(
                ALARM_STATUS,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                auto_offset_reset="earliest",
                value_deserializer=lambda b: json.loads(b.decode()),
                group_id="alarm_db_processor"
            )
            await consumer.start()
            logger.info("Started alarm status consumer for database processing")
            break
        except AIOKafkaNoBrokersAvailable:
            await asyncio.sleep(5)
    else:
        raise RuntimeError("Cannot connect alarm status consumer")

    try:
        async for msg in consumer:
            payload = msg.value
            alarm_data = payload
            
            if not alarm_data or not isinstance(alarm_data, dict):
                continue
                
            # Extract timestamp
            timestamp_str = alarm_data.get("ts", "")
            if timestamp_str:
                try:
                    # Parse timestamp to get date and time
                    dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                    alarm_date = dt.date()
                    alarm_time = dt.time()
                except:
                    # Fallback to current time
                    now = datetime.now()
                    alarm_date = now.date()
                    alarm_time = now.time()
            else:
                now = datetime.now()
                alarm_date = now.date()
                alarm_time = now.time()
            
            # Process each alarm in the payload
            for alarm_code, is_active in alarm_data.items():
                if alarm_code == "ts":  # Skip timestamp field
                    continue
                    
                try:
                    if is_active and is_active != 0:  # Alarm is active
                        # Check if this alarm is already active today
                        cur.execute("""
                            SELECT id FROM alarm_master 
                            WHERE alarm_code = %s 
                            AND alarm_date = %s 
                            AND status = 'active'
                        """, (alarm_code, alarm_date))
                        
                        existing_alarm = cur.fetchone()
                        
                        if not existing_alarm:
                            # Insert new active alarm
                            cur.execute("""
                                INSERT INTO alarm_master (
                                    alarm_date, alarm_time, alarm_code, 
                                    message, status, created_at
                                )
                                VALUES (%s, %s, %s, %s, 'active', NOW())
                            """, (
                                alarm_date, 
                                alarm_time, 
                                alarm_code,
                                f"Alarm triggered: {alarm_code}"
                            ))
                            
                            logger.info(f"New alarm inserted: {alarm_code} at {alarm_date} {alarm_time}")
                            
                    else:  # Alarm is not active - auto-resolve if it was active
                        cur.execute("""
                            UPDATE alarm_master 
                            SET status = 'resolved', resolved_at = NOW()
                            WHERE alarm_code = %s 
                            AND alarm_date = %s 
                            AND status = 'active'
                        """, (alarm_code, alarm_date))
                        
                        if cur.rowcount > 0:
                            logger.info(f"Auto-resolved alarm: {alarm_code}")
                            
                except Exception as e:
                    logger.error(f"Error processing alarm {alarm_code}: {e}")
                    continue
                    
            conn.commit()
            
    except Exception as e:
        logger.error(f"Error in alarm status consumer: {e}")
    finally:
        await consumer.stop()
        logger.info("Stopped alarm status consumer")

async def kafka_to_ws(stream: str, topic: str):
    """
    Generic Kafka to WebSocket streaming function
    """
    for i in range(10):
        try:
            consumer = AIOKafkaConsumer(
                topic,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                auto_offset_reset="latest",
                value_deserializer=lambda b: b.decode(),
                group_id=f"{stream}_websocket_group"
            )
            await consumer.start()
            logger.info(f"Started {stream} WebSocket streaming consumer")
            break
        except AIOKafkaNoBrokersAvailable:
            await asyncio.sleep(5)
    else:
        raise RuntimeError(f"Cannot connect {stream} WebSocket consumer")

    try:
        async for msg in consumer:
            await mgr.broadcast(stream, msg.value)
    except Exception as e:
        logger.error(f"Error in {stream} WebSocket consumer: {e}")
    finally:
        await consumer.stop()
        logger.info(f"Stopped {stream} WebSocket consumer")

async def init_kafka_producer():
    """
    Initialize the Kafka producer
    """
    global kafka_producer
    for i in range(10):
        try:
            kafka_producer = AIOKafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode()
            )
            await kafka_producer.start()
            logger.info("Kafka producer initialized")
            break
        except AIOKafkaNoBrokersAvailable:
            await asyncio.sleep(5)
    else:
        raise RuntimeError("Cannot connect Kafka producer")

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

async def consume_set_updates():
    """
    Consume individual set updates from set-specific topics 
    and broadcast them to machine-status WebSocket clients
    """
    for i in range(10):
        try:
            consumer = AIOKafkaConsumer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                auto_offset_reset="latest",
                value_deserializer=lambda b: json.loads(b.decode()),
                group_id="machine_status_set_updates"
            )
            # Use pattern subscription to catch all sanitized topic names
            await consumer.start()
            await consumer.subscribe(pattern=f"{MACHINE_STATUS_TOPIC}.set.*")
            logger.info(f"Started consumer for individual set updates on pattern {MACHINE_STATUS_TOPIC}.set.*")
            break
        except AIOKafkaNoBrokersAvailable:
            await asyncio.sleep(5)
    else:
        raise RuntimeError("Cannot connect to Kafka for set updates")

    try:
        async for msg in consumer:
            try:
                set_data = msg.value
                if not set_data:
                    continue
                    
                # Forward to all connected machine-status clients
                await mgr.broadcast("machine-status", json.dumps({
                    "type": "set_update",
                    "data": set_data
                }))
                
                if random.randint(1, 10) == 1:  # Log only 10% of messages to reduce noise
                    set_id = set_data.get("set", {}).get("set_id", "unknown")
                    logger.debug(f"Forwarded set update for {set_id} to WebSocket clients")
                    
            except Exception as e:
                logger.error(f"Error processing set update: {e}")
    finally:
        await consumer.stop()
        logger.info("Stopped set updates consumer")

# Also add the missing consume_machine_status_and_populate_db function
async def consume_machine_status_and_populate_db():
    """
    Consume machine status from Kafka and process cycle data for database
    """
    for i in range(10):
        try:
            consumer = AIOKafkaConsumer(
                MACHINE_STATUS_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                auto_offset_reset="earliest",
                value_deserializer=lambda b: json.loads(b.decode()),
                group_id="machine_status_db_processor"
            )
            await consumer.start()
            logger.info("Started machine status consumer for database processing")
            break
        except AIOKafkaNoBrokersAvailable:
            await asyncio.sleep(5)
    else:
        raise RuntimeError("Cannot connect machine status consumer")

    try:
        async for msg in consumer:
            payload = msg.value
            
            # Process machine status for cycle management
            if "sets" in payload:
                for set_data in payload["sets"]:
                    # Extract and clean the set_id and barcodes
                    original_set_id = set_data.get("set_id", "")
                    if original_set_id:
                        # Remove null bytes and + with any digits after it
                        original_set_id = original_set_id.replace("\x00", "")
                        if "+" in original_set_id:
                            original_set_id = original_set_id.split("+")[0]
                        
                        # Generate deterministic UUID for cycle_id
                        cycle_id = str(uuid.uuid5(uuid.NAMESPACE_OID, original_set_id))
                        
                        # Process barcodes
                        barcodes = set_data.get("barcodes", [])
                        primary_barcode = ""
                        if barcodes:
                            primary_barcode = str(barcodes[0]) if barcodes[0] else ""
                            # Clean barcode
                            if primary_barcode:
                                primary_barcode = primary_barcode.replace("\x00", "")
                                if "+" in primary_barcode:
                                    primary_barcode = primary_barcode.split("+")[0]
                        
                        # Insert cycle if not exists
                        try:
                            cur.execute("""
                                INSERT INTO cycle_master(cycle_id, barcode, operator, shift_id, variant, start_ts)
                                VALUES (%s, %s, %s, %s, %s, %s)
                                ON CONFLICT (cycle_id) DO NOTHING
                            """, (cycle_id, primary_barcode, "system", 1, "default", set_data.get("created_ts")))
                            
                            if cur.rowcount > 0:
                                logger.info(f"New cycle created: {cycle_id} with barcode: {primary_barcode}")
                                
                        except Exception as e:
                            logger.error(f"Error inserting cycle {cycle_id}: {e}")
                            
            conn.commit()
            
    except Exception as e:
        logger.error(f"Error in machine status consumer: {e}")
    finally:
        await consumer.stop()
        logger.info("Stopped machine status consumer")

@app.websocket("/ws/{stream}")
async def websocket_endpoint(websocket: WebSocket, stream: str):
    """
    Generic WebSocket endpoint for all streams except those with dedicated handlers
    """
    
    if stream not in WS_TOPICS:
        await websocket.close(code=4004, reason=f"Stream '{stream}' not found")
        return
    
    await mgr.connect(stream, websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        mgr.disconnect(stream, websocket)

@app.websocket("/ws/machine-status")
async def websocket_machine_status(websocket: WebSocket):
    """
    Dedicated WebSocket endpoint for machine status with support for:
    1. Real-time set updates
    2. Handling both individual and batch updates
    3. Initial state delivery
    """
    logger.info("WebSocket connection attempt to /ws/machine-status")
    await mgr.connect("machine-status", websocket)
    
    logger.info("WebSocket connected to machine-status")

    try:
        # Send initial full state on connection
        consumer = AIOKafkaConsumer(
            MACHINE_STATUS_TOPIC,  # Listen to the main topic, not .full suffix
            bootstrap_servers=KAFKA_BOOTSTRAP,
            auto_offset_reset="latest",
            group_id=f"ws-init-{str(uuid.uuid4())}",
            consumer_timeout_ms=5000,
            value_deserializer=lambda b: json.loads(b.decode())
        )
        await consumer.start()
        
        # Try to get the latest full update
        try:
            start_time = time.time()
            async for msg in consumer:
                # Forward any message with "sets" field as initial state
                if msg.value and "sets" in msg.value:
                    await websocket.send_json(msg.value)
                    logger.info("Sent initial machine status to new client")
                    break
                
                # Timeout after 2 seconds
                if time.time() - start_time > 2:
                    logger.warning("Timeout waiting for initial machine status")
                    break
        except Exception as e:
            logger.error(f"Error fetching initial machine status: {e}")
        
        await consumer.stop()
        
        # Stay connected for ongoing messages
        while True:
            data = await websocket.receive_text()
            await websocket.send_json({"type": "ack", "message": "received"})
            
    except WebSocketDisconnect:
        logger.info("WebSocket disconnected from machine-status")
        mgr.disconnect("machine-status", websocket)
    except Exception as e:
        logger.error(f"Error in machine-status WebSocket: {e}")
        if hasattr(websocket, 'client_state') and websocket.client_state.state != 4:
            await websocket.close(code=1011, reason=f"Error: {str(e)}")

@app.websocket("/ws/plc-write")
async def ws_plc_write(ws: WebSocket):
    logger.info("WebSocket connection attempt to /ws/plc-write")
    await mgr.connect("plc-write-responses", ws)
    logger.info("WebSocket connected to plc-write-responses")
    try:
        while True:
            data_raw = await ws.receive_text()  # Get as text first
            data = json.loads(data_raw)  # Parse JSON
            logger.info(f"Received WebSocket data: {data_raw[:100]}...")
            
            try:
                cmd = PlcWriteCommand(**data)
                rid = cmd.request_id or str(uuid.uuid4())
                cmd.request_id = rid
                
                if not hasattr(mgr, 'pending'):
                    mgr.pending = {}
                    
                mgr.pending[rid] = ws
                
                logger.info(f"Sending command to Kafka: {cmd.dict()}")
                if not kafka_producer:
                    logger.error("Kafka producer not initialized")
                    raise RuntimeError("Kafka producer not initialized")
                    
                await kafka_producer.send_and_wait(PLC_WRITE_COMMANDS_TOPIC, cmd.dict())
                logger.info(f"Command sent to Kafka, sending ACK for request_id: {rid}")
                
                await ws.send_json({"type":"ack","status":"pending","request_id":rid})
            except Exception as e:
                logger.error(f"Error processing PLC write command: {str(e)}")
                await ws.send_json({"type":"error","message":str(e)})
                
    except WebSocketDisconnect:
        logger.info("WebSocket disconnected from plc-write-responses")
        mgr.disconnect("plc-write-responses", ws)
    except Exception as e:
        logger.error(f"Error in plc-write WebSocket: {e}")
        if not ws.client_state.state == 4:  # If not already closed
            await ws.close(code=1011, reason=f"Error: {str(e)}")


@app.websocket("/ws/analytics")
async def websocket_analytics(ws: WebSocket):
    # Define the stream name
    stream = "analytics"  # Add this line to define the stream variable
    
    await mgr.connect(stream, ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        mgr.disconnect(stream, ws)

# Update the startup function to include alarm processing
@app.on_event("startup")
async def on_startup():
    await init_kafka_producer()
    
    # Start WebSocket streaming for all topics EXCEPT machine-status
    for name, topic in WS_TOPICS.items():
        asyncio.create_task(kafka_to_ws(name, topic))
    
    # Handle machine-status separately for database processing only
    asyncio.create_task(consume_machine_status_and_populate_db())
    
    #Add consumer for individual set updates
    asyncio.create_task(consume_set_updates())

    # Add alarm processing
    asyncio.create_task(consume_alarm_status_and_populate_db())
    
    # Start PLC write responses
    asyncio.create_task(listen_for_plc_write_responses())

@app.on_event("shutdown")
async def on_shutdown():
    if kafka_producer:
        await kafka_producer.stop()
    if conn:
        conn.close()
    if influx_client:
        influx_client.close()
