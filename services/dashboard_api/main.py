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
from zoneinfo import ZoneInfo

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
    async def send_write_response(self, request_id: str, payload: dict):
        ws = self.pending.pop(request_id, None)
        if ws:
            try:
                await ws.send_json({"type":"plc_write_response","data":payload})
            except:
                pass
    
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

class CycleReportItemAnalytics(BaseModel):
    operator: str
    shift_id: int
    variant: str
    barcode: str
    received_ts: datetime
    analytics: dict  # json_data stored here

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
async def receive_analytics(data: Dict, x_auth_token: str = Header(default=None, alias="X-Auth-Token")):
    logger.info(f"Received analytics webhook with keys: {list(data.keys())}")


    # Try to extract username from token if present
    username = "system"
    if x_auth_token:
        try:
            username = require_login(x_auth_token)
            logger.info(f"Current User '{field}': {username}")
        except Exception as e:
            logger.warning(f"Auth token provided but invalid: {e}. Proceeding as 'system'.")
    
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
        # cur.execute("SELECT cycle_id FROM cycle_master WHERE barcode = %s ORDER BY start_ts DESC LIMIT 1", 
        #            (barcode,))
        # row = cur.fetchone()
        
        # if not row:
        #     logger.warning(f"No cycle found for barcode {barcode} in analytics data")
        #     return {"status": "warning", "message": f"No cycle found for barcode {barcode}"}
        
        # cycle_id = row["cycle_id"]
        
        # logger.info(f"Found cycle_id {cycle_id} for barcode {barcode}")

        
        
        # Store the analytics data
        cur.execute(
            "INSERT INTO cycle_analytics(json_data, operator, shift_id, variant, barcode) VALUES(%s, %s, %s, %s, %s) RETURNING id",
            (json.dumps(data), username, 1, "default", barcode)
        )
       
        analytics_id = cur.fetchone()["id"]
        conn.commit()

         # After successful storage, broadcast to WebSocket
        await mgr.broadcast("analytics", json.dumps({
            # "cycle_id": cycle_id,
            # "barcode": barcode,
            "analytics_id": analytics_id,
            "analytics": data
        }))
        
       
        
        logger.info(f"Successfully stored analytics data with ID {analytics_id} ")
        return {"status": "success",  "analytics_id": analytics_id}
        
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




# report api added to fetch data only from cycle analytics table
@app.get("/api/cycles", response_model=List[CycleReportItemAnalytics])
def get_cycles(
    operator: Optional[str] = None,
    shift_id: Optional[int] = None,
    variant: Optional[str] = None,
    barcode: Optional[str] = None,
    from_ts: Optional[datetime] = Query(None, alias="from"),
    to_ts: Optional[datetime] = Query(None, alias="to"),
    limit: int = Query(1200, ge=1, le=1300),
    user: str = Depends(require_login)
):
    IST = timezone("Asia/Kolkata")

    clauses, params = [], []
    if operator:
        clauses.append("ca.operator = %s")
        params.append(operator)
    if shift_id is not None:
        clauses.append("ca.shift_id = %s")
        params.append(shift_id)
    if variant:
        clauses.append("ca.variant = %s")
        params.append(variant)
    if barcode:
        clauses.append("ca.barcode = %s")
        params.append(barcode)
    if from_ts:
        from_ts_ist = from_ts.astimezone(IST)
        clauses.append("ca.received_ts >= %s")
        params.append(from_ts_ist)
    if to_ts:
        to_ts_ist = to_ts.astimezone(IST)
        clauses.append("ca.received_ts <= %s")
        params.append(to_ts_ist)

    sql = f"""
        SELECT
            ca.operator,
            ca.shift_id,
            ca.variant,
            ca.barcode,
            ca.received_ts,
            ca.json_data AS analytics
        FROM cycle_analytics ca
        WHERE {' AND '.join(clauses) if clauses else 'TRUE'}
        ORDER BY ca.received_ts DESC
        LIMIT %s
    """
    params.append(limit)

    cur.execute(sql, params)
    rows = cur.fetchall()

    result = []
    for r in rows:
        item = {
            "operator": r["operator"],
            "shift_id": r["shift_id"],
            "variant": r["variant"] or "",
            "barcode": r["barcode"] or "",
            "analytics": r["analytics"] or {},
            "received_ts": r["received_ts"].astimezone(IST) if r["received_ts"] else None,
        }
        result.append(CycleReportItemAnalytics(**item))

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
    
async def get_current_operator() -> str:
    """
    Get the current logged-in operator from available session tables.
    Falls back to "system" if no operator is logged in.
    """
    try:
            
        # Check recent operator_sessions if no active session
        cur.execute("""
            SELECT username FROM operator_sessions
            WHERE logout_ts IS NULL
            ORDER BY login_ts DESC
            LIMIT 1
        """)
        row = cur.fetchone()
        if row and row["username"]:
            logger.info(f"Found active operator from operator_sessions table: {row['username']}")
            return row["username"]
    
    except Exception as e:
        logger.warning(f"Error getting current operator: {e}")
    
    # Fall back to "system" if no operator is found
    logger.info("No active operator found, using 'system'")
    return "system"

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
            await asyncio.sleep(0.5)
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
                    dt_ist = dt.astimezone(ZoneInfo("Asia/Kolkata"))
                    alarm_date = dt_ist.date()
                    alarm_time = dt_ist.time()
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
            await asyncio.sleep(0.5)
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
            await asyncio.sleep(0.5)
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
            await asyncio.sleep(0.5)
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
            await asyncio.sleep(0.5)
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
# async def consume_machine_status_and_populate_db():
#     """
#     Consume machine status from Kafka and process cycle data for database
#     """
#     for i in range(10):
#         try:
#             consumer = AIOKafkaConsumer(
#                 MACHINE_STATUS_TOPIC,
#                 bootstrap_servers=KAFKA_BOOTSTRAP,
#                 auto_offset_reset="earliest",
#                 value_deserializer=lambda b: json.loads(b.decode()),
#                 group_id="machine_status_db_processor"
#             )
#             await consumer.start()
#             logger.info("Saving Started machine status consumer for database processing")
#             break
#         except AIOKafkaNoBrokersAvailable:
#             await asyncio.sleep(0.5)
#     else:
#         raise RuntimeError("Cannot connect machine status consumer")

#     try:
#         async for msg in consumer:
#             payload = msg.value
#             logger.info(f"Saving Received payload: {payload}")

            
#             # Process machine status for cycle management
#             if "sets" in payload:
#                 logger.info(f"Saving Processing {len(payload['sets'])} sets from payload")
#                 for set_data in payload["sets"]:
#                     # Only process completed sets
#                     unload_status = set_data.get("progress", {}).get("unload_station", {}).get("status_1", 0)
#                     if unload_status != 1:
#                         continue  # Skip incomplete sets
#                     # Extract and clean the set_id and barcodes
#                     original_set_id = set_data.get("set_id", "")
#                     if original_set_id:
#                         # # Remove null bytes and + with any digits after it
#                         # original_set_id = original_set_id.replace("\x00", "")
#                         # if "+" in original_set_id:
#                         #     original_set_id = original_set_id.split("+")[0]
                        
#                         # Generate deterministic UUID for cycle_id
#                         cycle_id = str(uuid.uuid5(uuid.NAMESPACE_OID, original_set_id))
#                         logger.info(f"Saving Generated cycle_id: {cycle_id} from set_id: {original_set_id}")
                        
#                         # Process barcodes
#                         barcodes = set_data.get("barcodes", [])
#                         primary_barcode = ""
#                         if barcodes:
#                             primary_barcode = str(barcodes[0]) if barcodes[0] else ""
#                             # Clean barcode
#                             if primary_barcode:
#                                 primary_barcode = primary_barcode.replace("\x00", "")
#                                 if "_" in primary_barcode:
#                                     primary_barcode = primary_barcode.replace("_", "+")
#                                 if "\r" in primary_barcode:
#                                     primary_barcode = primary_barcode.split("\r")[0]
                        
#                                     logger.info(f"Saving Processed barcode: {primary_barcode}")
                        
#                         # Insert cycle if not exists
#                         try:
#                             cur.execute("""
#                                 INSERT INTO cycle_master(cycle_id, barcode, operator, shift_id, variant, start_ts)
#                                 VALUES (%s, %s, %s, %s, %s, %s)
#                                 ON CONFLICT (cycle_id) DO NOTHING
#                             """, (cycle_id, primary_barcode, "system", 1, "default", set_data.get("created_ts")))
                            
#                             if cur.rowcount > 0:
#                                 logger.info(f"Saving and inserted New cycle created into db: {cycle_id} with barcode: {primary_barcode}")
                            
#                             else:
#                                 logger.info(f"Saving Cycle already exists: {cycle_id}, skipping insert")
                                
#                         except Exception as e:
#                             logger.error(f"Error inserting cycle {cycle_id}: {e}")
                            
#             conn.commit()
#             logger.info("DB transaction committed")
            
#     except Exception as e:
#         logger.error(f"Error in machine status consumer: {e}")
#     finally:
#         await consumer.stop()
#         logger.info("Stopped machine status consumer")





# # modified kafka function for saving
# async def consume_machine_status_and_populate_db():
#     """
#     Consume machine status from Kafka and process cycle data for database.
#     Inserts one row per barcode (even duplicates) with a unique cycle_id each time.
#     """
#     for i in range(10):
#         try:
#             consumer = AIOKafkaConsumer(
#                 MACHINE_STATUS_TOPIC,
#                 bootstrap_servers=KAFKA_BOOTSTRAP,
#                 auto_offset_reset="earliest",
#                 value_deserializer=lambda b: json.loads(b.decode()),
#                 group_id="machine_status_db_processor"
#             )
#             await consumer.start()
#             logger.info("Saving Started machine status consumer for database processing")
#             break
#         except AIOKafkaNoBrokersAvailable:
#             logger.warning(f"Saving Kafka broker not available. Retry {i + 1}/10...")
#             await asyncio.sleep(0.5)
#     else:
#         raise RuntimeError("Saving Cannot connect machine status consumer")

#     try:
#         async for msg in consumer:
#             payload = msg.value
#             logger.debug(f"Saving Received payload: {payload}")

#             if "sets" in payload:
#                 for set_data in payload["sets"]:
#                     unload_status = set_data.get("progress", {}).get("unload_station", {}).get("status_1", 0)
#                     if unload_status != 1:
#                         logger.debug("Saving Skipping incomplete set")
#                         continue

#                     original_set_id = set_data.get("set_id", "")
#                     created_ts = set_data.get("created_ts")
#                     barcodes = set_data.get("barcodes", [])

#                     if not barcodes:
#                         logger.warning(f"Saving No barcodes found for set_id: {original_set_id}")
#                         continue

#                     for barcode in barcodes:
#                         if not barcode:
#                             continue

#                         # Clean barcode
#                         cleaned_barcode = barcode.replace("\x00", "")
#                         if "_" in cleaned_barcode:
#                             cleaned_barcode = cleaned_barcode.replace("_", "+")
#                         if "\r" in cleaned_barcode:
#                             cleaned_barcode = cleaned_barcode.split("\r")[0]

#                         # Generate a truly unique cycle_id
#                         cycle_id = str(uuid.uuid4())



#                         ist = ZoneInfo("Asia/Kolkata")
#                         try:
                            
#                             # Convert start_ts from payload to IST
#                             created_ts_raw = set_data.get("created_ts")
#                             start_ts_ist = datetime.fromisoformat(created_ts_raw).astimezone(ist) if created_ts_raw else None
#                             logger.info(f"Saving start_ts_ist time: {start_ts_ist} ")

#                             # Generate end_ts as current time in IST also removed microseconds.
#                             end_ts_ist = datetime.now(tz=ist).replace(microsecond=0)
#                             logger.info(f"Saving end_ts_ist time: {end_ts_ist} ")

#                             # Calculate time difference in seconds
#                             difference_seconds = int((end_ts_ist - start_ts_ist).total_seconds()) if start_ts_ist else None
#                             logger.info(f"Saving difference_seconds time: {difference_seconds} ")

#                             cur.execute("""
#                                 INSERT INTO cycle_master(cycle_id, barcode, operator, shift_id, variant, start_ts, end_ts, difference)
#                                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
#                             """, (cycle_id, cleaned_barcode, "system", 1, "default", start_ts_ist, end_ts_ist, difference_seconds))

#                             logger.info(f"Saving Inserted new cycle: {cycle_id} | Barcode: {cleaned_barcode}")

#                         except Exception as e:
#                             logger.error(f"Saving Error inserting cycle for barcode {cleaned_barcode}: {e}")

#                 conn.commit()
#                 logger.debug("Saving Committed DB transaction")

#     except Exception as e:
#         logger.error(f"Error in machine status consumer: {e}")

#     finally:
#         await consumer.stop()
#         logger.info("Stopped machine status consumer")  

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
            await asyncio.sleep(0.5)
    else:
        raise RuntimeError("Cannot connect machine status consumer")

    try:
        async for msg in consumer:
            payload = msg.value
            
            # Process machine status for cycle management
            if "sets" in payload:
                for set_data in payload["sets"]:
                    # Only process completed sets (those at unload station with status_1=1)
                    unload_status = set_data.get("progress", {}).get("unload_station", {}).get("status_1", 0)
                    
                    if unload_status == 1:
                        try:
                            set_id = set_data.get("set_id")
                            barcodes = set_data.get("barcodes", [])
                            barcode = barcodes[0] if barcodes else ""
                            created_ts = set_data.get("created_ts")
                            last_update = set_data.get("last_update")
                            
                            # Check if this completed cycle already exists in the database
                            cur.execute("SELECT cycle_id FROM cycle_master WHERE barcode = %s AND unload_status = TRUE", 
                                       (barcode,))
                            if cur.fetchone():
                                logger.debug(f"Cycle for barcode {barcode} already in database, skipping")
                                continue
                                
                            # Get current operator (if available) or use "system"
                            operator = await get_current_operator()
                            logger.info(f"Using operator '{operator}' for cycle with barcode {barcode}")
                            
                            # Get current shift from shift_master table
                            cur.execute("""
                              SELECT id FROM shift_master
                               WHERE (start_time < end_time AND start_time <= NOW()::time AND NOW()::time < end_time)
                                  OR (start_time > end_time AND (NOW()::time >= start_time OR NOW()::time < end_time))
                              LIMIT 1
                            """)
                            row = cur.fetchone()
                            shift_id = row["id"] if row else 1  # Default to shift 1 if none found
                            
                            # Generate a unique cycle_id
                            cycle_id = str(uuid.uuid4())
                            
                            # Insert the completed cycle
                            cur.execute("""
                                INSERT INTO cycle_master(
                                    cycle_id, operator, shift_id, variant, barcode, 
                                    start_ts, end_ts, unload_status, unload_ts
                                ) VALUES (
                                    %s, %s, %s, %s, %s, 
                                    %s, %s, TRUE, %s
                                )
                            """, (
                                cycle_id, operator, shift_id, "default", barcode,
                                created_ts, last_update, last_update
                            ))
                            
                            # Add progress stations as cycle events
                            for station, details in set_data.get("progress", {}).items():
                                if details.get("latched", False) and details.get("ts"):
                                    cur.execute("""
                                        INSERT INTO cycle_event(cycle_id, stage, ts)
                                        VALUES (%s, %s, %s)
                                    """, (cycle_id, station, details.get("ts")))
                            
                            logger.info(f"Added completed cycle {cycle_id} for barcode {barcode}")
                            
                        except Exception as e:
                            logger.error(f"Error processing completed set {set_data.get('set_id')}: {e}")
                            logger.error(traceback.format_exc())
                            
            conn.commit()
            
    except Exception as e:
        logger.error(f"Error in machine status consumer: {e}")
        logger.error(traceback.format_exc())
    finally:
        await consumer.stop()
        logger.info("Stopped machine status consumer")

# @app.websocket("/ws/machine-status")
# async def websocket_machine_status(websocket: WebSocket):
#     """
#     Dedicated WebSocket endpoint for machine status with support for:
#     1. Real-time set updates
#     2. Handling both individual and batch updates
#     3. Initial state delivery
#     """
#     logger.info("WebSocket connection attempt to /ws/machine-status")
#     await mgr.connect("machine-status", websocket)
    
#     logger.info("WebSocket connected to machine-status")

#     try:
#         # Send initial full state on connection
#         consumer = AIOKafkaConsumer(
#             MACHINE_STATUS_TOPIC,  # Listen to the main topic, not .full suffix
#             bootstrap_servers=KAFKA_BOOTSTRAP,
#             auto_offset_reset="latest",
#             group_id=f"ws-init-{str(uuid.uuid4())}",
#             consumer_timeout_ms=5000,
#             value_deserializer=lambda b: json.loads(b.decode())
#         )
#         await consumer.start()
        
#         # Try to get the latest full update
#         try:
#             start_time = time.time()
#             async for msg in consumer:
#                 # Forward any message with "sets" field as initial state
#                 if msg.value and "sets" in msg.value:
#                     await websocket.send_json(msg.value)
#                     logger.info("Sent initial machine status to new client")
#                     break
                
#                 # Timeout after 2 seconds
#                 if time.time() - start_time > 2:
#                     logger.warning("Timeout waiting for initial machine status")
#                     break
#         except Exception as e:
#             logger.error(f"Error fetching initial machine status: {e}")
        
#         await consumer.stop()
        
#         # Stay connected for ongoing messages
#         await asyncio.Future() 
#         # while True:
#         #     data = await websocket.receive_text()
#         #     await websocket.send_json({"type": "ack", "message": "received"})
            
#     except WebSocketDisconnect:
#         logger.info("WebSocket disconnected from machine-status")
#         mgr.disconnect("machine-status", websocket)
#     except Exception as e:
#         logger.error(f"Error in machine-status WebSocket: {e}")
#         if hasattr(websocket, 'client_state') and websocket.client_state.state != 4:
#             await websocket.close(code=1011, reason=f"Error: {str(e)}")


# @app.websocket("/ws/machine-status")
# async def websocket_machine_status(websocket: WebSocket):
#     """
#     WebSocket endpoint for real-time machine status updates.
#     Sends initial full state and continues with incremental updates.
#     """
#     logger.info("WebSocket connection attempt to /ws/machine-status")
#     await mgr.connect("machine-status", websocket)
#     logger.info("WebSocket connected to machine-status")

#     consumer = AIOKafkaConsumer(
#         MACHINE_STATUS_TOPIC,
#         bootstrap_servers=KAFKA_BOOTSTRAP,
#         auto_offset_reset="latest",
#         group_id=f"ws-{uuid.uuid4()}",
#         value_deserializer=lambda b: json.loads(b.decode()),
#     )

#     try:
#         await consumer.start()

#         # === Initial state (one-time full message with "sets") ===
#         try:
#             start_time = time.time()
#             async for msg in consumer:
#                 if msg.value and "sets" in msg.value:
#                     # --- DEBUG: Log the message before sending ---
#                     logger.info(f"KAFKA_MSG (initial): {msg.value}")
                    
#                     # --- Temporarily disable sending to WebSocket ---
#                     # await websocket.send_json(msg.value)
                    
#                     logger.info("Sent initial machine status to client") # This log will now mean "processed initial message"
#                     break
#                 if time.time() - start_time > 2:
#                     logger.warning("Timeout waiting for initial machine status")
#                     break
#         except Exception as e:
#             logger.error(f"Error fetching initial machine status: {e}")

#         # === Continuous updates ===
#         async for msg in consumer:
#             try:
#                 # --- DEBUG: Log the message before sending ---
#                 logger.info(f"KAFKA_MSG (update): {msg.value}")

#                 # --- Temporarily disable sending to WebSocket ---
#                 # await websocket.send_json(msg.value)

#             except Exception as e:
#                 logger.error(f"Error sending update to WebSocket: {e}")
#                 break  # Exit on WebSocket send failure

#     except WebSocketDisconnect:
#         logger.info("WebSocket disconnected from machine-status")
#     except Exception as e:
#         logger.error(f"Unexpected WebSocket error: {e}")
#         if hasattr(websocket, 'client_state') and websocket.client_state.state != 4:
#             await websocket.close(code=1011, reason=f"Error: {str(e)}")
#     finally:
#         await consumer.stop()
#         mgr.disconnect("machine-status", websocket)

@app.websocket("/ws/machine-status")
async def websocket_machine_status(websocket: WebSocket):
    """
    WebSocket endpoint for testing the Kafka channel for machine status.
    It connects and logs every message received from the topic.
    """
    logger.info("WebSocket connection attempt to /ws/machine-status")
    await mgr.connect("machine-status", websocket)
    logger.info("WebSocket connected to machine-status for testing.")

    consumer = AIOKafkaConsumer(
        MACHINE_STATUS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="latest",
        group_id=f"ws-test-{uuid.uuid4()}",  # Use a unique group_id for testing
        value_deserializer=lambda b: json.loads(b.decode()),
    )

    try:
        await consumer.start()
        logger.info("Kafka consumer started for machine-status testing.")

        # Single continuous loop to log all messages
        async for msg in consumer:
            try:
                # Log every message received from the Kafka topic
                logger.info(f"KAFKA_MSG: {msg.value}")

                # The websocket.send_json call is intentionally commented out for testing
                # await websocket.send_json(msg.value)

            except Exception as e:
                logger.error(f"Error processing message: {e}")
                # Continue to the next message
                continue

    except WebSocketDisconnect:
        logger.info("WebSocket disconnected from machine-status")
    except Exception as e:
        logger.error(f"Unexpected WebSocket error: {e}")
        if hasattr(websocket, 'client_state') and websocket.client_state.state != 4:
            await websocket.close(code=1011, reason=f"Error: {str(e)}")
    finally:
        # Ensure consumer is stopped and connection is cleaned up
        await consumer.stop()
        mgr.disconnect("machine-status", websocket)
        logger.info("Cleaned up resources for machine-status WebSocket.")


@app.websocket("/ws/plc-write")
async def ws_plc_write(ws: WebSocket):
    logger.info("WebSocket connection attempt to /ws/plc-write")
    await mgr.connect("plc-write-responses", ws)
    logger.info("WebSocket connected to plc-write")
    try:
        while True:
            data_raw = await ws.receive_text()  # Get as text first
            data = json.loads(data_raw)  # Parse JSON
            logger.info(f"Received WebSocket data: {data_raw[:100]}...")
            
            try:
                cmd = PlcWriteCommand(**data)
                rid = cmd.request_id or str(uuid.uuid4())
                cmd.request_id = rid    
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
        logger.info("WebSocket disconnected from plc-write")
        mgr.disconnect("plc-write", ws)
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

# Update the startup function to include alarm processing
@app.on_event("startup")
async def on_startup():
    dedicated_streams = {"machine-status", "plc-write", "plc-write-responses"}
    await init_kafka_producer()
    
    # Start WebSocket streaming for all topics EXCEPT machine-status
    for name, topic in WS_TOPICS.items():
        # if name not in dedicated_streams:
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
