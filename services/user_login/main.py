import os
import time
import uuid
import asyncio
from datetime import datetime, time as dtime, timedelta
import psycopg2
import requests
from fastapi import FastAPI, HTTPException, Depends, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
from aiokafka import AIOKafkaProducer

# ── Environment & DB Setup ─────────────────────────────────────────
DB_URL = os.getenv("DB_URL", "postgresql://edge:edgepass@postgres:5432/edgedb")
for _ in range(10):
    try:
        conn = psycopg2.connect(DB_URL)
        break
    except psycopg2.OperationalError:
        print("Postgres not ready, retrying…")
        time.sleep(2)
else:
    raise RuntimeError("Cannot connect to Postgres")
conn.autocommit = True
cur = conn.cursor()

# ── Ensure New Tables Exist ────────────────────────────────────────
cur.execute("""
CREATE TABLE IF NOT EXISTS shift_master (
  id SERIAL PRIMARY KEY,
  name TEXT UNIQUE NOT NULL,
  start_time TIME NOT NULL,
  end_time TIME NOT NULL
);
""")
cur.execute("""
CREATE TABLE IF NOT EXISTS machine_config (
  id SERIAL PRIMARY KEY,
  machine_id TEXT UNIQUE NOT NULL,
  mes_process_control_url TEXT NOT NULL,
  mes_upload_url TEXT NOT NULL,
  is_mes_enabled BOOLEAN NOT NULL DEFAULT TRUE
);
""")

# ── Seed Shifts (if missing) ──────────────────────────────────────
PREDEFINED_SHIFTS = [
    ("Shift A", dtime(6,0),  dtime(14,0)),
    ("Shift B", dtime(14,0), dtime(22,0)),
    ("Shift C", dtime(22,0), dtime(6,0))
]
for name, st, et in PREDEFINED_SHIFTS:
    cur.execute("""
      INSERT INTO shift_master(name,start_time,end_time)
      VALUES(%s,%s,%s) ON CONFLICT(name) DO NOTHING;
    """, (name, st, et))

# ── Kafka Producer Setup for PLC Writes ───────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BROKER", "kafka:9092")
PLC_WRITE_TOPIC = os.getenv("PLC_WRITE_COMMANDS_TOPIC", "plc_write_commands")
kafka_producer: AIOKafkaProducer

async def init_kafka():
    global kafka_producer
    kafka_producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    await kafka_producer.start()
    print("Kafka producer started.")

# ── FastAPI & Auth ────────────────────────────────────────────────
app = FastAPI()
_current_user: str = None

def require_login():
    if not _current_user:
        raise HTTPException(401, "Not logged in")

# ── WebSocket Manager ─────────────────────────────────────────────
class ConnectionManager:
    def __init__(self):
        self.clients: set[WebSocket] = set()
    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.clients.add(ws)
    def disconnect(self, ws: WebSocket):
        self.clients.discard(ws)
    async def broadcast(self, msg: dict):
        living = set()
        for ws in self.clients:
            try:
                await ws.send_json(msg)
                living.add(ws)
            except:
                pass
        self.clients = living

ws_mgr = ConnectionManager()

@app.websocket("/ws/login-status")
async def ws_login_status(ws: WebSocket):
    await ws_mgr.connect(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        ws_mgr.disconnect(ws)

# ── Models ────────────────────────────────────────────────────────
class LoginRequest(BaseModel):
    Username: str
    Password: str

class LogoutRequest(BaseModel):
    Username: str

# ── Startup Event ─────────────────────────────────────────────────
@app.on_event("startup")
async def on_startup():
    await init_kafka()
    # Schedule shift‐end watcher
    asyncio.create_task(shift_end_watcher())

# ── Helper: Determine current shift ────────────────────────────────
def get_current_shift():
    now = datetime.now().time()
    cur.execute("SELECT id, name, start_time, end_time FROM shift_master;")
    for sid,name,st,et in cur.fetchall():
        if st < et and st <= now < et or (st > et and (now >= st or now < et)):
            return sid,name,st,et
    return None, None, None, None
    

# ── 5) Auto-logout at shift end ─────────────────────────────────────
async def shift_end_watcher():
    while True:
        sid,name,st,et = get_current_shift()
        if sid:
            # compute next end datetime
            today = datetime.now().date()
            end_dt = datetime.combine(today, et)
            if et < st:  # overnight
                end_dt += timedelta(days=1)
            delay = (end_dt - datetime.now()).total_seconds()
            await asyncio.sleep(max(delay,0))
            # before logout: read auto_status once (stubbed here)
            cur.execute("SELECT value FROM config WHERE key='AUTO_STATUS';")
            auto_status = cur.fetchone()
            cur.execute("""
              INSERT INTO operator_sessions(username,login_ts)
              VALUES (%s, NOW()) RETURNING id
            """,(auto_status or ["UNKNOWN"],))
            # write PLC register=3309 → value=0 (auto logout)
            req_id = str(uuid.uuid4())
            await kafka_producer.send_and_wait(PLC_WRITE_TOPIC, {
              "section": "login", "tag_name": "login", "value": 0, "request_id": req_id
            })
            # broadcast
            await ws_mgr.broadcast({
              "event":"auto-logout",
              "shift": name,
              "timestamp": datetime.now().isoformat(),
              "auto_status": auto_status
            })
        else:
            # no active shift—check again in 5m
            await asyncio.sleep(300)

# ── 1➞3) Login Endpoint ────────────────────────────────────────────
@app.post("/api/login")
async def login(req: LoginRequest):
    global _current_user
    # 1) Verify operator in config
    allowed = os.getenv("OPERATOR_IDS","").split(",")
    if req.Username not in allowed:
        raise HTTPException(403, "Operator not permitted")

    # 2) (Optionally) call MES login URL from machine_config
    cur.execute("SELECT mes_process_control_url,mes_upload_url,is_mes_enabled FROM machine_config WHERE machine_id=%s",
                (os.getenv("MACHINE_ID"),))
    row = cur.fetchone()
    if row and not row[2]:
        raise HTTPException(503, "MES APIs disabled for this machine")

    # 3) Determine shift
    sid, shift_name, st, et = get_current_shift()
    if not sid:
        raise HTTPException(400, "No active shift")

    # 4) Record login
    _current_user = req.Username
    cur.execute("""
      INSERT INTO operator_sessions(username,login_ts,shift_id)
      VALUES (%s,NOW(),%s)
    """,(req.Username,sid))

    # 5) Write PLC register 3309 → 1
    req_id = str(uuid.uuid4())
    await kafka_producer.send_and_wait(PLC_WRITE_TOPIC, {
      "section":"login","tag_name":"login","value":1,"request_id":req_id
    })

    # 6) Broadcast via WS
    await ws_mgr.broadcast({
      "event":"login",
      "username": req.Username,
      "shift": shift_name,
      "shift_start": st.isoformat(),
      "shift_end": et.isoformat(),
      "ts": datetime.now().isoformat()
    })

    return {
      "message":"Login successful",
      "username":req.Username,
      "shift_id": sid,
      "shift_name": shift_name,
      "shift_start": st.isoformat(),
      "shift_end": et.isoformat(),
      "mes_urls": {
        "process_control": row[0] if row else None,
        "upload": row[1] if row else None
      }
    }

# ── 6) Logout Endpoint ─────────────────────────────────────────────
@app.post("/api/logout")
async def logout(req: LogoutRequest):
    global _current_user
    if req.Username != _current_user:
        raise HTTPException(400, "Not logged in")
    _current_user = None
    # PLC write =2
    req_id = str(uuid.uuid4())
    await kafka_producer.send_and_wait(PLC_WRITE_TOPIC, {
      "section":"login","tag_name":"login","value":2,"request_id":req_id
    })
    await ws_mgr.broadcast({
      "event":"logout","username":req.Username,"ts":datetime.now().isoformat()
    })
    return {"message":"Logged out"}

# ── Login verify ────────────────────────────────────
@app.get("/api/verify")
def verify(token: str = Header(None, alias="X-Auth-Token")):
    """
    Confirms that the token corresponds to a logged-in operator.
    """
    # e.g., look up in Redis/JWT or in‐memory map
    operator = session_store.get(token)
    if not operator:
        raise HTTPException(401, "Invalid session")
    return {"username": operator}


# ── Protect all other endpoints ────────────────────────────────────
@app.middleware("http")
async def auth_middleware(request, call_next):
    if request.url.path.startswith("/api") and request.url.path not in ["/api/login","/api/logout"]:
        if not _current_user:
            raise HTTPException(401,"Not logged in")
    return await call_next(request)

""")

# ── Seed Shifts (if missing) ──────────────────────────────────────
PREDEFINED_SHIFTS = [
    ("Shift A", dtime(6,0),  dtime(14,0)),
    ("Shift B", dtime(14,0), dtime(22,0)),
    ("Shift C", dtime(22,0), dtime(6,0))
]
for name, st, et in PREDEFINED_SHIFTS:
    cur.execute("""
      INSERT INTO shift_master(name,start_time,end_time)
      VALUES(%s,%s,%s) ON CONFLICT(name) DO NOTHING;
    """, (name, st, et))

# ── Kafka Producer Setup for PLC Writes ───────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BROKER", "kafka:9092")
PLC_WRITE_TOPIC = os.getenv("PLC_WRITE_COMMANDS_TOPIC", "plc_write_commands")
kafka_producer: AIOKafkaProducer

async def init_kafka():
    global kafka_producer
    kafka_producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    await kafka_producer.start()
    print("Kafka producer started.")

# ── FastAPI & Auth ────────────────────────────────────────────────
app = FastAPI()
_current_user: str = None

def require_login():
    if not _current_user:
        raise HTTPException(401, "Not logged in")

# ── WebSocket Manager ─────────────────────────────────────────────
class ConnectionManager:
    def __init__(self):
        self.clients: set[WebSocket] = set()
    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.clients.add(ws)
    def disconnect(self, ws: WebSocket):
        self.clients.discard(ws)
    async def broadcast(self, msg: dict):
        living = set()
        for ws in self.clients:
            try:
                await ws.send_json(msg)
                living.add(ws)
            except:
                pass
        self.clients = living

ws_mgr = ConnectionManager()

@app.websocket("/ws/login-status")
async def ws_login_status(ws: WebSocket):
    await ws_mgr.connect(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        ws_mgr.disconnect(ws)

# ── Models ────────────────────────────────────────────────────────
class LoginRequest(BaseModel):
    Username: str
    Password: str

class LogoutRequest(BaseModel):
    Username: str

# ── Startup Event ─────────────────────────────────────────────────
@app.on_event("startup")
async def on_startup():
    await init_kafka()
    # Schedule shift‐end watcher
    asyncio.create_task(shift_end_watcher())

# ── Helper: Determine current shift ────────────────────────────────
def get_current_shift():
    now = datetime.now().time()
    cur.execute("SELECT id,name,start_time,end_time FROM shift_master;")
    for sid,name,st,et in cur.fetchall():
        if st < et and st <= now < et or (st > et and (now >= st or now < et)):
            return sid,name,st,et
    return None, None, None, None

# ── 5) Auto-logout at shift end ─────────────────────────────────────
async def shift_end_watcher():
    while True:
        sid,name,st,et = get_current_shift()
        if sid:
            # compute next end datetime
            today = datetime.now().date()
            end_dt = datetime.combine(today, et)
            if et < st:  # overnight
                end_dt += timedelta(days=1)
            delay = (end_dt - datetime.now()).total_seconds()
            await asyncio.sleep(max(delay,0))
            # before logout: read auto_status once (stubbed here)
            cur.execute("SELECT value FROM config WHERE key='AUTO_STATUS';")
            auto_status = cur.fetchone()
            cur.execute("""
              INSERT INTO operator_sessions(username,login_ts)
              VALUES (%s, NOW()) RETURNING id
            """,(auto_status or ["UNKNOWN"],))
            # write PLC register=3309 → value=0 (auto logout)
            req_id = str(uuid.uuid4())
            await kafka_producer.send_and_wait(PLC_WRITE_TOPIC, {
              "section": "login", "tag_name": "login", "value": 0, "request_id": req_id
            })
            # broadcast
            await ws_mgr.broadcast({
              "event":"auto-logout",
              "shift": name,
              "timestamp": datetime.now().isoformat(),
              "auto_status": auto_status
            })
        else:
            # no active shift—check again in 5m
            await asyncio.sleep(300)

# ── 1➞3) Login Endpoint ────────────────────────────────────────────
@app.post("/api/login")
async def login(req: LoginRequest):
    global _current_user
    # 1) Verify operator in config
    allowed = os.getenv("OPERATOR_IDS","").split(",")
    if req.Username not in allowed:
        raise HTTPException(403, "Operator not permitted")

    # 2) (Optionally) call MES login URL from machine_config
    cur.execute("SELECT mes_process_control_url,mes_upload_url,is_mes_enabled FROM machine_config WHERE machine_id=%s",
                (os.getenv("MACHINE_ID"),))
    row = cur.fetchone()
    if row and not row[2]:
        raise HTTPException(503, "MES APIs disabled for this machine")

    # 3) Determine shift
    sid, shift_name, st, et = get_current_shift()
    if not sid:
        raise HTTPException(400, "No active shift")

    # 4) Record login
    _current_user = req.Username
    cur.execute("""
      INSERT INTO operator_sessions(username,login_ts,shift_id)
      VALUES (%s,NOW(),%s)
    """,(req.Username,sid))

    # 5) Write PLC register 3309 → 1
    req_id = str(uuid.uuid4())
    await kafka_producer.send_and_wait(PLC_WRITE_TOPIC, {
      "section":"login","tag_name":"login","value":1,"request_id":req_id
    })

    # 6) Broadcast via WS
    await ws_mgr.broadcast({
      "event":"login",
      "username": req.Username,
      "shift": shift_name,
      "shift_start": st.isoformat(),
      "shift_end": et.isoformat(),
      "ts": datetime.now().isoformat()
    })

    return {
      "message":"Login successful",
      "username":req.Username,
      "shift_id": sid,
      "shift_name": shift_name,
      "shift_start": st.isoformat(),
      "shift_end": et.isoformat(),
      "mes_urls": {
        "process_control": row[0] if row else None,
        "upload": row[1] if row else None
      }
    }

# ── 6) Logout Endpoint ─────────────────────────────────────────────
@app.post("/api/logout")
async def logout(req: LogoutRequest):
    global _current_user
    if req.Username != _current_user:
        raise HTTPException(400, "Not logged in")
    _current_user = None
    # PLC write =2
    req_id = str(uuid.uuid4())
    await kafka_producer.send_and_wait(PLC_WRITE_TOPIC, {
      "section":"login","tag_name":"login","value":2,"request_id":req_id
    })
    await ws_mgr.broadcast({
      "event":"logout","username":req.Username,"ts":datetime.now().isoformat()
    })
    return {"message":"Logged out"}

# ── Login verify ────────────────────────────────────
@app.get("/api/verify")
def verify(token: str = Header(None, alias="X-Auth-Token")):
    """
    Confirms that the token corresponds to a logged-in operator.
    """
    # e.g., look up in Redis/JWT or in‐memory map
    operator = session_store.get(token)
    if not operator:
        raise HTTPException(401, "Invalid session")
    return {"username": operator}

# ── current operator ────────────────────────────────────
@app.get("/api/current_operator", status_code=status.HTTP_200_OK)
def current_operator():
    """
    Returns the operator who most recently logged in (and not yet logged out).
    """
    if not _current_user:
        raise HTTPException(404, "No operator currently logged in")
    sid, shift_name, st, et = get_current_shift()
    return {
      "username":   _current_user,
      "shift_id":   sid,
      "shift_name": shift_name,
      "shift_start": st.isoformat(),
      "shift_end":   et.isoformat(),
    }

# ── Protect all other endpoints ────────────────────────────────────
@app.middleware("http")
async def auth_middleware(request, call_next):
    if request.url.path.startswith("/api") and request.url.path not in ["/api/login","/api/logout"]:
        if not _current_user:
            raise HTTPException(401,"Not logged in")
    return await call_next(request)
