import os, uuid, asyncio, json, time as pytime, requests
from datetime import datetime, timedelta, time as dtime
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Header, status
from pydantic import BaseModel, validator
import psycopg2
from aiokafka import AIOKafkaProducer
from pymodbus.client.tcp import AsyncModbusTcpClient
from passlib.hash import bcrypt

def verify_password(plain: str, hashed: str) -> bool:
    return bcrypt.verify(plain, hashed)

# toggle to skip the external MES login call while it’s not available
SKIP_MES_LOGIN = os.getenv("SKIP_MES_LOGIN", "false").lower() in ("1","true")
OPERATOR_IDS  = os.getenv("OPERATOR_IDS", "op1").split(",")
# ─── Env & DB ───────────────────────────────────────────────
DB_URL       = os.getenv("DB_URL","postgresql://edge:edgepass@postgres:5432/edgedb")
KAFKA_BOOT   = os.getenv("KAFKA_BROKER","kafka:9092")
PLC_IP       = os.getenv("PLC_IP","192.168.10.3")
PLC_PORT     = int(os.getenv("PLC_PORT","502"))
LOGIN_BIT    = int(os.getenv("MODE_REGISTER","3309"))   # PLC register for login

# connect to Postgres
for _ in range(10):
    try:
        conn = psycopg2.connect(DB_URL)
        break
    except psycopg2.OperationalError:
        pytime.sleep(2)
else:
    raise RuntimeError("Cannot connect to Postgres")
conn.autocommit = True
cur = conn.cursor()

# ensure sessions tables exist (init.sql should have done this):
# ── ensure our session tables + enum exist ─────────────────────────
# (we split the statements so Postgres parses them cleanly)
try:
    cur.execute("CREATE TYPE user_role AS ENUM ('operator','admin','engineer')")
except psycopg2.errors.DuplicateObject:
    # type already exists, no-op
    pass

cur.execute("""
  CREATE TABLE IF NOT EXISTS sessions (
    token      UUID       PRIMARY KEY,
    username   TEXT       NOT NULL,
    role       user_role  NOT NULL,
    shift_id   INTEGER    NOT NULL REFERENCES shift_master(id),
    login_ts   TIMESTAMP  NOT NULL DEFAULT NOW(),
    logout_ts  TIMESTAMP
  )
""")

cur.execute("""
  CREATE TABLE IF NOT EXISTS operator_sessions (
    id         SERIAL     PRIMARY KEY,
    username   TEXT       NOT NULL,
    shift_id   INTEGER    NOT NULL REFERENCES shift_master(id),
    login_ts   TIMESTAMP  NOT NULL DEFAULT NOW(),
    logout_ts  TIMESTAMP
  )
""")

# ─── Kafka producer ───────────────────────────────────────────
producer: AIOKafkaProducer
async def init_kafka():
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOT,
        value_serializer=lambda v: json.dumps(v).encode()
    )
    await producer.start()

# ─── FastAPI + WS ────────────────────────────────────────────
app = FastAPI()
class LoginReq(BaseModel):
    Username: str
    Password: str
    Role: str

    @validator('Role')
    def must_be_valid_role(cls, v):
        if v not in ('operator','admin','engineer'):
            raise ValueError("Role must be one of operator,admin,engineer")
        return v

# WS manager for login-status
class WSManager:
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

ws_mgr = WSManager()

@app.websocket("/ws/login-status")
async def ws_login_status(ws: WebSocket):
    await ws_mgr.connect(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        ws_mgr.disconnect(ws)

# ─── shift lookup ──────────────────────────────────────────────
def current_shift():
    now = datetime.now().time()
    cur.execute("SELECT id,name,start_time,end_time FROM shift_master;")
    for sid,name,st,et in cur.fetchall():
        if (st < et and st <= now < et) or (st > et and (now >= st or now < et)):
            return sid,name,st,et
    return None,None,None,None

# ─── auto-logout watcher ────────────────────────────────────────
async def shift_watcher():
    while True:
        sid,name,st,et = current_shift()
        if sid:
            # compute next end
            today = datetime.now().date()
            end_dt = datetime.combine(today, et)
            if et < st:
                end_dt += timedelta(days=1)
            await asyncio.sleep(max((end_dt - datetime.now()).total_seconds(), 0))

            # read current PLC login‐bit
            client = AsyncModbusTcpClient(host=PLC_IP, port=PLC_PORT)
            await client.connect()
            prev = None
            if client.connected:
                rr = await client.read_holding_registers(LOGIN_BIT,1)
                if not rr.isError():
                    prev = rr.registers[0]
            await client.close()

            # log in auto_status_log
            cur.execute("""
              INSERT INTO auto_status_log(shift_id,status_val,ts)
              VALUES(%s,%s,NOW())
            """,(sid,prev))

            # mark operator_sessions
            cur.execute("""
              UPDATE operator_sessions
                 SET logout_ts=NOW()
               WHERE shift_id=%s AND logout_ts IS NULL
            """,(sid,))

            # send PLC write=0
            req_id = str(uuid.uuid4())
            await producer.send_and_wait("plc_write_commands", {
                "section":"login","tag_name":"login","value":0,"request_id":req_id
            })

            # WS broadcast
            await ws_mgr.broadcast({
              "event":"auto-logout",
              "shift": name,
              "prev_status": prev,
              "ts": datetime.utcnow().isoformat()+"Z"
            })

        else:
            await asyncio.sleep(300)  # no active shift

# ─── startup ───────────────────────────────────────────────────
@app.on_event("startup")
async def startup():
    await init_kafka()
    asyncio.create_task(shift_watcher())

# ─── Login / Logout / Verify / Current ──────────────────────────
@app.post("/api/login")
async def login(req: LoginReq):
    
    # 1) fetch stored hash & role
    if req.Role == "operator" and SKIP_MES_LOGIN:
        # only check membership in OPERATOR_IDS
        if req.Username not in OPERATOR_IDS:
            raise HTTPException(403, f"Operator '{req.Username}' not permitted")
    else:   
        cur.execute(
            "SELECT password_hash, role FROM users WHERE username = %s",
            (req.Username,)
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(403, f"User '{req.Username}' not found")
        stored_hash, actual_role = row

        # 2) enforce declared role
        if actual_role != req.Role:
            raise HTTPException(403, f"User '{req.Username}' is not a {req.Role}")

        # 3) credential check
        
        if req.Role == "operator":
            if not SKIP_MES_LOGIN:
                # delegate to MES
                cur.execute(
                    "SELECT value FROM config WHERE key = 'MES_OPERATOR_LOGIN_URL'"
                )
                mes_url = cur.fetchone()[0]
                resp = requests.post(
                    mes_url,
                    json={"Username": req.Username, "Password": req.Password},
                    timeout=5
                )
                if resp.status_code != 200 or not resp.json().get("IsSuccessful"):
                    raise HTTPException(403, "MES login failed")
            else:
                # ── TEMP SKIP MES: verify against local users table instead ──
                # (so you can still run the service end-to-end)
                if not verify_password(req.Password, stored_hash):
                    raise HTTPException(401, "Invalid operator credentials (local DB check)")
        else:
            # local bcrypt check for admin/engineer
            if not verify_password(req.Password, stored_hash):
                raise HTTPException(401, "Invalid credentials")

    # 4) determine current shift
    sid, shift_name, st, et = current_shift()
    if not sid:
        raise HTTPException(400, "No active shift")

    # 5) persist session
    token = str(uuid.uuid4())
    cur.execute("""
      INSERT INTO sessions(token, username, role, shift_id, login_ts)
      VALUES (%s, %s, %s, %s, NOW())
    """, (token, req.Username, req.Role, sid))

    if req.Role == "operator":
        cur.execute("""
          INSERT INTO operator_sessions(username, shift_id, login_ts)
          VALUES (%s, %s, NOW())
        """, (req.Username, sid))

    # 6) fire PLC login‐bit = 1
    request_id = str(uuid.uuid4())
    await producer.send_and_wait("plc_write_commands", {
      "section":    "login",
      "tag_name":   "login",
      "value":      1,
      "request_id": request_id
    })

    # 7) broadcast WebSocket event
    await ws_mgr.broadcast({
      "event":        "login",
      "username":     req.Username,
      "role":         req.Role,
      "shift":        shift_name,
      "shift_start":  st.isoformat(),
      "shift_end":    et.isoformat(),
      "ts":           datetime.utcnow().isoformat() + "Z"
    })

    # 8) response
    return {
      "token":        token,
      "username":     req.Username,
      "role":         req.Role,
      "shift_id":     sid,
      "shift_name":   shift_name,
      "shift_start":  st.isoformat(),
      "shift_end":    et.isoformat()
    }


@app.post("/api/logout")
async def logout(token: str = Header(...,alias="X-Auth-Token")):
    # verify
    cur.execute("SELECT username,shift_id FROM sessions WHERE token=%s AND logout_ts IS NULL",(token,))
    row=cur.fetchone()
    if not row:
        raise HTTPException(401,"Invalid session")
    user,sid=row

    # mark logout
    cur.execute("UPDATE sessions SET logout_ts=NOW() WHERE token=%s",(token,))
    cur.execute("""
      UPDATE operator_sessions SET logout_ts=NOW()
       WHERE username=%s AND shutdown_ts IS NULL
    """,(user,))

    # PLC write=2
    req_id = str(uuid.uuid4())
    await producer.send_and_wait("plc_write_commands", {
      "section":"login","tag_name":"login","value":2,"request_id":req_id
    })

    await ws_mgr.broadcast({"event":"logout","username":user,"ts":datetime.utcnow().isoformat()+"Z"})
    return {"message":"Logged out"}

@app.get("/api/verify")
def verify(token: str = Header(None,alias="X-Auth-Token")):
    cur.execute("SELECT username,role,shift_id FROM sessions WHERE token=%s AND logout_ts IS NULL",(token,))
    row=cur.fetchone()
    if not row:
        raise HTTPException(401,"Invalid session")
    return {"username":row[0],"role":row[1],"shift_id":row[2]}

@app.get("/api/current_operator")
def current_operator(token: str = Header(None,alias="X-Auth-Token")):
    cur.execute("SELECT username,shift_id FROM sessions WHERE token=%s AND logout_ts IS NULL",(token,))
    row=cur.fetchone()
    if not row:
        raise HTTPException(404,"No operator logged in")
    sid=row[1]
    cur.execute("SELECT name,start_time,end_time FROM shift_master WHERE id=%s",(sid,))
    name,st,et=cur.fetchone()
    return {"username":row[0],"shift_id":sid,"shift_name":name,"shift_start":st.isoformat(),"shift_end":et.isoformat()}

# ── Protect all other /api ─────────────────────────────────────────
@app.middleware("http")
async def protect(request,call_next):
    if request.url.path.startswith("/api") and request.url.path not in ("/api/login","/api/verify","/api/current_operator"):
        token=request.headers.get("X-Auth-Token")
        cur.execute("SELECT 1 FROM sessions WHERE token=%s AND logout_ts IS NULL",(token,))
        if not cur.fetchone():
            raise HTTPException(401,"Not logged in")
    return await call_next(request)
