import os, uuid, asyncio, json, time as pytime, requests
from datetime import datetime, timedelta, time as dtime
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Header, status,APIRouter, Depends
from pydantic import BaseModel, validator, constr, Field
import psycopg2
from aiokafka import AIOKafkaProducer
from pymodbus.client.tcp import AsyncModbusTcpClient
from passlib.hash import bcrypt
from typing import List
from fastapi.middleware.cors import CORSMiddleware


# Router for CRUD operations
router = APIRouter(prefix="/api", tags=["CRUD"])

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
# ─── Pydantic Schemas ─────────────────────────────────────────────────────────────
class MachineConfig(BaseModel):
    machine_id: str
    mes_process_control_url: str
    mes_upload_url: str
    is_mes_enabled: bool = True
    trace_process_control_url: str
    trace_interlock_url: str
    is_trace_enabled: bool = True

class UserCreate(BaseModel):
    first_name: str
    last_name: str
    username: str = Field(...,pattern=r"^[a-zA-Z0-9_]+$")
    password: str
    role: str = Field(...,pattern=r"^(operator|admin|engineer)$")

class UserOut(BaseModel):
    id: uuid.UUID
    first_name: str
    last_name: str
    username: str
    role: str

class AccessEntry(BaseModel):
    role: str = Field(...,pattern=r"^(operator|admin|engineer)$")
    page_name: str
    can_read: bool = True
    can_write: bool = False

class MessageEntry(BaseModel):
    code: str
    message: str

class PLCTest(BaseModel):
    param1: bool
    param2: bool
    param3: bool
    param4: bool
    param5: bool
    param6: bool

class ShiftIn(BaseModel):
     name: str
     start_time: str = Field(..., pattern=r'^\d{2}:\d{2}:\d{2}$')  # "HH:MM:SS"
     end_time:   str = Field(..., pattern=r'^\d{2}:\d{2}:\d{2}$')

class ShiftOut(ShiftIn):
    id: int


# ─── CRUD Endpoints ──────────────────────────────────────────────────────────────
# 1) Machine Config
@router.post("/machine-config", response_model=MachineConfig)
def create_machine(cfg: MachineConfig):
    cur.execute(
        "INSERT INTO machine_config (machine_id, mes_process_control_url, mes_upload_url, is_mes_enabled, trace_process_control_url, trace_interlock_url, is_trace_enabled) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING machine_id,mes_process_control_url,mes_upload_url,is_mes_enabled,trace_process_control_url,trace_interlock_url,is_trace_enabled",
        (cfg.machine_id, cfg.mes_process_control_url, cfg.mes_upload_url, cfg.is_mes_enabled, cfg.trace_process_control_url, cfg.trace_interlock_url, cfg.is_trace_enabled)
    )
    row = cur.fetchone()
    return MachineConfig(**dict(zip([c.name for c in cur.description], row)))

@router.get("/machine-config/{machine_id}", response_model=MachineConfig)
def read_machine(machine_id: str):
    cur.execute("SELECT machine_id,mes_process_control_url,mes_upload_url,is_mes_enabled,trace_process_control_url,trace_interlock_url,is_trace_enabled FROM machine_config WHERE machine_id=%s", (machine_id,))
    row = cur.fetchone()
    if not row:
        raise HTTPException(404, "Machine not found")
    return MachineConfig(**dict(zip([c.name for c in cur.description], row)))

@router.put("/machine-config/{machine_id}", response_model=MachineConfig)
def update_machine(machine_id: str, cfg: MachineConfig):
    cur.execute(
        "UPDATE machine_config SET mes_process_control_url=%s,mes_upload_url=%s,is_mes_enabled=%s,trace_process_control_url=%s,trace_interlock_url=%s,is_trace_enabled=%s WHERE machine_id=%s RETURNING machine_id,mes_process_control_url,mes_upload_url,is_mes_enabled,trace_process_control_url,trace_interlock_url,is_trace_enabled",
        (cfg.mes_process_control_url, cfg.mes_upload_url, cfg.is_mes_enabled, cfg.trace_process_control_url, cfg.trace_interlock_url, cfg.is_trace_enabled, machine_id)
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(404, "Machine not found")
    return MachineConfig(**dict(zip([c.name for c in cur.description], row)))

@router.delete("/machine-config/{machine_id}")
def delete_machine(machine_id: str):
    cur.execute("DELETE FROM machine_config WHERE machine_id=%s RETURNING 1", (machine_id,))
    if not cur.fetchone():
        raise HTTPException(404, "Machine not found")
    return {"ok": True}

# 2) User creation + management
@router.post("/users", response_model=UserOut)
def create_user(u: UserCreate):
    hashed = bcrypt.hash(u.password)
    user_id = uuid.uuid4()
    cur.execute(
        "INSERT INTO users (id, first_name, last_name, username, password_hash, role) VALUES (%s,%s,%s,%s,%s,%s) RETURNING id, first_name, last_name, username, role",
        (str(user_id), u.first_name, u.last_name, u.username, hashed, u.role)
    )
    row = cur.fetchone()
    return UserOut(**dict(zip([c.name for c in cur.description], row)))

@router.get("/users/{username}", response_model=UserOut)
def read_user(username: str):
    cur.execute("SELECT id,first_name,last_name,username,role FROM users WHERE username=%s", (username,))
    row = cur.fetchone()
    if not row:
        raise HTTPException(404, "User not found")
    return UserOut(**dict(zip([c.name for c in cur.description], row)))

@router.put("/users/{username}", response_model=UserOut)
def update_user(username: str, u: UserCreate):
    hashed = bcrypt.hash(u.password)
    cur.execute(
        "UPDATE users SET first_name=%s,last_name=%s,password_hash=%s,role=%s WHERE username=%s RETURNING id, first_name, last_name, username, role",
        (u.first_name, u.last_name, hashed, u.role, username)
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(404, "User not found")
    return UserOut(**dict(zip([c.name for c in cur.description], row)))

@router.delete("/users/{username}")
def delete_user(username: str):
    cur.execute("DELETE FROM users WHERE username=%s RETURNING 1", (username,))
    if not cur.fetchone():
        raise HTTPException(404, "User not found")
    return {"ok": True}


# added a get api to fetch the predefined roles.
@router.get("/roles", response_model=List[str])
def get_user_roles():
    try:
        
        # Query enum values from PostgreSQL
        cur.execute("SELECT unnest(enum_range(NULL::user_role)) AS role;")
        roles = [row[0] for row in cur.fetchall()]

        return roles

    except Exception as e:
        print("Error fetching roles:", e)
        raise HTTPException(status_code=500, detail="Internal Server Error")


# 3) User access
@router.post("/access", response_model=AccessEntry)
def create_access(a: AccessEntry):
    cur.execute(
        "INSERT INTO user_access (role,page_name,can_read,can_write) VALUES(%s,%s,%s,%s) RETURNING role,page_name,can_read,can_write",
        (a.role, a.page_name, a.can_read, a.can_write)
    )
    row = cur.fetchone()
    return AccessEntry(**dict(zip([c.name for c in cur.description], row)))

@router.get("/access", response_model=list[AccessEntry])
def list_access():
    cur.execute("SELECT role,page_name,can_read,can_write FROM user_access")
    return [AccessEntry(**dict(zip([c.name for c in cur.description], row))) for row in cur.fetchall()]

@router.put("/access/{role}/{page_name}", response_model=AccessEntry)
def update_access(role: str, page_name: str, a: AccessEntry):
    cur.execute(
        "UPDATE user_access SET can_read=%s,can_write=%s WHERE role=%s AND page_name=%s RETURNING role,page_name,can_read,can_write",
        (a.can_read, a.can_write, role, page_name)
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(404, "Access entry not found")
    return AccessEntry(**dict(zip([c.name for c in cur.description], row)))

@router.delete("/access/{role}/{page_name}")
def delete_access(role: str, page_name: str):
    cur.execute("DELETE FROM user_access WHERE role=%s AND page_name=%s RETURNING 1", (role, page_name))
    if not cur.fetchone():
        raise HTTPException(404, "Access entry not found")
    return {"ok": True}

# 4) Message master
@router.post("/messages", response_model=MessageEntry)
def create_message(m: MessageEntry):
    cur.execute("INSERT INTO message_master(code,message) VALUES(%s,%s) RETURNING code,message", (m.code, m.message))
    row = cur.fetchone()
    return MessageEntry(**dict(zip([c.name for c in cur.description], row)))

@router.get("/messages/{code}", response_model=MessageEntry)
def read_message(code: str):
    cur.execute("SELECT code,message FROM message_master WHERE code=%s", (code,))
    row = cur.fetchone()
    if not row:
        raise HTTPException(404, "Message not found")
    return MessageEntry(**dict(zip([c.name for c in cur.description], row)))

@router.put("/messages/{code}", response_model=MessageEntry)
def update_message(code: str, m: MessageEntry):
    cur.execute(
        "UPDATE message_master SET message=%s WHERE code=%s RETURNING code,message",
        (m.message, code)
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(404, "Message not found")
    return MessageEntry(**dict(zip([c.name for c in cur.description], row)))

@router.delete("/messages/{code}")
def delete_message(code: str):
    cur.execute("DELETE FROM message_master WHERE code=%s RETURNING 1", (code,))
    if not cur.fetchone():
        raise HTTPException(404, "Message not found")
    return {"ok": True}

# 5) PLC Test parameters
@router.post("/plc-tests", status_code=201)
def create_plc_test(p: PLCTest):
    cur.execute(
        "INSERT INTO plc_test(name,param1,param2,param3,param4,param5,param6) VALUES(%s,%s,%s,%s,%s,%s) RETURNING id,param1,param2,param3,param4,param5,param6",
        (p.param1,p.param2,p.param3,p.param4,p.param5,p.param6)
    )
    row = cur.fetchone()
    return dict(zip([c.name for c in cur.description], row))

@router.get("/plc-tests", response_model=list[PLCTest])
def list_plc_tests():
    cur.execute("SELECT name,param1,param2,param3,param4,param5,param6 FROM plc_test")
    return [PLCTest(**dict(zip([c.name for c in cur.description], row))) for row in cur.fetchall()]

@router.get("/plc-tests/{test_id}")
def read_plc_test(test_id: int):
    cur.execute("SELECT name,param1,param2,param3,param4,param5,param6 FROM plc_test WHERE id=%s", (test_id,))
    row = cur.fetchone()

# 6) Shifts CRUD
@router.post("/shifts", response_model=ShiftOut, status_code=201)
def create_shift(s: ShiftIn):
    cur.execute(
        """
        INSERT INTO shift_master(name,start_time,end_time)
        VALUES (%s, %s::time, %s::time)
        RETURNING id,name,start_time::text AS start_time,end_time::text AS end_time
        """,
        (s.name, s.start_time, s.end_time)
    )
    row = cur.fetchone()
    return ShiftOut(**dict(zip([c.name for c in cur.description], row)))

@router.get("/shifts", response_model=List[ShiftOut])
def list_shifts():
    cur.execute("""
      SELECT id,name,
             start_time::text AS start_time,
             end_time::text   AS end_time
        FROM shift_master
        ORDER BY id
    """)
    return [
        ShiftOut(**dict(zip([c.name for c in cur.description], row)))
        for row in cur.fetchall()
    ]

@router.get("/shifts/{shift_id}", response_model=ShiftOut)
def read_shift(shift_id: int):
    cur.execute("""
      SELECT id,name,
             start_time::text AS start_time,
             end_time::text   AS end_time
        FROM shift_master
       WHERE id = %s
    """, (shift_id,))
    row = cur.fetchone()
    if not row:
        raise HTTPException(404, "Shift not found")
    return ShiftOut(**dict(zip([c.name for c in cur.description], row)))

@router.put("/shifts/{shift_id}", response_model=ShiftOut)
def update_shift(shift_id: int, s: ShiftIn):
    cur.execute("""
      UPDATE shift_master
         SET name = %s,
             start_time = %s::time,
             end_time   = %s::time
       WHERE id = %s
    RETURNING id,name,start_time::text AS start_time,end_time::text AS end_time
    """, (s.name, s.start_time, s.end_time, shift_id))
    row = cur.fetchone()
    if not row:
        raise HTTPException(404, "Shift not found")
    return ShiftOut(**dict(zip([c.name for c in cur.description], row)))

@router.delete("/shifts/{shift_id}", status_code=204)
def delete_shift(shift_id: int):
    cur.execute("DELETE FROM shift_master WHERE id = %s RETURNING 1", (shift_id,))
    if not cur.fetchone():
        raise HTTPException(404, "Shift not found")
    return

# ─── FastAPI + WS ────────────────────────────────────────────
app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

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
# @app.middleware("http")
# async def protect(request,call_next):
#     if request.url.path.startswith("/api") and request.url.path not in ("/api/login","/api/verify","/api/current_operator"):
#         token=request.headers.get("X-Auth-Token")
#         cur.execute("SELECT 1 FROM sessions WHERE token=%s AND logout_ts IS NULL",(token,))
#         if not cur.fetchone():
#             raise HTTPException(401,"Not logged in")
#     return await call_next(request)

app.include_router(router)