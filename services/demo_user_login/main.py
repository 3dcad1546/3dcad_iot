import os
import uuid
import asyncio
import json
from datetime import datetime, time as dtime, timedelta

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, validator

import psycopg2
from aiokafka import AIOKafkaProducer

# ─── Configuration ─────────────────────────────────────────────────────
DB_URL      = os.getenv("DB_URL",      "postgresql://edge:edgepass@postgres:5432/edgedb")
KAFKA_BOOT  = os.getenv("KAFKA_BROKER", "kafka:9092")
LOGIN_TOPIC = os.getenv("LOGIN_TOPIC", "login_status")

# ─── Postgres Setup ────────────────────────────────────────────────────
conn = psycopg2.connect(DB_URL)
conn.autocommit = True
cur  = conn.cursor()

# ─── Kafka Producer (fire‐and‐forget) ──────────────────────────────────
producer = None
async def init_producer():
    global producer
    p = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOT,
        value_serializer=lambda v: v.encode()
    )
    await p.start()
    producer = p

# ─── FastAPI & CORS ────────────────────────────────────────────────────
app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ─── Models ────────────────────────────────────────────────────────────
class LoginReq(BaseModel):
    Username: str
    Password: str
    Role: str

    @validator('Role')
    def role_must_be_valid(cls, v):
        if v not in ('operator','admin','engineer'):
            raise ValueError('Role must be one of operator, admin, engineer')
        return v

<<<<<<< HEAD
def require_token(token: str = Header(None, alias="X-Auth-Token")):
    if not token or token not in session_store:
        raise HTTPException(401, "Invalid or missing auth token")
    return session_store[token]  # returns {"username":..., "role":...}

# ─── Login ───────────────────────────────────────────────────────────────────
@app.post("/api/login")
async def login(req: LoginRequest):
    # 1) determine role
    if req.Username in ADMIN_IDS:
        role = "admin"
    elif req.Username in ENGINEER_IDS:
        role = "engineer"
    elif req.Username in OPERATOR_IDS:
        role = "operator"
    else:
        raise HTTPException(403, "User not permitted")

    # 2) (demo) accept any password, issue token
    token = str(uuid.uuid4())
    session_store[token] = {
        "username": req.Username,
        "role":     role
    }

    # 3) broadcast to any WS listeners
    try:
        await ws_mgr.broadcast({
        "event":    "login",
        "username": req.Username,
        "role":     role,
        "ts":       datetime.utcnow().isoformat() + "Z"
        })
    except Exception as e:
        print("WebSocket broadcast error:", e)


    # 4) return token + info
    return {
        
        "token":    token,
        "username": req.Username,
        "role":     role,
        "shift":    get_current_shift()
    }

# ─── Logout ──────────────────────────────────────────────────────────────────
@app.post("/api/logout")
async def logout(req: LogoutRequest):
    info = session_store.pop(req.Token, None)
    if not info:
        raise HTTPException(401, "Invalid or expired token")

    # broadcast
    try:
        await ws_mgr.broadcast({
                "event":    "logout",
                "username": info["username"],
                "ts":       datetime.utcnow().isoformat() + "Z"
            })
    except Exception as e:
        print("WebSocket broadcast error:", e)


    return {"message":"Logged out"}

# ─── Verify ──────────────────────────────────────────────────────────────────
@app.get("/api/verify")
def verify(sess=Depends(require_token)):
    # simply echo back
    return {"username": sess["username"], "role": sess["role"]}

# ─── WebSocket manager ───────────────────────────────────────────────────────
=======
# ─── WebSocket Connection Manager ───────────────────────────────────────
>>>>>>> c135ddc98651c7cc1fdb70e57fc108783fb7aa14
class ConnectionManager:
    def __init__(self):
        self.conns: set[WebSocket] = set()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.conns.add(ws)

    def disconnect(self, ws: WebSocket):
        self.conns.discard(ws)

    async def broadcast(self, msg: dict):
        data = json.dumps(msg)
        living = set()
        for ws in self.conns:
            try:
                await ws.send_text(data)
                living.add(ws)
            except:
                pass
        self.conns = living

ws_mgr = ConnectionManager()

# ─── Startup: create tables & producer ─────────────────────────────────
@app.on_event("startup")
async def startup():
    await init_producer()
    cur.execute("""
      CREATE TABLE IF NOT EXISTS shift_master (
        id SERIAL PRIMARY KEY,
        name TEXT UNIQUE,
        start_time TIME,
        end_time TIME
      );
      CREATE TABLE IF NOT EXISTS sessions (
        token UUID PRIMARY KEY,
        username TEXT,
        role TEXT,
        shift_name TEXT,
        login_ts TIMESTAMP DEFAULT NOW(),
        logout_ts TIMESTAMP
      );
    """)

# ─── Helper: find current shift window ─────────────────────────────────
def get_current_shift():
    now = datetime.now().time()
    cur.execute("""
      SELECT name, start_time::text, end_time::text
      FROM shift_master
      WHERE
        (start_time < end_time AND %s BETWEEN start_time AND end_time)
     OR (start_time > end_time AND (%s >= start_time OR %s <= end_time))
    """, (now, now, now))
    row = cur.fetchone()
    if row:
        name, st, et = row
        return {"name": name, "start": st[:5], "end": et[:5]}
    return None

# ─── Auto‐logout scheduler ──────────────────────────────────────────────
async def schedule_auto_logout(shift: dict, token: str):
    # Sleep until shift end (wrap-around)
    now = datetime.now()
    end_t = datetime.combine(now.date(), dtime.fromisoformat(shift["end"]))
    if end_t <= now:
        end_t += timedelta(days=1)
    await asyncio.sleep((end_t - now).total_seconds())

    # 1) mark session closed
    cur.execute("UPDATE sessions SET logout_ts=NOW() WHERE token=%s", (token,))
    # 2) WS update
    await ws_mgr.broadcast({
      "event": "auto-logout",
      "shift": shift["name"],
      "ts":    datetime.utcnow().isoformat() + "Z"
    })
    # 3) Kafka → PLC register 3309 = 0
    await producer.send_and_wait(LOGIN_TOPIC, json.dumps({
      "status": 0,
      "ts":     datetime.utcnow().isoformat() + "Z"
    }))

# ─── Login endpoint ────────────────────────────────────────────────────
@app.post("/api/login")
async def login(req: LoginReq):
    # 1) make sure user is allowed in that role
    if req.Role == "operator":
        if req.Username not in OPERATOR_IDS:
            raise HTTPException(403, "Operator not permitted")
    elif req.Role == "admin":
        if req.Username not in ADMIN_IDS:
            raise HTTPException(403, "Admin not permitted")
    else:  # engineer
        if req.Username not in ENGINEER_IDS:
            raise HTTPException(403, "Engineer not permitted")

    # 2) if operator, forward to MES-login; else local DB check
    if req.Role == "operator":
        cur.execute("SELECT value FROM config WHERE key='MES_OPERATOR_LOGIN_URL'")
        mes_login_url = cur.fetchone()[0]
        resp = requests.post(
            mes_login_url,
            json={"Username": req.Username, "Password": req.Password},
            timeout=5
        )
        if resp.status_code != 200 or not resp.json().get("IsSuccessful"):
            raise HTTPException(403, "MES login failed")
    else:
        # e.g. check against your users table / hashed passwords
        cur.execute("SELECT password_hash FROM users WHERE username=%s", (req.Username,))
        row = cur.fetchone()
        if not row or not verify_password(req.Password, row[0]):
            raise HTTPException(401, "Invalid credentials")

    # 3) fetch current shift
    sid, shift_name, st, et = current_shift()
    if not sid:
        raise HTTPException(400, "No active shift")

    # 4) persist session
    token = str(uuid.uuid4())
    cur.execute("""
      INSERT INTO sessions(token,username,role,shift_id)
      VALUES(%s,%s,%s,%s)
    """, (token, req.Username, req.Role, sid))
    if req.Role == "operator":
        cur.execute("""
          INSERT INTO operator_sessions(username,shift_id)
          VALUES(%s,%s)
        """, (req.Username, sid))

    # 5) tell PLC “login bit = 1”
    req_id = str(uuid.uuid4())
    await producer.send_and_wait("plc_write_commands", {
      "section":"login", "tag_name":"login", "value":1, "request_id":req_id
    })

    # 6) broadcast over WS
    await ws_mgr.broadcast({
      "event":"login",
      "username":   req.Username,
      "role":       req.Role,
      "shift":      shift_name,
      "shift_start":st.isoformat(),
      "shift_end":  et.isoformat(),
      "ts":         datetime.utcnow().isoformat()+"Z"
    })

    # 7) return the token + session info
    return {
      "token":       token,
      "username":    req.Username,
      "role":        req.Role,
      "shift_id":    sid,
      "shift_name":  shift_name,
      "shift_start": st.isoformat(),
      "shift_end":   et.isoformat()
    }


# ─── Manual logout endpoint ────────────────────────────────────────────
@app.post("/api/logout")
async def logout(token: str):
    # mark session closed
    cur.execute("UPDATE sessions SET logout_ts=NOW() WHERE token=%s", (token,))
    # WS notify
    await ws_mgr.broadcast({"event":"manual-logout","ts":datetime.utcnow().isoformat()+"Z"})
    # Kafka → PLC register 3309 = 2
    await producer.send_and_wait(LOGIN_TOPIC, json.dumps({
      "status": 2,
      "ts":     datetime.utcnow().isoformat() + "Z"
    }))
    return {"message": "Logged out"}

# ─── WS for real‐time login/auto/manual status ─────────────────────────
@app.websocket("/ws/login-status")
async def ws_login(ws: WebSocket):
    await ws_mgr.connect(ws)
    try:
        while True:
            await ws.receive_text()   # keep connection alive
    except WebSocketDisconnect:
        ws_mgr.disconnect(ws)
