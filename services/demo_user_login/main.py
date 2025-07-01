import os, uuid
import asyncio
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Header
from pydantic import BaseModel
from datetime import datetime
from fastapi.middleware.cors import CORSMiddleware

# ─── Allowed demo users ───────────────────────────────────────────────────────
OPERATOR_IDS = os.getenv("OPERATOR_IDS", "op1,op2,op3").split(",")
ADMIN_IDS    = os.getenv("ADMIN_IDS",   "admin").split(",")
ENGINEER_IDS = os.getenv("ENGINEER_IDS","eng").split(",")

# ─── In-memory session store: token → (username, role) ─────────────────────────
session_store: dict[str, dict[str,str]] = {}

# ─── FastAPI setup ───────────────────────────────────────────────────────────
app = FastAPI()
app.add_middleware(
  CORSMiddleware,
  allow_origins=["*"],
  allow_methods=["*"],
  allow_headers=["*"],
  allow_credentials=True,
)

class LoginRequest(BaseModel):
    Username: str
    Password: str

class LogoutRequest(BaseModel):
    Token: str

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
    await ws_mgr.broadcast({
        "event":    "login",
        "username": req.Username,
        "role":     role,
        "ts":       datetime.utcnow().isoformat() + "Z"
    })

    # 4) return token + info
    return {
        "message":  "Login successful",
        "token":    token,
        "username": req.Username,
        "role":     role
    }

# ─── Logout ──────────────────────────────────────────────────────────────────
@app.post("/api/logout")
async def logout(req: LogoutRequest):
    info = session_store.pop(req.Token, None)
    if not info:
        raise HTTPException(401, "Invalid or expired token")

    # broadcast
    await ws_mgr.broadcast({
        "event":    "logout",
        "username": info["username"],
        "ts":       datetime.utcnow().isoformat() + "Z"
    })

    return {"message":"Logged out"}

# ─── Verify ──────────────────────────────────────────────────────────────────
@app.get("/api/verify")
def verify(sess=Depends(require_token)):
    # simply echo back
    return {"username": sess["username"], "role": sess["role"]}

# ─── WebSocket manager ───────────────────────────────────────────────────────
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
            # we don't expect incoming messages, just keep alive
            await ws.receive_text()
    except WebSocketDisconnect:
        ws_mgr.disconnect(ws)
