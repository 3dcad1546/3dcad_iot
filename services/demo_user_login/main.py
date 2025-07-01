import os
import asyncio
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
from datetime import datetime
from fastapi.middleware.cors import CORSMiddleware
# ─── Allowed demo users (override via env if you like) ─────────────
OPERATOR_IDS = os.getenv("OPERATOR_IDS", "op1,op2,op3").split(",")
ADMIN_IDS    = os.getenv("ADMIN_IDS",   "admin").split(",")
ENGINEER_IDS = os.getenv("ENGINEER_IDS","eng").split(",")

# ─── In-memory session state ────────────────────────────────────────
_current_user = None
_current_role = None

# ─── FastAPI setup ────────────────────────────────────────────────
app = FastAPI()


app.add_middleware(
  CORSMiddleware,
  allow_origins=["*"],  # or ["*"] for a quick demo
  allow_methods=["*"],
  allow_headers=["*"],
  allow_credentials=True,
)
class LoginRequest(BaseModel):
    Username: str
    Password: str

class LogoutRequest(BaseModel):
    Username: str

def require_login():
    """Raise if nobody is logged in."""
    if _current_user is None:
        raise HTTPException(401, "Not logged in")

@app.post("/api/login")
async def login(req: LoginRequest):
    global _current_user, _current_role
    # simple demo check: user must appear in one of the lists
    if req.Username in ADMIN_IDS:
        role = "admin"
    elif req.Username in ENGINEER_IDS:
        role = "engineer"
    elif req.Username in OPERATOR_IDS:
        role = "operator"
    else:
        raise HTTPException(403, "User not permitted")

    # *in a real app you’d check req.Password!* here we accept any password
    _current_user = req.Username
    _current_role = role

    # notify any WS listeners
    await ws_mgr.broadcast({
        "event":     "login",
        "username":  _current_user,
        "role":      _current_role,
        "timestamp": datetime.utcnow().isoformat()+"Z"
    })

    return {
        "message":   "Login successful",
        "username":  _current_user,
        "role":      _current_role
    }

@app.post("/api/logout")
async def logout(req: LogoutRequest):
    global _current_user, _current_role
    if req.Username != _current_user:
        raise HTTPException(400, "Not logged in or wrong user")

    # capture for WS
    user = _current_user

    _current_user = None
    _current_role = None

    await ws_mgr.broadcast({
        "event":     "logout",
        "username":  user,
        "timestamp": datetime.utcnow().isoformat()+"Z"
    })
    return {"message": "Logged out"}

@app.get("/api/verify")
def verify():
    require_login()
    return {"username": _current_user, "role": _current_role}

# ─── WebSocket for login status updates ─────────────────────────────
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
            # keep the connection alive; we don't expect incoming messages
            await ws.receive_text()
    except WebSocketDisconnect:
        ws_mgr.disconnect(ws)
