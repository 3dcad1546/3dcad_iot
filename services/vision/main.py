import base64
import math
import os
import random
import string
import csv
from datetime import datetime
from io import StringIO

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def generate_object_id():
    return "".join(random.choices(string.hexdigits.lower(), k=24))

# pre-defined polygon templates
polygons = [
    # ... your existing polygons array ...
]

def generate_layer_output(camera_index, layer_index):
    return {
        "index": layer_index,
        "name": f"L-{layer_index}",
        "score_threshold": 99.0,
        "result_label": random.choice(["good", "bad"]),
        "score": round(random.uniform(0.0, 2.0), 2),
        "shape": {
            "type": "polygon",
            "points": polygons[camera_index][layer_index],
        }
    }

def generate_metadata():
    return f"META{random.randint(10000, 99999)}"

def get_image_base64(image_filename):
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        image_path = os.path.join(script_dir, image_filename)
        if os.path.exists(image_path):
            with open(image_path, "rb") as f:
                return base64.b64encode(f.read()).decode("utf-8")
    except:
        pass
    # fallback 1×1 transparent PNG
    return "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg=="

def generate_model_wise_record(camera_index):
    layers = [generate_layer_output(camera_index, i)
              for i in range(len(polygons[camera_index]))]
    image_filename = f"image_{camera_index+1}.png"
    return {
        "output": {"layers": layers},
        "result_label": random.choice(["good", "bad"]),
        "model_name": "M-1",
        "result_image": get_image_base64(image_filename),
    }

def generate_face_record(camera_index):
    return {
        "camera_index": camera_index,
        "result_label": random.choice(["good", "bad"]),
        "models": [generate_model_wise_record(camera_index)],
        "face_name": f"F{camera_index}",
    }

def generate_dummy_record(metadata=None, vs_name=None):
    if metadata is None:
        metadata = generate_metadata()
    faces = [generate_face_record(i) for i in range(5)]
    overall = "bad" if any(f["result_label"]=="bad" for f in faces) else "good"
    return {
        "sku_name": "SKU1",
        "vision_system_name": vs_name or generate_object_id(),
        "timestamp": datetime.utcnow().isoformat() + "000",
        "result_label": overall,
        "metadata": metadata,
        "faces": faces,
    }

@app.get("/edge/api/v1/analytics/record")
async def get_single_record(metadata: Optional[str] = Query(None)):
    return generate_dummy_record(metadata)

@app.get("/edge/api/v1/analytics/image")
async def get_image_only(camera_index: int = Query(0, ge=0, le=4)):
    return {
        "camera_index": camera_index,
        "image_base64": get_image_base64(f"image_{camera_index+1}.png")
    }

@app.get("/edge/api/v1/analytics")
async def get_dummy_data(metadata: Optional[str] = Query(None)):
    return [generate_dummy_record(metadata) for _ in range(2)]

@app.get("/edge/api/v1/analytics/batch")
async def get_batch_dummy_data(count: int = Query(5, ge=1, le=100)):
    records = [generate_dummy_record() for _ in range(count)]
    return {"records": records, "count": len(records)}

@app.get("/edge/api/v1/analytics/vs")
async def get_vs_batch(count: int = Query(5, ge=1, le=100),
                       vs_count: int = Query(4, ge=1, le=10)):
    batches = []
    for _ in range(count):
        meta = generate_metadata()
        vs_list = [generate_dummy_record(meta, vs_name=f"VS-{i+1}") for i in range(vs_count)]
        batches.append({"vs": vs_list})
    return {"records": batches, "count": len(batches)}

@app.get("/")
async def home():
    return {"message": "Analytics API", "endpoints": {
        "/edge/api/v1/analytics": "GET list",
        "/edge/api/v1/analytics/record": "GET single",
        "/edge/api/v1/analytics/batch": "GET batch",
        "/edge/api/v1/analytics/vs": "GET multi-VS",
    }}

@app.websocket("/ws/analytics")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            data = await ws.receive_json()
            t = data.get("type")
            d = data.get("data", {})
            if t=="get_single_record":
                resp = generate_dummy_record(d.get("metadata"))
                await ws.send_json({"type":"single_record","data":resp})
            elif t=="get_batch_records":
                n = min(max(int(d.get("count",5)),1),100)
                resp = [generate_dummy_record() for _ in range(n)]
                await ws.send_json({"type":"batch_records","data":resp})
            elif t=="get_vs_records":
                n = min(max(int(d.get("count",5)),1),100)
                v = min(max(int(d.get("vs_count",4)),1),10)
                recs = []
                for _ in range(n):
                    m = generate_metadata()
                    recs.append([generate_dummy_record(m, vs_name=f"VS-{i+1}") for i in range(v)])
                await ws.send_json({"type":"vs_records","data":recs})
            else:
                await ws.send_json({"error":"unknown type"})
    except WebSocketDisconnect:
        pass

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=5000)
