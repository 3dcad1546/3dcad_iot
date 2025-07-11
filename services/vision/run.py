from fastapi import FastAPI
from datetime import datetime
from pprint import pprint
from typing import Dict
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Test Webhook Service", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)
@app.post("/analytics/receive")
async def test_webhook_endpoint(payload: Dict):
    pprint(payload)
    return {
        "message": "Test webhook received",
        "processed_at": datetime.now()
    }
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=3010)