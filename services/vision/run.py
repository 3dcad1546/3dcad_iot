from fastapi import FastAPI
from datetime import datetime
from pprint import pprint
from typing import Dict
app = FastAPI(title="Test Webhook Service", version="1.0.0")

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