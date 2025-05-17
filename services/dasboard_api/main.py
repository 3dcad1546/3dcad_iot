from fastapi import FastAPI
import os
from influxdb_client import InfluxDBClient
import psycopg2

app = FastAPI()

# Health check
@app.get("/api/health")
def health():
    return {"status": "ok"}

# TODO: Add real endpoints to query PostgreSQL & InfluxDB
# e.g. app.get("/api/data") -> query influxdb

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
