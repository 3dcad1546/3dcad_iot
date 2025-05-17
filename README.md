# 3dcad_iot Industrial Edge Device Microservices

This repository contains a set of Python microservices containerized with Docker Compose to run on an industrial edge device.  
It implements:

- **Polling Service**: polls local REST endpoints and publishes to Kafka  
- **Trigger Service**: subscribes to MQTT topics and republishes to Kafka  
- **Processing Service**: consumes raw data, applies business logic, issues machine commands  
- **Machine Interface**: consumes commands from Kafka and sends to machines (Modbus/etc.)  
- **Dashboard API**: FastAPI backend exposing health and control endpoints  
- **Kafka + Zookeeper**, **Mosquitto MQTT**, **PostgreSQL**, **InfluxDB**

### 📂 Structure
edge-device-project/
│
├── docker-compose.yml
├── mosquitto/
│ └── mosquitto.conf
├── README.md
└── services/
├── polling_service/
│ ├── Dockerfile
│ ├── requirements.txt
│ └── main.py
├── trigger_service/
│ ├── Dockerfile
│ ├── requirements.txt
│ └── main.py
├── processing_service/
│ ├── Dockerfile
│ ├── requirements.txt
│ └── main.py
├── machine_interface/
│ ├── Dockerfile
│ ├── requirements.txt
│ └── main.py
└── dashboard_api/
├── Dockerfile
├── requirements.txt
└── main.py

### 🚀 Getting Started

1. **Install** Docker & Docker Compose on your edge device.  
2. **Clone** this repo and `cd edge-device-project`.  
3. **Build & Run** all services:
   ```bash
   docker-compose up --build```
4. **Verify:**
    * Dashboard health: http://<device-ip>:8000/api/health
    * MQTT broker on mqtt://<device-ip>:1883
    * Kafka on localhost:9092, ZK on 2181
    * PostgreSQL on 5432, InfluxDB on 8086
5. **Shut down:**
```docker-compose down
```
