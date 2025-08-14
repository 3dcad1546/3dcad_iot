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

```
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
```

### 🚀 Getting Started

1. **Install** Docker & Docker Compose on your edge device.  
2. **Clone** this repo and `cd edge-device-project`.  
3. **Build & Run** all services:
   `docker-compose up --build`
4. **Verify:**
    * Dashboard health: `http://<device-ip>:8000/api/health`
    * MQTT broker on `mqtt://<device-ip>:1883`
    * Kafka on `localhost:9092`, ZK on `2181`
    * PostgreSQL on `5432`, InfluxDB on `8086`
5. **Shut down:**
    `docker-compose down`

### **Ops Wrapper**
* It **waits for Postgres and Kafka health** (you have healthchecks defined) before declaring “up”.

* ```seed-db``` re-applies ```init.sql``` inside the running edge_pg container and, if found, also applies ```services/postgres/seed_data.sql``` (you can drop that file in your repo).

* backup writes to a ```backups/``` folder next to the script: ```edgedb_YYYYMMDD-HHMM.sql``` and a zipped Influx backup if the CLI is available in the container.

* Autostart uses a Scheduled Task that runs: ```pwsh.exe -File <path>\edgectl.ps1 up ``` at boot with highest privileges.

* You can also use the ```.bat``` wrapper to avoid execution policy prompts (double-click or call from ```cmd.exe```).
