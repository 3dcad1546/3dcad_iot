import os
import json
import requests
import time
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
import psycopg2

# ─── Configuration ─────────────────────────────────────────────
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
RAW_TOPIC = os.getenv("RAW_KAFKA_TOPIC", "raw_server_data")
COMMAND_TOPIC = os.getenv("COMMAND_KAFKA_TOPIC", "machine_commands")
MES_UPLOAD_URL = os.getenv("MES_UPLOAD_URL", "")
TRACE_PROXY_HOST = os.getenv("TRACE_PROXY_HOST", "localhost:8765")
MACHINE_ID = os.getenv("MACHINE_ID", "Machine01")
OPERATOR_ID = os.getenv("OPERATOR_ID", "OperatorX")
CBS_STREAM = os.getenv("CBS_STREAM_NAME", "Ignition")
FAILURE_REASONS = os.getenv("FAILURE_REASON_CODES", "REJECT").split(",")
NCM_REASON_CODES = os.getenv("NCM_REASON_CODES", "DEFECT").split(",")

# ─── Kafka Setup ───────────────────────────────────────────────
def get_consumer():
    for attempt in range(10):
        try:
            consumer = KafkaConsumer(
                RAW_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset='latest',
                enable_auto_commit=True,
                group_id="processor-service",
                consumer_timeout_ms=10000,  # Stop polling after 10s of inactivity
                max_poll_records=10
            )
            return consumer
        except Exception as e:
            logging.error(f"[KafkaConsumer] Connection failed (attempt {attempt+1}/10): {e}")
            time.sleep(5)
    raise RuntimeError("KafkaConsumer: Failed to connect after retries")

consumer = get_consumer()

def get_producer():
    for attempt in range(10):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=5,
                linger_ms=10,
                request_timeout_ms=10000,
                max_block_ms=10000
            )
            return producer
        except KafkaError as e:
            print(f"[KafkaProducer] Connection failed (attempt {attempt+1}/10): {e}")
            time.sleep(5)
    raise RuntimeError("KafkaProducer: Failed to connect after retries")

producer = get_producer()

# ─── PostgreSQL Setup ──────────────────────────────────────────
conn = psycopg2.connect(os.getenv("DB_URL"))
conn.autocommit = True
cur = conn.cursor()

def trace_process_control(serial):
    params = {"serial": serial, "serial_type": "band"}
    resp = requests.get(f"http://{TRACE_PROXY_HOST}/v2/process_control", params=params)
    resp.raise_for_status()
    return resp.json()

def trace_interlock(serial):
    params = {"serial": serial, "serial_type": "band"}
    resp = requests.post(f"http://{TRACE_PROXY_HOST}/interlock", params=params)
    resp.raise_for_status()
    return resp.json()

def upload_to_mes(serial, result):
    payload = {
        "UniqueIds": [serial],
        "MachineIds": [MACHINE_ID],
        "OperatorIds": [OPERATOR_ID],
        "Tools": [],
        "RawMaterials": [],
        "DateTimeStamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "CbsStreamName": CBS_STREAM
    }

    if result == "pass":
        payload.update({"IsPass": True, "IsFail": False, "IsScrap": False, "Defects": []})
    elif result == "fail":
        payload.update({
            "IsPass": False, "IsFail": True, "IsScrap": False,
            "FailureDefects": [{
                "FailureReasonCodes": FAILURE_REASONS,
                "NcmReasonCodes": NCM_REASON_CODES
            }]
        })
    elif result == "scrap":
        payload.update({
            "IsPass": False, "IsFail": False, "IsScrap": True,
            "Defects": [{"DefectCode": "Trace Scrap", "Quantity": 1}]
        })
    else:
        raise ValueError("Invalid result type")

    mes = requests.post(MES_UPLOAD_URL, json=payload)
    mes.raise_for_status()
    return mes.json()

def process_data(data):
    serial = data.get("serial")
    result = data.get("result", "pass")
    print(f"[PROCESS] Serial: {serial} - Result: {result}")

    try:
        # Step 1: Trace Process Control
        tp = trace_process_control(serial)
        if not tp.get("pass"):
            raise Exception("Trace PC Failed")

        # Step 2: Trace Interlock
        inter = trace_interlock(serial)
        if not inter.get("pass"):
            raise Exception("Trace Interlock Failed")

        # Step 3: MES Upload
        mes = upload_to_mes(serial, result)

        # Step 4: Log to PostgreSQL
        cur.execute(
            "INSERT INTO mes_trace_history(serial, step, response_json, ts) VALUES (%s, %s, %s::jsonb, NOW())",
            (serial, "auto_process", json.dumps(mes))
        )

        # Step 5: Trigger Command (optional)
        producer.send(COMMAND_TOPIC, {
            "serial": serial,
            "command": "update_led",
            "status": result
        })

    except Exception as e:
        print(f"[ERROR] Processing {serial}: {e}")
        cur.execute(
            "INSERT INTO error_logs(context, error_msg, details, ts) VALUES (%s, %s, %s::jsonb, NOW())",
            ("processor", str(e), json.dumps(data))
        )

if __name__ == "__main__":
    print("[Processing Service] Started")
    for message in consumer:
        data = message.value
        process_data(data)
