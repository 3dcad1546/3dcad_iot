import os
import json
import time
from kafka import KafkaConsumer, KafkaProducer
from influxdb_client import InfluxDBClient, Point, WritePrecision

# at module level, after imports:
INFLUX_URL   = os.getenv("INFLUXDB_URL")
INFLUX_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUX_ORG   = os.getenv("INFLUXDB_ORG")
INFLUX_BUCKET= os.getenv("INFLUXDB_BUCKET")

influx = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
writer = influx.write_api()

BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
SENSOR_TOPIC = "raw_server_data"
TRIGGER_TOPIC = "trigger_events"
CMD_TOPIC = "machine_commands"
THRESHOLD = float(os.getenv("VALUE_THRESHOLD", "50.0"))

consumer = KafkaConsumer(
    SENSOR_TOPIC, TRIGGER_TOPIC,
    bootstrap_servers=BROKER,
    value_deserializer=lambda m: json.loads(m.decode()),
    auto_offset_reset='latest',
    group_id='edge_proc_group'
)
producer = KafkaProducer(
    bootstrap_servers=BROKER,
    value_serializer=lambda v: json.dumps(v).encode()
)

for msg in consumer:
    topic = msg.topic
    data = msg.value
    point = (
        Point("sensor_data")
        .tag("source", data["source"])
        .field("value", data["data"].get("value", 0))
        .time(time.time(), WritePrecision.S)
        )
    writer.write(INFLUX_BUCKET, INFLUX_ORG, point)
    if topic == SENSOR_TOPIC:
        val = data.get("data", {}).get("value", 0)
        if val > THRESHOLD:
            cmd = {"cmd": "STOP_MACHINE", "reason": f"value {val}>{THRESHOLD}"}
        else:
            cmd = {"cmd": "KEEP_RUNNING"}
        print(f"[Processing] {topic}: {val} -> {cmd}")
        producer.send(CMD_TOPIC, cmd)
        
    else:
        # TRIGGER_TOPIC
        print(f"[Processing] trigger {data}")
        cmd = {"cmd": "TRIGGERED_ACTION", "info": data}
        producer.send(CMD_TOPIC, cmd)