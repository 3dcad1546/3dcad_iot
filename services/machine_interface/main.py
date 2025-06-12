import os
import json
import time
import asyncio
from pymodbus.client import AsyncModbusTcpClient
from pymodbus.client import ModbusTcpClient
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ─── kafka init─────────────────────────────────────────────────────



# ─── Configuration ─────────────────────────────────────────────────────
PLC_HOST = os.getenv("MODBUS_HOST", "localhost")
PLC_PORT = int(os.getenv("MODBUS_PORT", "502"))
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC_BARCODE = os.getenv("BARCODE_TOPIC", "trigger_events")
KAFKA_TOPIC_STATUS = os.getenv("MACHINE_STATUS_TOPIC", "machine_status")

# Barcode related registers
BARCODE_FLAG_1 = 3303
BARCODE_FLAG_2 = 3304
BARCODE_1_BLOCK = (3100, 16)   # 16 words = 32 chars
BARCODE_2_BLOCK = (3132, 16)

# 13-bit status array starts from register 3400
STATUS_REGISTER = 3400
STATUS_BITS = [
    "Input Station", "Trace", "Process", "MES", "Transfer-1", "Vision-1",
    "PickPlace-1", "Transfer-2", "Vision-2", "PickPlace-2", "Trace Upload",
    "MES Upload", "Unload Station"
]

TAG_MAP = {
    "startup": {
        "read": {
            "INPUT_CONVEYOR_MODE_POS": 1000,
            "CONTROLLER_STATUS": 1001,
            "PROGRAM_STATUS": 1002,
            "ACTUAL_POS": 1003,
            "ACTUAL_SPEED": 1004,
            "JOG_SPEED": 1005,
        },
        "write": {
            "HOME": 2000,
            "INITIALIZE": 2001,
            "OP_ENABLE": 2002,
            "START": 2003,
            "STOP": 2004
        }
    },
    "manual": {
        "read": {
            "INPUT_CONVEYOR_MODE_POS": 1100,
            "CONTROLLER_STATUS": 1101,
            "PROGRAM_STATUS": 1102,
            "ACTUAL_POS": 1103,
            "ACTUAL_SPEED": 1104,
            "JOG_SPEED": 1105,
        },
        "write": {
            "TARGET_POS": 2100,
            "TARGET_SPEED": 2101,
            "HOME": 2102,
            "INITIALIZE": 2103,
            "JOG_FORWARD": 2104,
            "JOG_REVERSE": 2105,
            "OP_ENABLE": 2106,
            "START": 2107,
            "STOP": 2108
        }
    },
    "auto": {
        "read": {
            "OK": 3000,
            "NOK": 3001,
            "NR": 3002,
            "SKIP": 3004,
            "BARCODE_1": 3100,
            "BARCODE_2": 3116,
            "BARCODE_3": 3132,
            "BARCODE_4": 3148,
            "ALARM": 3200,
            "RUNNING_MSG": 3201,
            "PLC_KEEPALIVE_BIT": 3202,
            "VISION_COMM_OK": 3203,
            "SCARRA_ROBOT_COMM_OK": 3204,
            "LCMR_COMM_OK": 3205,
            "GANTRY_1": 3206,
            "GANTRY_2": 3207
        }
    },
    "io": {
        "read": {
            "DIGITAL_INPUT_1": 3300,
            "DIGITAL_INPUT_2": 3301,
            "DIGITAL_OUTPUT_1": 3302,
            "DIGITAL_OUTPUT_2": 3303
        }
    },
    "robo": {
        "read": {
            "ROBOT_MODE": 4000,
            "CONTROLLER_STATUS": 4001,
            "PROGRAM_STATUS": 4002
        },
        "write": {
            "ABORT_PROGRAM": 4100,
            "HOME_PROGRAM": 4101,
            "MAIN_PROGRAM": 4102,
            "INITIALIZE_PROGRAM": 4103,
            "PAUSE_PROGRAM": 4104,
            "ERROR_ACKNOWLEDGE": 4105,
            "CONTINUE_PROGRAM": 4106,
            "START_PROGRAM": 4107
        }
    }
}

ALARM_CODES = {
    0: "No Alarm",
    1: "Emergency Stop Triggered",
    2: "Overheat Error",
    3: "Limit Switch Fault",
    4: "Power Loss Detected",
    5: "Vision Mismatch"
}

# ─── Kafka Producer ────────────────────────────────────────────────────
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

# ─── Helper Functions ──────────────────────────────────────────────────
def get_client():
    return ModbusTcpClient(PLC_HOST, port=PLC_PORT)

def read_tags(section):
    client = get_client()
    client.connect()
    out = {}
    for name, addr in TAG_MAP[section].get("read", {}).items():
        rr = client.read_holding_registers(addr, 1)
        if rr.isError():
            out[name] = None
        else:
            out[name] = rr.registers[0]
    client.close()
    return out

def write_tags(section, tags: dict):
    client = get_client()
    client.connect()
    for name, val in tags.items():
        addr = TAG_MAP[section]["write"].get(name)
        if addr is not None:
            client.write_register(addr, int(val))
    client.close()

def get_alarm_message(code: int):
    return ALARM_CODES.get(code, "Unknown Alarm")
def decode_string(words):
    """Convert list of 16-bit words into ASCII string."""
    raw_bytes = b''.join([(w & 0xFF).to_bytes(1, 'little') + ((w >> 8) & 0xFF).to_bytes(1, 'little') for w in words])
    return raw_bytes.decode("ascii", errors="ignore").rstrip("\x00")

async def read_loop():
    client = AsyncModbusTcpClient(host=os.getenv("PLC_IP"), port=int(os.getenv("PLC_PORT")))

    await client.connect()
    if not client.connected:
        raise ConnectionError("Failed to connect to PLC")

    print(f"Connected to PLC at {PLC_HOST}:{PLC_PORT}")

    try:
        while True:
            now = time.strftime("%Y-%m-%dT%H:%M:%S")

            # Read barcode flags
            flags = await client.read_holding_registers(address=BARCODE_FLAG_1, count=2)
            flag1, flag2 = flags.registers

            if flag1 == 1:
                words = await client.read_holding_registers(*BARCODE_1_BLOCK)
                barcode1 = decode_string(words.registers)
                producer.send(KAFKA_TOPIC_BARCODE, {"barcode": barcode1, "camera": "1", "ts": now})
                await client.write_register(BARCODE_FLAG_1, 0)

            if flag2 == 1:
                words = await client.read_holding_registers(*BARCODE_2_BLOCK)
                barcode2 = decode_string(words.registers)
                producer.send(KAFKA_TOPIC_BARCODE, {"barcode": barcode2, "camera": "2", "ts": now})
                await client.write_register(BARCODE_FLAG_2, 0)

            # Read both station status registers (for 13-bit states)
            statuses = await client.read_holding_registers(address = STATUS_REGISTER,count= 2)
            s1, s2 = statuses.registers

            bitfield1 = format(s1, "013b")[::-1]
            bitfield2 = format(s2, "013b")[::-1]

            status_set_1 = {STATUS_BITS[i]: int(bitfield1[i]) for i in range(13)}
            status_set_2 = {STATUS_BITS[i]: int(bitfield2[i]) for i in range(13)}

            # Read corresponding barcodes
            bc1 = await client.read_holding_registers(3100, 16)
            bc2 = await client.read_holding_registers(3116, 16)
            bc3 = await client.read_holding_registers(3132, 16)
            bc4 = await client.read_holding_registers(3148, 16)

            barcode1 = decode_string(bc1.registers)
            barcode2 = decode_string(bc2.registers)
            barcode3 = decode_string(bc3.registers)
            barcode4 = decode_string(bc4.registers)

            # Enrich status messages with barcode info
            status_set_1.update({
                "barcode1": barcode1,
                "barcode2": barcode2,
                "ts": now
            })

            status_set_2.update({
                "barcode3": barcode3,
                "barcode4": barcode4,
                "ts": now
            })

            # Send both sets to Kafka
            producer.send(KAFKA_TOPIC_STATUS, status_set_1)
            producer.send(KAFKA_TOPIC_STATUS, status_set_2)

            await asyncio.sleep(2)

    except Exception as e:
        print("Error in machine interface loop:", e)

    finally:
        if client and hasattr(client, "close"):
            await client.close()


# ─── Main ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    asyncio.run(read_loop())
