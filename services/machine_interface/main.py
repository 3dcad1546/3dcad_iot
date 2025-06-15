import os
import json
import time
import asyncio
from pymodbus.client.tcp import AsyncModbusTcpClient
#from pymodbus.client.tcp import ModbusTcpClient
from pymodbus.exceptions import ModbusException
from kafka import KafkaProducer, KafkaAdminClient
from kafka.errors import NoBrokersAvailable, KafkaError
# ─── kafka init─────────────────────────────────────────────────────



# ─── Configuration ─────────────────────────────────────────────────────
PLC_HOST = os.getenv("PLC_IP", "192.168.10.3")
PLC_PORT = int(os.getenv("PLC_PORT", "502"))
# PLC_IP = os.getenv("PLC_IP", "192.168.1.100")  # Default to your PLC IP
# PLC_PORT = int(os.getenv("PLC_PORT", "502"))

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
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
            "DIGITAL_INPUT_1": 1000,
            "DIGITAL_INPUT_2": 1001,
            "DIGITAL_OUTPUT_1": 1002,
            "DIGITAL_OUTPUT_2": 1003
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
    """Create and return a Kafka producer with connection testing"""
    kafka_broker = os.getenv("KAFKA_BROKER", "kafka:9092")
    print(f"Connecting to Kafka broker at {kafka_broker}...")
    
    for attempt in range(10):
        try:
            # First test connection with AdminClient
            admin = KafkaAdminClient(
                bootstrap_servers=kafka_broker,
                request_timeout_ms=10000
            )
            admin.list_topics()  # Test connection
            admin.close()
            
            # Then create producer
            producer = KafkaProducer(
                bootstrap_servers=kafka_broker,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=5,
                request_timeout_ms=10000,
                api_version=(2, 8, 1)
            )
            print("Kafka connection established")
            return producer
            
        except (NoBrokersAvailable, KafkaError) as e:
            print(f"Kafka connection failed (attempt {attempt+1}/10): {str(e)}")
            time.sleep(min(2 ** attempt, 10))  # Exponential backoff with max 10s
        finally:
            if 'admin' in locals():
                admin.close()
    
    raise RuntimeError("Failed to connect to Kafka after 10 attempts")

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
    #client = AsyncModbusTcpClient(host=os.getenv("PLC_IP"), port=int(os.getenv("PLC_PORT")))
    client = AsyncModbusTcpClient(host='192.168.10.3', port=502)
     # Establish connection
    await client.connect()
    if not client.connected:
        print("❌ Could not connect to PLC.")
        return

    print(f"✅ Connected to PLC at {PLC_HOST}:{PLC_PORT}")
    

    try:
        while True:
            now = time.strftime("%Y-%m-%dT%H:%M:%S")

            # Read barcode flags
            flags = await client.read_holding_registers(address=BARCODE_FLAG_1,count=2)
            flag1, flag2 = flags.registers

            if flag1 == 1:
                words = await client.read_holding_registers(*BARCODE_2_BLOCK)
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
            bc1 = await client.read_holding_registers(address= 3100, count=16)
            bc2 = await client.read_holding_registers(address= 3116, count=16)
            bc3 = await client.read_holding_registers(address= 3132, count=16)
            bc4 = await client.read_holding_registers(address= 3148, count=16)

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
    except ModbusException as e:
        print("❌ Modbus Exception:", e)
        
    except Exception as e:
        print("Error in machine interface loop:", e)

    finally:
        if client and hasattr(client, "close"):
            client.close()


# ─── Main ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    asyncio.run(read_loop())
