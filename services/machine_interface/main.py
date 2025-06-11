import os
import json
import time
import asyncio
from pymodbus.client import AsyncModbusTcpClient
from kafka import KafkaProducer

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

# ─── Kafka Producer ────────────────────────────────────────────────────
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# ─── Helper Functions ──────────────────────────────────────────────────
def decode_string(words):
    """Convert list of 16-bit words into ASCII string."""
    raw_bytes = b''.join([(w & 0xFF).to_bytes(1, 'little') + ((w >> 8) & 0xFF).to_bytes(1, 'little') for w in words])
    return raw_bytes.decode("ascii", errors="ignore").rstrip("\x00")

async def read_loop():
    client = AsyncModbusTcpClient(host=PLC_HOST, port=PLC_PORT)
    await client.connect()
    print(f"Connected to PLC at {PLC_HOST}:{PLC_PORT}")

    try:
        while True:
            now = time.strftime("%Y-%m-%dT%H:%M:%S")
            
            # Read barcode flags
            flags = await client.read_holding_registers(BARCODE_FLAG_1, 2)
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

            # Read 13 status bits from PLC register
            status = await client.read_holding_registers(STATUS_REGISTER, 1)
            bitfield = format(status.registers[0], "013b")[::-1]
            status_dict = {STATUS_BITS[i]: int(bitfield[i]) for i in range(13)}
            status_dict["ts"] = now
            producer.send(KAFKA_TOPIC_STATUS, status_dict)

            await asyncio.sleep(2)

    except Exception as e:
        print("Error in machine interface loop:", e)

    finally:
        await client.close()

# ─── Main ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    asyncio.run(read_loop())
