import os
import json
import asyncio
from pymodbus.client.tcp import AsyncModbusTcpClient

REGISTER_MAP_PATH = "register_map.json"
PLC_HOST = os.getenv("PLC_IP", "192.168.10.3")
PLC_PORT = int(os.getenv("PLC_PORT", "502"))

def read_json_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)

async def main():
    cfg = read_json_file(REGISTER_MAP_PATH)
    stations = cfg.get("stations", {})

    client = AsyncModbusTcpClient(host=PLC_HOST, port=PLC_PORT)
    await client.connect()
    if not client.connected:
        print(f"Could not connect to PLC at {PLC_HOST}:{PLC_PORT}")
        return

    print(f"Connected to PLC at {PLC_HOST}:{PLC_PORT}")
    print("Polling station status registers with handshake...\n")

    # Track previous status for edge detection
    previous_status = {name: 0 for name in stations.keys()}

    try:
        while True:
            for name, spec in stations.items():
                reg1, bit1 = spec["status_1"]
                rr1 = await client.read_holding_registers(address=reg1, count=1)
                v1 = (rr1.registers[0] >> bit1) & 1 if not rr1.isError() and rr1.registers else None

                # Rising edge detection and handshake
                if previous_status[name] == 0 and v1 == 1:
                    print(f"[EVENT] {name:20s} | status_1: reg={reg1}, bit={bit1} → {v1} (resetting bit)")
                    # Reset the bit to 0 after reading
                    current = rr1.registers[0]
                    new = current & ~(1 << bit1)
                    await client.write_register(reg1, new)
                else:
                    print(f"{name:20s} | status_1: reg={reg1}, bit={bit1} → {v1}")

                previous_status[name] = v1

            await asyncio.sleep(0.5)  # Poll every 50ms (adjust as needed)
    finally:
        await client.close()

if __name__ == "__main__":
    asyncio.run(main())