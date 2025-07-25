import os
import json
import asyncio
from pymodbus.client.tcp import AsyncModbusTcpClient

REGISTER_MAP_PATH = "../services/machine_interface/register_map.json"  # Adjust path if needed
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
    print("Reading all station status registers...\n")

    for name, spec in stations.items():
        reg1, bit1 = spec["status_1"]
        reg2, bit2 = spec["status_2"]
        rr1 = await client.read_holding_registers(address=reg1, count=1)
        rr2 = await client.read_holding_registers(address=reg2, count=1)
        v1 = (rr1.registers[0] >> bit1) & 1 if not rr1.isError() and rr1.registers else None
        v2 = (rr2.registers[0] >> bit2) & 1 if not rr2.isError() and rr2.registers else None
        print(f"{name:20s} | status_1: reg={reg1}, bit={bit1} → {v1} | status_2: reg={reg2}, bit={bit2} → {v2}")

    await client.close()

if __name__ == "__main__":
    asyncio.run(main())