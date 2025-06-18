import asyncio
import websockets
import json
from pymodbus.client import AsyncModbusTcpClient
from pymodbus.exceptions import ModbusIOException

modbus_bits = {
    "IPCONVEYORMODEPOS1": "1100",
    "CONTROLLERSTATUS1": "1101",
    "PROGRAMSTATUS1": "1102",
    "ACTUALPOS1": "1103",
    "ACTUALSPEED1": "1105",
    "JOGSPEED1": "1107",
    "TARGETPOS1": "1109",
    "TARGETSPEED1": "1111",
    "HOME1": "1130.0",
    "INITIALIZE1": "1130.1",
    "OPENENABLE1": "1130.2",
    "JOGFORWARD1": "1130.3",
    "JOGREVERSE1": "1130.4",
    "START1": "1130.5",
    "STOP1": "1130.6",
    "IPCONVEYORMODEPOS2": "1140",
    "CONTROLLERSTATUS2": "1141",
    "PROGRAMSTATUS2": "1142",
    "ACTUALPOS2": "1143",
    "ACTUALSPEED2": "1145",
    "JOGSPEED2": "1147",
    "TARGETPOS2": "1149",
    "TARGETSPEED2": "1151",
    "HOME2": "1170.0",
    "INITIALIZE2": "1170.1",
    "OPENENABLE2": "1170.2",
    "JOGFORWARD2": "1170.3",
    "JOGREVERSE2": "1170.4",
    "START2": "1170.5",
    "STOP2": "1170.6",
    "IPCONVEYORMODEPOS3": "1180",
    "CONTROLLERSTATUS3": "1181",
    "PROGRAMSTATUS3": "1182",
    "ACTUALPOS3": "1183",
    "ACTUALSPEED3": "1185",
    "JOGSPEED3": "1187",
    "TARGETPOS3": "1189",
    "TARGETSPEED3": "1191"
}

modbus_data = {}
clients = set()

def parse_address(addr):
    if '.' in addr:
        base, bit = addr.split('.')
        return int(base), int(bit), True
    else:
        return int(addr), None, False

async def poll_modbus():
    global modbus_data
    client = AsyncModbusTcpClient("192.168.10.3", port=502)
    await client.connect()

    try:
        while True:
            data = {}
            for name, addr_str in modbus_bits.items():
                reg, bit_index, is_bit = parse_address(addr_str)
                try:
                    rr = await client.read_holding_registers(reg, 1)
                    if rr.isError():
                        data[name] = None
                    else:
                        if is_bit:
                            bit_val = (rr.registers[0] >> bit_index) & 1
                            data[name] = bool(bit_val)
                        else:
                            data[name] = rr.registers[0]
                except Exception as e:
                    data[name] = None

            modbus_data = data

            disconnected = set()
            for ws in clients:
                try:
                    await ws.send(json.dumps(modbus_data))
                except:
                    disconnected.add(ws)

            clients.difference_update(disconnected)
            await asyncio.sleep(2)
    finally:
        await client.close()

async def websocket_handler(websocket):
    clients.add(websocket)
    print("Client connected")

    client = AsyncModbusTcpClient("192.168.10.3", port=502)
    await client.connect()

    try:
        data = json.loads(message)
        for name, value in data.items():
            if name not in modbus_bits:
                print(f"Unknown Modbus key: {name}")
                continue
            addr_str = modbus_bits[name]
            reg, bit_index, is_bit = parse_address(addr_str)
            if is_bit:
                rr = await client.read_holding_registers(reg, 1)
                if rr.isError():
                    continue
                current_val = rr.registers[0]
                if value == 1:
                    new_val = current_val | (1 << bit_index)
                else:
                    new_val = current_val & ~(1 << bit_index)
                await client.write_register(reg, new_val)
            else:
                await client.write_register(reg, int(value))
        print("Write success:", data)
    except Exception as e:
        print("Error handling WebSocket message:", e)
    finally:
        clients.discard(websocket)
        await client.close()
        print("Client disconnected")

async def main():
    server = await websockets.serve(websocket_handler, "localhost", 8765)
    print("WebSocket server running at ws://localhost:8765")
    await asyncio.gather(
        poll_modbus(),
        server.wait_closed()
    )

if __name__ == "__main__":
    asyncio.run(main())
