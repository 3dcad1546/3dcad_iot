# import asyncio
# import websockets
# import json
# import uuid

# # Replace with the actual FastAPI server URL
# WS_URL = "ws://localhost:8000/ws/plc-write"

# # Sample write command payload
# write_command = {
#     "section": "auto",             # Section defined in your register_map
#     "tag_name": "StartCycle",      # Tag you want to write to
#     "value": 1,                    # Value to write 
#     "request_id": str(uuid.uuid4())  # Optional, can be omitted if auto-generated
# }

# async def send_plc_write():
#     async with websockets.connect(WS_URL) as ws:
#         print(f"Connected to WebSocket: {WS_URL}")

#         # Send the write command
#         await ws.send(json.dumps(write_command))
#         print(f"Sent write command: {write_command}")

#         # Wait for acknowledgment or error
#         try:
#             while True:
#                 response = await ws.recv()
#                 print(f"Received: {response}")
#         except websockets.ConnectionClosed as e:
#             print(f"Connection closed: {e}")

# if __name__ == "__main__":
#     asyncio.run(send_plc_write())


import asyncio
from pymodbus.client import AsyncModbusTcpClient

async def read_main_control_unit_coil():
    host = '192.168.10.3'  # Your Modbus TCP server IP
    port = 502
    coil_address = 1001

    async with AsyncModbusTcpClient(host, port=port) as client:
        if client.connected:
            try:
                
                result = await client.read_coils(address=coil_address, count=1, slave=1)

                if result.isError():
                    print(f"Error reading coil at address {coil_address}: {result}")
                else:
                    state = result.bits[0]
                    print(state,"state")
                    print(f"Coil at address {coil_address} (STOPPB-MAINCONTROLUNIT) is: {'ON' if state else 'OFF'}")

            except Exception as e:
                print(f"Exception during Modbus read: {e}")
        else:
            print("Failed to connect to Modbus server.")

# Run the async task
asyncio.run(read_main_control_unit_coil())


