import asyncio
import websockets
import json
import uuid

# Replace with the actual FastAPI server URL
WS_URL = "ws://localhost:8000/ws/plc-write"

# Sample write command payload
write_command = {
    "section": "auto",             # Section defined in your register_map
    "tag_name": "StartCycle",      # Tag you want to write to
    "value": 1,                    # Value to write
    "request_id": str(uuid.uuid4())  # Optional, can be omitted if auto-generated
}

async def send_plc_write():
    async with websockets.connect(WS_URL) as ws:
        print(f"Connected to WebSocket: {WS_URL}")

        # Send the write command
        await ws.send(json.dumps(write_command))
        print(f"Sent write command: {write_command}")

        # Wait for acknowledgment or error
        try:
            while True:
                response = await ws.recv()
                print(f"Received: {response}")
        except websockets.ConnectionClosed as e:
            print(f"Connection closed: {e}")

if __name__ == "__main__":
    asyncio.run(send_plc_write())
