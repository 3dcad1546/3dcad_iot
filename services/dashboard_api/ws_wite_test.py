import asyncio
import websockets
import json

# --- Configuration ---
# Replace 'device-ip' with the actual IP address or hostname of your device
WEBSOCKET_URL = "ws://device-ip/ws/plc-write"

# The message to send to the PLC
MESSAGE_TO_SEND = {
    "section": "manual",
    "tag_name": "START",
    "value": 1
}

# --- WebSocket Client Function ---
async def plc_websocket_client():
    """
    Connects to the PLC WebSocket endpoint, sends a message, and listens for responses.
    """
    print(f"Attempting to connect to WebSocket: {WEBSOCKET_URL}")

    try:
        # Establish a WebSocket connection
        # The 'async with' statement ensures the connection is properly closed
        async with websockets.connect(WEBSOCKET_URL) as websocket:
            print("WebSocket connection established successfully.")

            # Convert the Python dictionary to a JSON string
            json_message = json.dumps(MESSAGE_TO_SEND)
            print(f"Sending message: {json_message}")

            # Send the JSON message over the WebSocket
            await websocket.send(json_message)
            print("Message sent.")

            # --- Listen for responses (optional but recommended for debugging) ---
            print("Listening for messages from the server...")
            try:
                # Keep listening for messages from the server indefinitely
                # You can add logic here to break the loop if a specific response is received
                while True:
                    # Receive a message from the server
                    response = await websocket.recv()
                    print(f"Received message from server: {response}")
            except websockets.exceptions.ConnectionClosedOK:
                print("Server closed the connection gracefully.")
            except websockets.exceptions.ConnectionClosedError as e:
                print(f"Server closed connection with error: {e}")
            except Exception as e:
                print(f"An error occurred while receiving messages: {e}")

    except websockets.exceptions.InvalidURI:
        print(f"Error: Invalid WebSocket URI. Please check the URL: {WEBSOCKET_URL}")
    except ConnectionRefusedError:
        print(f"Error: Connection refused. Is the device at '{WEBSOCKET_URL}' running and accessible?")
    except websockets.exceptions.WebSocketException as e:
        print(f"WebSocket error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

# --- Run the client ---
if __name__ == "__main__":
    # Ensure to replace 'device-ip' in the WEBSOCKET_URL with your actual device's IP address
    # For example: WEBSOCKET_URL = "ws://192.168.1.100/ws/plc-write"
    print("Starting PLC WebSocket client...")
    asyncio.run(plc_websocket_client())
    print("PLC WebSocket client finished.")