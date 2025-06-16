import os
import json
import time
import asyncio
from pymodbus.client.tcp import AsyncModbusTcpClient
from pymodbus.exceptions import ModbusException
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer 
from aiokafka.errors import KafkaConnectionError, NoBrokersAvailable as AIOKafkaNoBrokersAvailable 

# ─── Configuration ─────────────────────────────────────────────────────
PLC_HOST = os.getenv("PLC_IP", "192.168.10.3")
PLC_PORT = int(os.getenv("PLC_PORT", "502"))

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC_BARCODE = os.getenv("BARCODE_TOPIC", "trigger_events")
KAFKA_TOPIC_STATUS = os.getenv("MACHINE_STATUS_TOPIC", "machine_status")
KAFKA_TOPIC_GENERIC_TAGS = os.getenv("GENERIC_TAGS_TOPIC", "plc_generic_tags")
KAFKA_TOPIC_WRITE_COMMANDS = os.getenv("PLC_WRITE_COMMANDS_TOPIC", "plc_write_commands")
KAFKA_TOPIC_WRITE_RESPONSES = os.getenv("PLC_WRITE_RESPONSES_TOPIC", "plc_write_responses")


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

# ─── Global AIOKafkaProducer Instance ──────────────────────────────────
aiokafka_producer: AIOKafkaProducer = None


async def init_aiokafka_producer():
    global aiokafka_producer
    print(f"Connecting to Kafka broker at {KAFKA_BROKER} for AIOKafkaProducer...")
    for attempt in range(10):
        try:
            producer = AIOKafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                request_timeout_ms=10000, # Increased timeout
                api_version=(2, 8, 1) # Specify API version for broader compatibility
            )
            await producer.start()
            aiokafka_producer = producer
            print("[AIOKafka Producer] Connection established.")
            return
        except AIOKafkaNoBrokersAvailable:
            print(f"[AIOKafka Producer] Kafka not ready. Retrying ({attempt + 1}/10)...")
            await asyncio.sleep(5)
        except Exception as e:
            print(f"[AIOKafka Producer] Error starting producer: {e}. Retrying ({attempt + 1}/10)...")
            await asyncio.sleep(5)
    raise RuntimeError("Failed to connect to AIOKafkaProducer after 10 attempts")

# ─── Helper Functions ──────────────────────────────────────────────────
def read_json_file(file_path):
    """
    Reads a JSON file and returns its content as a Python dictionary.
    """
    if not os.path.exists(file_path):
        print(f"Error: File not found at '{file_path}'")
        return None
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from '{file_path}': {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred while reading '{file_path}': {e}")
        return None

async def read_tags_sync(client: AsyncModbusTcpClient, section:str):
    out = {}
    if not client.connected:
        print(f"Warning: Modbus client not connected when trying to read section '{section}'.")
        return out

    config_data = read_json_file("register_map.json")
    if not config_data:
        print("Error: Could not read register map configuration.")
        return out

    TAG_MAP = config_data.get("tags", config_data) # Use config_data directly if 'tags' key isn't there
    section_data = TAG_MAP.get(section)
    if not section_data:
        print(f"Error: Section '{section}' not found in register map.")
        return out

    for name, addr in section_data.get("read", {}).items():
        try:
            # Use await with the async client
            rr = await client.read_holding_registers(addr, 1)
            if rr.isError():
                out[name] = None
                print(f"Error reading tag {name} at address {addr}: {rr}")
            else:
                out[name] = rr.registers[0]
        except ModbusException as e:
            out[name] = None
            print(f"ModbusException reading tag {name} at address {addr}: {e}")
        except Exception as e:
            out[name] = None
            print(f"Exception reading tag {name} at address {addr}: {e}")
    return out
   

async def async_write_tags(client: AsyncModbusTcpClient, section: str, tags: dict, request_id: str = None):
    """
    Writes tags to the PLC asynchronously using the provided Modbus client.
    Includes a request_id for response tracking.
    Publishes the result (SUCCESS/FAILED/TIMEOUT) to KAFKA_TOPIC_WRITE_RESPONSES.
    """
    response_payload = {
        "request_id": request_id,
        "section": section,
        "tags": tags,
        "status": "FAILED",
        "message": "Unknown error during write.",
        "ts": time.strftime("%Y-%m-%dT%H:%M:%S")
    }

    if not client.connected:
        response_payload["message"] = "Modbus client not connected."
        if aiokafka_producer:
            await aiokafka_producer.send_and_wait(KAFKA_TOPIC_WRITE_RESPONSES, value=response_payload)
        print(f"Warning: Modbus client not connected when trying to write to section '{section}'.")
        return

    config_data = read_json_file("register_map.json")
    if not config_data:
        response_payload["message"] = "Could not read register map for writing."
        if aiokafka_producer:
            await aiokafka_producer.send_and_wait(KAFKA_TOPIC_WRITE_RESPONSES, value=response_payload)
        print("Error: Could not read register map for writing.")
        return
    
    TAG_MAP = config_data.get("tags", config_data)
    section_data = TAG_MAP.get(section)
    if not section_data:
        response_payload["message"] = f"Section '{section}' not found in register map for writing."
        if aiokafka_producer:
            await aiokafka_producer.send_and_wait(KAFKA_TOPIC_WRITE_RESPONSES, value=response_payload)
        print(f"Error: Section '{section}' not found in register map for writing.")
        return

    all_writes_successful = True
    for name, val in tags.items():
        addr = section_data.get("write", {}).get(name)
        if addr is not None:
            try:
                rr = await client.write_register(addr, int(val))
                if rr.isError():
                    all_writes_successful = False
                    print(f"Error writing tag {name} to address {addr}: {rr}")
                    response_payload["message"] = f"Failed to write tag '{name}': {rr}"
                    # No break here, try to write all tags even if one fails.
                    # Or break if you want to stop on first error.
                else:
                    print(f"Successfully wrote {val} to {name} at {addr}")
            except ModbusException as e:
                all_writes_successful = False
                response_payload["message"] = f"ModbusException writing tag {name} to address {addr}: {e}"
                print(response_payload["message"])
                break # Break on Modbus or other severe exceptions
            except Exception as e:
                all_writes_successful = False
                response_payload["message"] = f"Exception writing tag {name} to address {addr}: {e}"
                print(response_payload["message"])
                break
        else:
            all_writes_successful = False
            response_payload["message"] = f"Warning: Tag '{name}' not found in write section for '{section}'"
            print(response_payload["message"])
            break # Break if tag not configured for write

    if all_writes_successful:
        response_payload["status"] = "SUCCESS"
        response_payload["message"] = "All tags written successfully."
    
    # Publish the final response to Kafka (always publish, even on failure)
    if aiokafka_producer:
        await aiokafka_producer.send_and_wait(KAFKA_TOPIC_WRITE_RESPONSES, value=response_payload)
    else:
        print("Error: AIOKafkaProducer not initialized, cannot send write response.")

def decode_string(words):
    """Convert list of 16-bit words into ASCII string."""
    raw_bytes = b''.join([(w & 0xFF).to_bytes(1, 'little') + ((w >> 8) & 0xFF).to_bytes(1, 'little') for w in words])
    return raw_bytes.decode("ascii", errors="ignore").rstrip("\x00")

async def read_specific_plc_data(client: AsyncModbusTcpClient):
    """
    Reads specific barcode and status data using the provided AsyncModbusTcpClient.
    Runs continuously.
    """
    while True:
        if not client.connected:
            print("❌ Async client not connected for specific data read. Attempting reconnect...")
            try:
                await client.connect() # Attempt to reconnect
                if not client.connected:
                    print("❌ Could not reconnect to PLC for specific data read. Waiting...")
                    await asyncio.sleep(5)
                    continue
                else:
                    print("✅ Reconnected to PLC for specific data read.")
            except Exception as e:
                print(f"Error during reconnection attempt for specific data: {e}. Waiting...")
                await asyncio.sleep(5)
                continue

        now = time.strftime("%Y-%m-%dT%H:%M:%S")

        try:
            # Read barcode flags
            flags_response = await client.read_holding_registers(address=BARCODE_FLAG_1, count=2)
            if not flags_response.isError():
                flag1, flag2 = flags_response.registers
                
                # --- Barcode 1 ---
                if flag1 == 1:
                    words_response = await client.read_holding_registers(*BARCODE_1_BLOCK)
                    if not words_response.isError():
                        barcode1 = decode_string(words_response.registers)
                        if aiokafka_producer:
                            await aiokafka_producer.send(KAFKA_TOPIC_BARCODE, value={"barcode": barcode1, "camera": "1", "ts": now})
                        await client.write_register(BARCODE_FLAG_1, 0) # Reset flag
                        print(f"Barcode 1 ({barcode1}) triggered.")
                    else:
                        print(f"Error reading BARCODE_1_BLOCK: {words_response}")
                
                # --- Barcode 2 ---
                if flag2 == 1:
                    words_response = await client.read_holding_registers(*BARCODE_2_BLOCK)
                    if not words_response.isError():
                        barcode2 = decode_string(words_response.registers)
                        if aiokafka_producer:
                            await aiokafka_producer.send(KAFKA_TOPIC_BARCODE, value={"barcode": barcode2, "camera": "2", "ts": now})
                        await client.write_register(BARCODE_FLAG_2, 0) # Reset flag
                        print(f"Barcode 2 ({barcode2}) triggered.")
                    else:
                        print(f"Error reading BARCODE_2_BLOCK: {words_response}")
            else:
                print("Error reading barcode flags:", flags_response)

            # Read both station status registers (for 13-bit states)
            statuses_response = await client.read_holding_registers(address = STATUS_REGISTER, count= 2)
            if not statuses_response.isError():
                s1, s2 = statuses_response.registers

                bitfield1 = format(s1, "013b")[::-1]
                bitfield2 = format(s2, "013b")[::-1]

                status_set_1 = {STATUS_BITS[i]: int(bitfield1[i]) for i in range(min(len(STATUS_BITS), len(bitfield1)))}
                status_set_2 = {STATUS_BITS[i]: int(bitfield2[i]) for i in range(min(len(STATUS_BITS), len(bitfield2)))}

                # Read corresponding barcodes (these are usually kept separate from trigger barcodes)
                bc1_response = await client.read_holding_registers(address= 3100, count=16)
                bc2_response = await client.read_holding_registers(address= 3116, count=16)
                bc3_response = await client.read_holding_registers(address= 3132, count=16)
                bc4_response = await client.read_holding_registers(address= 3148, count=16)

                barcode1_status = decode_string(bc1_response.registers) if (not bc1_response.isError() and bc1_response.registers) else None
                barcode2_status = decode_string(bc2_response.registers) if (not bc2_response.isError() and bc2_response.registers) else None
                barcode3_status = decode_string(bc3_response.registers) if (not bc3_response.isError() and bc3_response.registers) else None
                barcode4_status = decode_string(bc4_response.registers) if (not bc4_response.isError() and bc4_response.registers) else None

                # Enrich status messages with barcode info
                status_set_1.update({
                    "barcode1_at_status": barcode1_status,
                    "barcode2_at_status": barcode2_status,
                    "ts": now
                })

                status_set_2.update({
                    "barcode3_at_status": barcode3_status,
                    "barcode4_at_status": barcode4_status,
                    "ts": now
                })

                # Send both sets to Kafka
                if aiokafka_producer:
                    await aiokafka_producer.send(KAFKA_TOPIC_STATUS, value=status_set_1)
                    await aiokafka_producer.send(KAFKA_TOPIC_STATUS, value=status_set_2)
            else:
                print("Error reading status registers:", statuses_response)

        except ModbusException as e:
            print(f"❌ Modbus Exception during specific data read: {e}. Closing connection to force a reconnect...")
            client.close() # Close connection to force a reconnect on next loop iteration
            await asyncio.sleep(1) # Small delay before next retry
        except Exception as e:
            print(f"Error in specific data reading loop: {e}")
        
        await asyncio.sleep(2) # Polling interval for specific data

async def read_generic_tags_and_publish_loop(client: AsyncModbusTcpClient, interval_seconds=5):
    """
    Periodically reads generic tags from the PLC using read_tags_async
    and publishes them to a Kafka topic.
    """
    while True:
        if not client.connected:
            print("Warning: Client not connected for generic tags. Skipping this cycle.")
            await asyncio.sleep(interval_seconds)
            continue
            
        try:
            now = time.strftime("%Y-%m-%dT%H:%M:%S")
            
            sections_to_read = ["startup", "auto", "io", "robo", "manual"] # Added "manual"
            combined_generic_data = {"ts": now}

            for section in sections_to_read:
                section_data = await read_tags_async(client, section)
                # Combine data with section prefix
                combined_generic_data.update({f"{section}_{k}": v for k, v in section_data.items()})

            if aiokafka_producer and combined_generic_data and len(combined_generic_data) > 1:
                await aiokafka_producer.send(KAFKA_TOPIC_GENERIC_TAGS, value=combined_generic_data)
                # print(f"[{now}] Sent generic tags to Kafka topic '{KAFKA_TOPIC_GENERIC_TAGS}'.")
            else:
                pass 

        except Exception as e:
            print(f"[{now}] Error in generic tags publishing loop: {e}")

        await asyncio.sleep(interval_seconds)


async def kafka_write_consumer_loop(client: AsyncModbusTcpClient):
    """
    AIOKafkaConsumer loop that listens for write commands and executes them on the PLC.
    This runs entirely asynchronously.
    """
    consumer = None
    print(f"Starting AIOKafkaConsumer for write commands on topic: {KAFKA_TOPIC_WRITE_COMMANDS}")
    for attempt in range(10): # Connection retry logic
        try:
            consumer = AIOKafkaConsumer(
                KAFKA_TOPIC_WRITE_COMMANDS,
                bootstrap_servers=KAFKA_BROKER,
                group_id='plc-write-gateway-group', # Required for AIOKafkaConsumer
                auto_offset_reset='earliest',
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            await consumer.start()
            print("[AIOKafka Consumer] Write command consumer started.")
            break
        except AIOKafkaNoBrokersAvailable:
            print(f"[AIOKafka Consumer] Kafka not ready for consumer. Retrying ({attempt + 1}/10)...")
            await asyncio.sleep(5)
        except Exception as e:
            print(f"[AIOKafka Consumer] Error starting consumer: {e}. Retrying ({attempt + 1}/10)...")
            await asyncio.sleep(5)
    else:
        raise RuntimeError("Failed to connect to AIOKafkaConsumer after 10 attempts")

    try:
        async for message in consumer:
            now = time.strftime("%Y-%m-%dT%H:%M:%S")
            print(f"[{now}] Received Kafka write command: {message.value}")
            
            command = message.value
            section = command.get("section")
            tag_name = command.get("tag_name")
            value = command.get("value")
            request_id = command.get("request_id")

            if not all([section, tag_name, value is not None]):
                print(f"[{now}] Invalid write command received: {command}. Skipping.")
                if aiokafka_producer:
                    await aiokafka_producer.send(KAFKA_TOPIC_WRITE_RESPONSES, value={
                        "request_id": request_id,
                        "status": "FAILED",
                        "message": "Invalid command format. Requires 'section', 'tag_name', 'value'.",
                        "original_command": command,
                        "ts": now
                    })
                continue
            
            try:
                tags_to_write = {tag_name: value}
                
                # Directly await async_write_tags, no ThreadPoolExecutor needed
                await async_write_tags(client, section, tags_to_write, request_id)
                
                print(f"[{now}] Completed processing write command for request_id: {request_id}")
                
            except asyncio.TimeoutError: # This might be caught by async_write_tags, but good to have here too
                print(f"[{now}] Timeout while writing to PLC for command (request_id: {request_id}): {command}")
                if aiokafka_producer:
                    await aiokafka_producer.send(KAFKA_TOPIC_WRITE_RESPONSES, value={
                        "request_id": request_id,
                        "status": "TIMEOUT",
                        "message": "PLC write operation timed out.",
                        "original_command": command,
                        "ts": now
                    })
            except Exception as e:
                print(f"[{now}] Error executing write command (request_id: {request_id}) {command}: {e}")
                if aiokafka_producer:
                    await aiokafka_producer.send(KAFKA_TOPIC_WRITE_RESPONSES, value={
                        "request_id": request_id,
                        "status": "FAILED",
                        "message": f"Error during PLC write: {str(e)}",
                        "original_command": command,
                        "ts": now
                    })
    except Exception as e:
        print(f"[{time.strftime('%Y-%m-%dT%H:%M:%S')}] Unexpected error in AIOKafkaConsumer: {e}")
    finally:
        if consumer:
            await consumer.stop()
            print("AIOKafkaConsumer for write commands stopped.")


# ─── Main ──────────────────────────────────────────────────────────────
async def main():
    """
    Main function to establish a single Modbus client connection
    and start all asynchronous tasks.
    """
    # Create a single AsyncModbusTcpClient instance
    client = AsyncModbusTcpClient(host=PLC_HOST, port=PLC_PORT)

    # Establish connection once for all tasks
    print(f"Connecting to PLC at {PLC_HOST}:{PLC_PORT} for all tasks...")
    await client.connect()
    if not client.connected:
        print("❌ Initial connection to PLC failed. Exiting.")
        return # Exit if initial connection fails

    print(f"✅ Connected to PLC at {PLC_HOST}:{PLC_PORT}")

    try:
        # Initialize the AIOKafkaProducer first
        await init_aiokafka_producer()

        # Start all other asynchronous tasks
        asyncio.create_task(read_specific_plc_data(client))
        asyncio.create_task(read_generic_tags_and_publish_loop(client, interval_seconds=5))
        asyncio.create_task(kafka_write_consumer_loop(client)) # This is now fully async

        # Keep the main event loop running indefinitely
        await asyncio.Future()

    except Exception as e:
        print(f"An unexpected error occurred in main loop: {e}")
    finally:
        if client.connected:
            print("Closing Modbus client connection.")
            client.close()
        if aiokafka_producer:
            await aiokafka_producer.stop()
            print("AIOKafkaProducer closed.")
        # No more ThreadPoolExecutor to shut down for Kafka, as AIOKafka is async



# ─── Main ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    try:
        print("Starting PLC Machine Interface Service...")
        # Run the main async function
        import logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        print("Initializing Modbus client and Kafka producer...")
         # Run the main async function
         # This will block until the service is stopped or an error occurs    
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nServer stopped by user.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
