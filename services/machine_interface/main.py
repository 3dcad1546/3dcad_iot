import os,json,time,asyncio,logging,requests,struct
from typing import Dict
from pymodbus.client.tcp import AsyncModbusTcpClient
from pymodbus.exceptions import ModbusException
from pymodbus.payload import BinaryPayloadBuilder, BinaryPayloadDecoder
from pymodbus.constants import Endian
from aiokafka import AIOKafkaProducer,AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError, NoBrokersAvailable as AIOKafkaNoBrokersAvailable

logger = logging.getLogger("machine_interface")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# ─── Toggle Simulation Mode ─────────────────────────────────────
USE_SIMULATOR = os.getenv("USE_SIMULATOR", "false").lower() in ("1", "true", "yes")

# module‐scope
current_token: str = None

# in your WS code, on login event:
async def handle_ws_login_event(msg):
    global current_token
    if msg["event"] == "login":
        current_token = msg["token"]   # assuming your payload includes token

# ─── Configuration ─────────────────────────────────────────────────────
PLC_HOST = os.getenv("PLC_IP", "192.168.10.3")
PLC_PORT = int(os.getenv("PLC_PORT", "502"))
LOGIN_TOPIC = os.getenv("LOGIN_TOPIC", "login_status")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
print("KAFKA_BROKER =", repr(KAFKA_BROKER), "| Type:", type(KAFKA_BROKER))
KAFKA_TOPIC_BARCODE = os.getenv("BARCODE_TOPIC", "trigger_events")

# Existing general machine status topic (from 13-bit array)
KAFKA_TOPIC_MACHINE_STATUS = os.getenv("MACHINE_STATUS_TOPIC", "machine_status") # Renamed for clarity vs per-section

# Topics for PLC write commands/responses
KAFKA_TOPIC_WRITE_COMMANDS = os.getenv("PLC_WRITE_COMMANDS_TOPIC", "plc_write_commands")
KAFKA_TOPIC_WRITE_RESPONSES = os.getenv("PLC_WRITE_RESPONSES_TOPIC", "plc_write_responses")

# --- NEW: Specific topics for each section's status ---
KAFKA_TOPIC_STARTUP_STATUS = os.getenv("STARTUP_STATUS_TOPIC", "startup_status")
KAFKA_TOPIC_MANUAL_STATUS = os.getenv("MANUAL_STATUS_TOPIC", "manual_status")
KAFKA_TOPIC_AUTO_STATUS = os.getenv("AUTO_STATUS_TOPIC", "auto_status")
KAFKA_TOPIC_ROBO_STATUS = os.getenv("ROBO_STATUS_TOPIC", "robo_status")
KAFKA_TOPIC_IO_STATUS = os.getenv("IO_STATUS_TOPIC", "io_status")
KAFKA_TOPIC_OEE_STATUS = os.getenv("OEE_STATUS_TOPIC", "oee_status") # New topic for OEE data
KAFKA_TOPIC_ON_OFF_STATUS = os.getenv("ON_OFF_TOPIC", "on_off_status")

# Barcode related registers (No change)
BARCODE_FLAG_1 = 3303
BARCODE_FLAG_2 = 3304

LOGIN_REGISTER   = int(os.getenv("LOGIN_REGISTER", "3309"))


# Mode 1 → Auto and 0 → Manual
MODE_REGISTER    = int(os.getenv("MODE_REGISTER", "1001"))
MES_PC_URL       = os.getenv("MES_PC_URL")
TRACE_HOST       = os.getenv("TRACE_HOST", "trace-proxy")
CBS_STREAM_NAME  = os.getenv("CBS_STREAM", "line1")

# user login url

USER_LOGIN_URL = os.getenv("USER_LOGIN_URL", "http://user_login:8001")

# 13-bit status array starts from register 3400 (No change)
STATUS_REGISTER = 3400
STATUS_BITS = [
    "InputStation", "Trace", "Process", "MES", "Transfer-1", "Vision-1",
    "PickPlace-1", "Transfer-2", "Vision-2", "PickPlace-2", "TraceUpload",
    "MESUpload", "UnloadStation"
]

# ─── Global AIOKafkaProducer Instance ──────────────────────────────────
aio_producer: AIOKafkaProducer = None

async def init_aiokafka_producer():
    global aio_producer
    logger.info(f"Connecting to Kafka at {KAFKA_BROKER}")
    broker = str(KAFKA_BROKER).strip()
    print(broker,"brokee")
    broker_list = [ broker ]
    print(broker_list,"broker_list")                   # <<< wrap it in a list
    logger.info(f"Connecting to Kafka at {broker_list!r}")
    def serializer(value):
        try:
            return json.dumps(value).encode('utf-8')
        except Exception as e:
            logger.error(f"[Kafka serializer] Failed for {value}: {e}")
            return b'{"error": "serialization failed"}'

    for attempt in range(10):
        try:
            
            producer = AIOKafkaProducer(
                bootstrap_servers=broker_list,
                value_serializer=serializer,
                request_timeout_ms=10000,
                # api_version=(2, 8, 1)
            )
            await producer.start()
            aio_producer = producer
            await aio_producer.send_and_wait("diagnostic", {"status": "connected"})
            logger.info("Kafka producer started and diagnostic sent.")
            return
        except KafkaConnectionError as e:
            logger.warning(f"Kafka connection error ({attempt + 1}/10): {e}")
        except Exception as e:
            logger.warning(f"Kafka error ({attempt + 1}/10): {e}")
        await asyncio.sleep(5)
    raise RuntimeError("Kafka producer failed after 10 attempts")



# Simulation of PLC
async def simulate_plc_data():
    """
    Periodically send dummy status payloads to Kafka topics so dashboard_api
    can pick them up over websockets.
    """
    print("🔧 Starting PLC data simulator…")
    while True:
        now = time.strftime("%Y-%m-%dT%H:%M:%S")
        # build two simple sets
        set1 = {"InputStation":1, "Trace":0, "MES":1, "ts": now}
        set2 = {"UnloadStation":0, "OEE":100, "ts": now}
        payload = {"set1":[set1], "set2":[set2]}
        await aio_producer.send_and_wait(KAFKA_TOPIC_MACHINE_STATUS, value=payload)
        # also simulate section‐specific topics if you like:
        await aio_producer.send_and_wait(KAFKA_TOPIC_STARTUP_STATUS, value={"status":"OK","ts":now})
        await asyncio.sleep(2)

async def simulate_plc_write_responses():
    """
    Listen for write commands and immediately echo back a SUCCESS response.
    """
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC_WRITE_COMMANDS,
        bootstrap_servers=KAFKA_BROKER,
        group_id="sim-write-resp",
        value_deserializer=lambda x: json.loads(x.decode())
    )
    await consumer.start()
    print("🔧 Write‐response simulator listening…")
    async for msg in consumer:
        cmd = msg.value
        resp = {
            "request_id": cmd.get("request_id"),
            "section":   cmd.get("section"),
            "tags":      {cmd.get("tag_name"): cmd.get("value")},
            "status":    "SUCCESS",
            "message":   "Simulated OK",
            "ts":        time.strftime("%Y-%m-%dT%H:%M:%S")
        }
        await aio_producer.send_and_wait(KAFKA_TOPIC_WRITE_RESPONSES, value=resp)
    await consumer.stop()


# ─── Helper Functions (decode_string, read_json_file, async_write_tags - No change) ──────────────────────────
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

async def read_tags_async(client: AsyncModbusTcpClient, section: str):
    """
    Reads tags from a specified section in the register map.
    Handles both single-word (Int, Boolean) and multi-word (String) reads.
    """
    out = {}
    if not client.connected:
        logging.info(f"Warning: Modbus client not connected when trying to read section '{section}'.")
        return out

    config_data = read_json_file("register_map.json")
    if not config_data:
        logging.info("Error: Could not read register map configuration.")
        return out
    
    # Debug print to verify config loading
    logging.info(f"Config data for section {section}: {json.dumps(config_data.get(section, {}), indent=2)}")

    section_data = config_data.get(section,{}).get("read",{})
    if not section_data:
        logging.error(f"Error: Section '{section}' not found in register map.")
        return out

    for name, cfg in section_data.items():
        # normalize to dict form
        if isinstance(cfg, dict):
            addr  = cfg["address"]
            typ   = cfg.get("type","holding")
            count = cfg.get("count", 1)
        elif isinstance(cfg, list) and len(cfg)==2:
            addr, count = cfg
            typ = "holding"
        elif isinstance(cfg, int):
            addr, count, typ = cfg, 1, "holding"
        else:
            print(f"Skipping bad config for {name}: {cfg}")
            continue

      


        try:
            if typ == "coil":
                rr = await client.read_coils(address=addr, count=count, slave=1)
                print(rr, "rrr")
                val = rr.bits[0] if not rr.isError() and rr.bits else None
                print(val, "val")
            else:
                rr = await client.read_holding_registers(address=addr, count=count)
                print("Raw register response:", rr)

                if rr.isError() or not rr.registers:
                    print(f"Error reading holding {name}@{addr}: Empty or Error")
                    val = None

                elif len(rr.registers) == 2:
                    # Combine two 16-bit registers into one 32-bit float
                    try:
                        # Pack the two registers into a byte array
                        packed_data = struct.pack('>HH',  rr.registers[1],rr.registers[0])
                        # Unpack the byte array as a single float
                        val = struct.unpack('>f', packed_data)[0]
                        print(f"Decoded float value: {val}")
                    except Exception as e:
                        print(f"Error decoding float for {name}: {e}")
                        val = rr.registers  # Fallback: return raw registers

                elif len(rr.registers) > 2:
                    try:
                        decoder = BinaryPayloadDecoder.fromRegisters(
                            rr.registers, 
                            byteorder=Endian.Big, 
                            wordorder=Endian.Big
                        )
                        val = decoder.decode_string(len(rr.registers) * 2).rstrip('\x00')
                    except Exception as e:
                        print(f"Error decoding string for {name}: {e}")
                        val = rr.registers  # Fallback

                else:  # Single register
                    val = rr.registers[0]

            out[name] = val
            print(out[name], "out[name]")





        except Exception as e:
            print(f"Error reading {typ} {name}@{addr}: {e}")
            out[name] = None
        except ModbusException as e: # Catch Modbus-specific errors
            logging.error(f"Modbus error reading tag '{name}' at {addr}: {e}")
            out[name] = None
        except Exception as e: # Catch any other unexpected errors during read
            logging.error(f"Unexpected error reading tag '{name}' at {addr}: {e}")
            out[name] = None
    return out 

async def async_write_tags(client: AsyncModbusTcpClient, section: str, tags: dict, request_id: str = None):
    """
    Writes tags to the PLC asynchronously using the provided Modbus client.
    Includes a request_id for response tracking.
    Publishes the result (SUCCESS/FAILED/TIMEOUT) to KAFKA_TOPIC_WRITE_RESPONSES.
    """
    print("cominginsideasync_write_tags")
    response_payload = {
        "request_id": request_id,
        "section": section,
        "tags": tags,
        "status": "FAILED",
        "message": "Unknown error during write.",
        "ts": time.strftime("%Y-%m-%dT%H:%M:%S")
    }
    print(response_payload,"response_payload")

    if not client.connected:
        response_payload["message"] = "Modbus client not connected."
        if aio_producer:
            await aio_producer.send_and_wait(KAFKA_TOPIC_WRITE_RESPONSES, value=response_payload)
        print(f"Warning: Modbus client not connected when trying to write to section '{section}'.")
        return

    config_data = read_json_file("register_map.json")
    if not config_data:
        response_payload["message"] = "Could not read register map for writing."
        if aio_producer:
            await aio_producer.send_and_wait(KAFKA_TOPIC_WRITE_RESPONSES, value=response_payload)
        print("Error: Could not read register map for writing.")
        return
    
    
    section_data = config_data.get(section, {})
    if not section_data:
        response_payload["message"] = f"Section '{section}' not found in register map for writing."
        if aio_producer:
            await aio_producer.send_and_wait(KAFKA_TOPIC_WRITE_RESPONSES, value=response_payload)
        print(f"Error: Section '{section}' not found in register map for writing.")
        return

    all_writes_successful = True

# ─── for decimal address────── 

    for name, val in tags.items():
        raw_cfg = section_data.get("write", {}).get(name)
        if raw_cfg is None:
            all_writes_successful = False
            response_payload["message"] = f"Tag '{name}' not found in write section."
            print(response_payload["message"])
            break

        # 1) Normalize address config into (register, bit_index)
        if isinstance(raw_cfg, (list, tuple)):
            if len(raw_cfg) == 2:
                register, bit_index = raw_cfg
            elif len(raw_cfg) == 1:
                register, bit_index = raw_cfg[0], None
            else:
                all_writes_successful = False
                response_payload["message"] = f"Invalid write config for '{name}': {raw_cfg}"
                print(response_payload["message"])
                break
        elif isinstance(raw_cfg, str) and "." in raw_cfg:
            reg_str, bit_str = raw_cfg.split(".", 1)
            register = int(reg_str)
            bit_index = int(bit_str)
        else:
            # assume single int or numeric string
            register = int(raw_cfg)
            bit_index = None

        # 2) Perform the write
        try:
            if bit_index is not None:
                # read-modify-write single bit
                rr = await client.read_holding_registers(register, count=1)
                if rr.isError() or not rr.registers:
                    raise ModbusException(f"Read failed at {register}: {rr}")
                current = rr.registers[0]
                target = int(val)
                if target not in (0, 1):
                    raise ValueError(f"Invalid bit value {val} for '{name}'; must be 0 or 1.")

                new = (current | (1 << bit_index)) if target else (current & ~(1 << bit_index))
                wr = await client.write_register(register, new)
                if wr.isError():
                    raise ModbusException(f"Write failed at {register}: {wr}")

                print(f"[BIT WRITE] {name}: reg={register}, bit={bit_index} → {target}")

            else:
                # full-register write
                wr = await client.write_register(register, int(val))
                if wr.isError():
                    raise ModbusException(f"Write failed at {register}: {wr}")

                print(f"[FULL WRITE] {name}: reg={register} → {val}")

        except (ModbusException, ValueError) as e:
            all_writes_successful = False
            response_payload["message"] = str(e)
            print(f"[WRITE ERROR] {e}")
            break

    if all_writes_successful:
        response_payload["status"] = "SUCCESS"
        response_payload["message"] = "All tags written successfully."
    
    if aio_producer:
        await aio_producer.send_and_wait(KAFKA_TOPIC_WRITE_RESPONSES, value=response_payload)
    else:
        print("Error: AIOKafkaProducer not initialized, cannot send write response.")


def decode_string(words):
    """Convert list of 16-bit words into ASCII string."""
    if not isinstance(words, (list, tuple)):
        logging.warning(f"Warning: decode_string received non-list/tuple input: {words}")
        return ""
    
    raw_bytes = b''.join([(w & 0xFF).to_bytes(1, 'little') + ((w >> 8) & 0xFF).to_bytes(1, 'little') for w in words])
    return raw_bytes.decode("ascii", errors="ignore").rstrip("\x00")

# ─── Main PLC Data Reading and Publishing Loops ──────────────────────────────────

async def read_specific_plc_data(client: AsyncModbusTcpClient):
    """
    1) For each station in register_map.json:
       - Read its status_1 and status_2 bits
       - If bit==1: read the corresponding barcode block, clear the bit, emit trigger_events
       - Always include both bits + last-read barcodes in the final grouped machine_status
    2) Publish one JSON of all stations to MACHINE_STATUS_TOPIC
    """
    cfg    = read_json_file("register_map.json")
    stations = cfg.get("stations", {})

    while True:
        if not client.connected:
            logging.warning("❌ PLC not connected—reconnecting…")
            try:
                await client.connect()
                if not client.connected:
                    await asyncio.sleep(5)
                    continue
            except Exception as e:
                logging.error(f"PLC reconnect error: {e}")
                await asyncio.sleep(5)
                continue

        now = time.strftime("%Y-%m-%dT%H:%M:%S")

        # fetch current operator if available
        try:
            resp = requests.get(f"{USER_LOGIN_URL}/api/current_operator", timeout=1)
            resp.raise_for_status()
            operator = resp.json().get("username")
        except:
            operator = None

        machine_payload: Dict[str, dict] = {}

        for name, spec in stations.items():
            # unpack registers/bits
            r1, b1 = spec["status_1"]
            r2, b2 = spec["status_2"]
            addr1, cnt1 = spec["barcode_block_1"]
            addr2, cnt2 = spec["barcode_block_2"]

            async def read_bit(reg, bit):
                rr = await client.read_holding_registers(reg, 1)
                if rr.isError() or not rr.registers:
                    return 0
                return (rr.registers[0] >> bit) & 1

            # 1) read both bits
            flag1 = await read_bit(r1, b1)
            flag2 = await read_bit(r2, b2)

            bc1 = None
            if flag1:
                # 2) read barcode1, clear bit, emit trigger
                rr = await client.read_holding_registers(addr1, cnt1)
                if not rr.isError() and rr.registers:
                    bc1 = decode_string(rr.registers)
                    # clear the bit
                    old = (await client.read_holding_registers(r1,1)).registers[0]
                    new = old & ~(1 << b1)
                    await client.write_register(r1, new)
                    # send trigger event for this station
                    await aio_producer.send_and_wait(
                        KAFKA_TOPIC_BARCODE,
                        {
                          "station":   name,
                          "barcode":   bc1,
                          "flag":      1,
                          "ts":        now,
                          "operator":  operator
                        }
                    )

            bc2 = None
            if flag2:
                rr = await client.read_holding_registers(addr2, cnt2)
                if not rr.isError() and rr.registers:
                    bc2 = decode_string(rr.registers)
                    # clear the bit
                    old = (await client.read_holding_registers(r2,1)).registers[0]
                    new = old & ~(1 << b2)
                    await client.write_register(r2, new)
                    await aio_producer.send_and_wait(
                        KAFKA_TOPIC_BARCODE,
                        {
                          "station":   name,
                          "barcode":   bc2,
                          "flag":      2,
                          "ts":        now,
                          "operator":  operator
                        }
                    )

            # 3) stash into the big payload
            machine_payload[name] = {
                "status_1": flag1,
                "status_2": flag2,
                "barcode1": bc1,
                "barcode2": bc2,
                "operator": operator,
                "ts":       now
            }

        # 4) publish the grouped machine_status
        if aio_producer:
            await aio_producer.send_and_wait(
                KAFKA_TOPIC_MACHINE_STATUS,
                value=machine_payload
            )
            logging.info(f"Published machine_status for {len(stations)} stations")
        else:
            logging.error("Kafka producer not ready; dropped machine_status")

        await asyncio.sleep(2)
async def read_and_publish_per_section_loop(client: AsyncModbusTcpClient, interval_seconds=5):
    """
    Periodically reads tags from each configured section and publishes them
    to their respective Kafka topics.
    """
    topic_map = {
        "startup": KAFKA_TOPIC_STARTUP_STATUS,
        "auto": KAFKA_TOPIC_AUTO_STATUS,
        "io": KAFKA_TOPIC_IO_STATUS,
        "robo": KAFKA_TOPIC_ROBO_STATUS,
        "manual": KAFKA_TOPIC_MANUAL_STATUS,
        "oee": KAFKA_TOPIC_OEE_STATUS,
    }

    while True:
        if not client.connected:
            print("Warning: Client not connected for generic tags. Skipping this cycle.")
            await asyncio.sleep(interval_seconds)
            continue
            
        try:
            now = time.strftime("%Y-%m-%dT%H:%M:%S")
            
            for section, topic in topic_map.items():
                section_data = await read_tags_async(client, section)
                
                if aio_producer and section_data:
                    # Add timestamp to the data payload for each section
                    section_data["ts"] = now
                    await aio_producer.send(topic, value=section_data)
                    print(f"[{now}] Sent '{section}' tags to Kafka topic '{topic}'.")
                else:
                    pass # Or print a message if no data or producer not ready

        except Exception as e:
            print(f"[{now}] Error in per-section publishing loop: {e}")

        await asyncio.sleep(interval_seconds)


async def kafka_write_consumer_loop(client: AsyncModbusTcpClient):
    """
    AIOKafkaConsumer loop that listens for write commands and executes them on the PLC.
    This runs entirely asynchronously.
    """
    print("cominginsidekafka_write_consumer_loop")
    consumer = None
    print(f"Starting AIOKafkaConsumer for write commands on topic: {KAFKA_TOPIC_WRITE_COMMANDS}")
    for attempt in range(10):
        try:
            consumer = AIOKafkaConsumer(
                KAFKA_TOPIC_WRITE_COMMANDS,
                bootstrap_servers=KAFKA_BROKER,
                group_id='plc-write-gateway-group',
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
                if aio_producer:
                    await aio_producer.send(KAFKA_TOPIC_WRITE_RESPONSES, value={
                        "request_id": request_id,
                        "status": "FAILED",
                        "message": "Invalid command format. Requires 'section', 'tag_name', 'value'.",
                        "original_command": command,
                        "ts": now
                    })
                continue
            
            try:
                tags_to_write = {tag_name: value}
                
                await async_write_tags(client, section, tags_to_write, request_id)
                
                print(f"[{now}] Completed processing write command for request_id: {request_id}")
                
            except asyncio.TimeoutError:
                print(f"[{now}] Timeout while writing to PLC for command (request_id: {request_id}): {command}")
                if aio_producer:
                    await aio_producer.send(KAFKA_TOPIC_WRITE_RESPONSES, value={
                        "request_id": request_id,
                        "status": "TIMEOUT",
                        "message": "PLC write operation timed out.",
                        "original_command": command,
                        "ts": now
                    })
            except Exception as e:
                print(f"[{now}] Error executing write command (request_id: {request_id}) {command}: {e}")
                if aio_producer:
                    await aio_producer.send(KAFKA_TOPIC_WRITE_RESPONSES, value={
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



# ─── Login status ──────────────────────────────────────────────────────────────

async def listen_for_login_status(client: AsyncModbusTcpClient):
    """
    Consume login_status from Kafka and write to PLC register 3309:
      status=1 → user logged in
      status=0 → auto‐logout
      status=2 → manual logout
    """
    consumer = AIOKafkaConsumer(
        LOGIN_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id="login-status-listener",
        value_deserializer=lambda b: json.loads(b.decode("utf-8"))
    )
    await consumer.start()
    print(f"[LoginListener] consuming topic {LOGIN_TOPIC}")
    try:
        async for msg in consumer:
            payload = msg.value
            status  = int(payload.get("status", 0))
            print(f"[LoginListener] got status={status}, writing to PLC reg {LOGIN_REGISTER}")
            # write_register returns a Deferred—await it
            wr = await client.write_register(LOGIN_REGISTER, status)
            if wr.isError():
                print(f"[LoginListener] Write error: {wr}")
    finally:
        await consumer.stop()
        print("[LoginListener] stopped")


# ─── Main ──────────────────────────────────────────────────────────────
async def main():
    """
    Main function to establish a single Modbus client connection
    and start all asynchronous tasks.
    """
    if USE_SIMULATOR:
        client = None
        logger.warning("🔧 SIMULATOR MODE ENABLED — skipping real PLC connect")
    else:     
        client = AsyncModbusTcpClient(host=PLC_HOST, port=PLC_PORT)

        logger.info(f"Connecting to PLC at {PLC_HOST}:{PLC_PORT} for all tasks...")
        await client.connect()
        if not client.connected:
            print("❌ Initial connection to PLC failed. Exiting.")
            return

        logger.info(f"✅ Connected to PLC at {PLC_HOST}:{PLC_PORT}")

    try:
        await init_aiokafka_producer()

        if not USE_SIMULATOR:
            # client is your AsyncModbusTcpClient instance
            asyncio.create_task(listen_for_login_status(client))

        if USE_SIMULATOR:
            # fire up simulators instead of real PLC loops
            asyncio.create_task(simulate_plc_data())
            asyncio.create_task(simulate_plc_write_responses())
        else:
            # Specific PLC data (barcodes, 13-bit machine status)
            asyncio.create_task(read_specific_plc_data(client))
            
            # Generic tags (now per-section topics)
            asyncio.create_task(read_and_publish_per_section_loop(client, interval_seconds=5))
            
            # Kafka consumer for PLC write commands
            asyncio.create_task(kafka_write_consumer_loop(client))

        await asyncio.Future()

    except Exception as e:
        logger.exception(f"Unexpected error in main loop: {e}")
    finally:
        if client.connected:
            logger.info("Closing Modbus client connection.")
            client.close()
        if aio_producer:
            await aio_producer.stop()
            logger.info("AIOKafkaProducer closed.")


# ─── Main ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    try:
        logger.info("Starting PLC Machine Interface Service...")
        if os.name == 'nt':
            import asyncio
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Server stopped by user.")
    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}")