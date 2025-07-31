import os,json,time,asyncio,logging,requests,struct,re
from typing import Dict
from collections import deque
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



# In-memory tracking of all “in-flight” barcode-sets:
active_sets: list = []
pending_load = { 'bcA': None, 'bcB': None }


# Simple guard to only spawn one new set per rising edge of the two global flags
_load_seen = False

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
KAFKA_TOPIC_ALARM_STATUS = os.getenv("ALARM_STATUS_TOPIC", "alarm_status")
KAFKA_TOPIC_OEE_STATUS = os.getenv("OEE_STATUS_TOPIC", "oee_status") 
KAFKA_TOPIC_ON_OFF_STATUS = os.getenv("ON_OFF_TOPIC", "on_off_status")

# Barcode related registers (No change)
BARCODE_FLAG_1 = 3303
BARCODE_FLAG_2 = 3304

LOGIN_REGISTER   = int(os.getenv("LOGIN_REGISTER", "1960"))


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
        await asyncio.sleep(0.5)
    raise RuntimeError("Kafka producer failed after 10 attempts")



# Simulation of PLC
async def simulate_plc_data():
    """
    Periodically send properly structured mock data to match what
    consume_machine_status_and_populate_db expects.
    """
    print("🔧 Starting PLC data simulator with barcode sets…")
    
    # Generate a new test barcode every few cycles
    cycle_counter = 0
    active_sets = []
    
    while True:
        now = time.strftime("%Y-%m-%dT%H:%M:%S")
        
        # Every 5 cycles, create a new simulated set with barcodes
        if cycle_counter % 5 == 0:
            test_barcode1 = f"SIM-{int(time.time())}"
            test_barcode2 = f"B{random.randint(10000, 99999)}"
            
            new_set = {
                "set_id": f"{test_barcode1}|{test_barcode2}",
                "barcodes": [test_barcode1, test_barcode2],
                "progress": {
                    "loading_station": {"status_1": 1, "status_2": 0, "ts": now},
                    "trace_station": {"status_1": 0, "status_2": 0, "ts": now},
                    "process_station": {"status_1": 0, "status_2": 0, "ts": now},
                    "unload_station": {"status_1": 0, "status_2": 0, "ts": now}
                },
                "created_ts": now
            }
            active_sets.append(new_set)
            print(f"Created simulated set: {new_set['set_id']}")
        
        # Update progress on existing sets
        for s in active_sets:
            # Simulate progress through stations
            stage = cycle_counter % 4  # 0=loading, 1=trace, 2=process, 3=unload
            
            if stage == 0:
                s["progress"]["loading_station"]["status_1"] = 1
            elif stage == 1:
                s["progress"]["loading_station"]["status_1"] = 0
                s["progress"]["trace_station"]["status_1"] = 1
            elif stage == 2:
                s["progress"]["trace_station"]["status_1"] = 0
                s["progress"]["process_station"]["status_1"] = 1
            elif stage == 3:
                s["progress"]["process_station"]["status_1"] = 0
                s["progress"]["unload_station"]["status_1"] = 1
            
            # Update timestamps
            for station in s["progress"]:
                s["progress"][station]["ts"] = now
        
        # Remove completed sets
        active_sets = [s for s in active_sets 
                      if s["progress"]["unload_station"]["status_1"] != 1]
        
        # Proper payload format for dashboard_api
        payload = {"sets": active_sets, "ts": now}
        
        # Send to the same topic as the real code
        await aio_producer.send_and_wait(KAFKA_TOPIC_MACHINE_STATUS, value=payload)
        print(f"[{now}] Sent {len(active_sets)} simulated sets to {KAFKA_TOPIC_MACHINE_STATUS}")
        
        # Also send section-specific data like before
        await aio_producer.send_and_wait(KAFKA_TOPIC_STARTUP_STATUS, value={"status":"OK","ts":now})
        
        cycle_counter += 1
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

REGISTER_MAP = read_json_file("register_map.json")

async def read_float_async(client, base_address, reverse=False, unit=1):
    # Read two 16-bit registers (32 bits)
    result = await client.read_holding_registers(address=base_address, count=2, slave=unit)
    if result.isError():
        raise Exception(f"Modbus read error: {result}")
    regs = result.registers
    # print(f"Raw registers: {regs}")

    if reverse:
        regs = [regs[1], regs[0]]

    raw_bytes = struct.pack('>HH', *regs)
    return struct.unpack('>f', raw_bytes)[0]

async def read_tags_async(client: AsyncModbusTcpClient, section: str):
    """
    Reads tags from a specified section in the register map.
    Handles both single-word (Int, Boolean) and multi-word (String) reads.
    """
    out = {}
    if not client.connected:
        logging.info(f"Warning: Modbus client not connected when trying to read section '{section}'.")
        return out

    config_data = REGISTER_MAP
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
        dataType = None
        reverse  = False
        if isinstance(cfg, dict):
            addr     = cfg["address"]
            typ      = cfg.get("type","holding")
            count    = cfg.get("count", 1)
            dataType = cfg.get("datatype", None)
            reverse  = cfg.get("reverse", False)
        elif isinstance(cfg, list) and len(cfg)==2:
            addr, count = cfg
            typ         = "holding"
        elif isinstance(cfg, int):
            addr, count, typ = cfg, 1, "holding"
        else:
            logger.warning(f"Skipping bad config for {name}: {cfg}")
            continue

        try:
            # 1) Coils
            if typ == "coil":
                rr = await client.read_coils(address=addr, count=count, slave=1)
                val = rr.bits[0] if not rr.isError() and rr.bits else None

            # 2) Explicit floats (32-bit)
            elif dataType == "float" and count == 2:
                val = await read_float_async(client, addr, reverse=reverse, unit=1)
            # 3) Everything else is a standard 1- or N-word register
            else:
                rr = await client.read_holding_registers(address=addr, count=count, slave=1)
                logger.debug(f"Raw registers for {name}@{addr}: {rr.registers}")
                if rr.isError() or not rr.registers:
                    val = None
                elif count == 1:
                    val = rr.registers[0]
                else:
                    val = rr.registers

            out[name] = val
            logger.debug(f"→ {name} = {val}")





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

    config_data = REGISTER_MAP
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
                rr = await client.read_holding_registers(address=register, count=1)
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

# ─
def sanitize_topic_name(topic_base, identifier):
    """
    Sanitizes a string to be used as part of a Kafka topic name.
    - Replaces invalid characters with '_'
    - Truncates if too long
    - Ensures the final topic name is valid
    """
    # Step 1: Replace invalid characters with underscore
    sanitized = re.sub(r'[^a-zA-Z0-9\._\-]', '_', identifier)
    
    # Step 2: Ensure topic isn't too long (max Kafka topic length is 249)
    max_id_length = 20  # Reserve space for the base name and separator
    if len(sanitized) > max_id_length:
        sanitized = sanitized[:max_id_length]
    
    # Step 3: Create full topic name
    full_topic = f"{topic_base}.{sanitized}"
    
    return full_topic


    


async def read_specific_plc_data(client: AsyncModbusTcpClient):
    """
    Advanced workpiece tracking system that:
    1) Detects new sets based on station status transitions
    2) Tracks up to 3 concurrent sets through the process
    3) Reads each station's associated barcodes
    4) Publishes real-time updates for status changes
    5) Maintains proper barcode-status association
    """
    global active_sets, pending_load, aio_producer # Make sure aio_producer is global if initialized elsewhere
    PROCESS_STATIONS = [
        "loading_station", "xbot_1", "vision_1", "gantry_1",
        "xbot_2", "vision_2", "gantry_2", "vision_3", "unload_station"
    ]

    cfg = REGISTER_MAP
    stations = cfg.get("stations", {})

    # Keep track of previous station statuses for edge detection
    previous_station_statuses = {}
    for station_name in stations.keys():
        previous_station_statuses[station_name] = {"status_1": 0, "status_2": 0}

    # Pre-calculate all register addresses for efficient batch reading
    all_registers = []
    for name, spec in stations.items():
        if "status_1" in spec:
            reg1, _ = spec["status_1"]
            all_registers.append(reg1)
        if "status_2" in spec: # Handle stations with two status bits
            reg2, _ = spec["status_2"]
            all_registers.append(reg2)
    if not all_registers:
        logger.error("No status registers defined in the register map.")
        return
    # Calculate optimal read range for status registers
    min_reg = min(all_registers) if all_registers else 0
    max_reg = max(all_registers) if all_registers else 0
    status_count = max_reg - min_reg + 1

    logger.info(f"Status register range: {min_reg}-{max_reg}, count: {status_count}")

    # Set to keep track of set_ids we've seen and are currently tracking
    seen_set_ids = set()
    for s in active_sets: # Initialize with any existing active sets
        seen_set_ids.add(s["set_id"])

    # Main processing loop
    while True:
        if not client.connected:
            logger.warning("❌ PLC not connected—reconnecting…")
            try:
                await client.connect()
            except Exception as e:
                logger.error(f"Connection error: {e}")
                await asyncio.sleep(0.5)
                continue

        try:
            now = time.strftime("%Y-%m-%dT%H:%M:%S")

            # 1. READ ALL STATUS REGISTERS IN ONE BATCH
            rr_status = await client.read_holding_registers(address=min_reg, count=status_count)
            if rr_status.isError():
                logger.error(f"Failed to read status registers: {rr_status}")
                await asyncio.sleep(0.05) # Keep the sleep to avoid hammering PLC on error
                continue

            # 2. EXTRACT STATION STATUSES AND DETECT CHANGES
            current_station_statuses = {}
            station_changes_detected = {} # Only store stations with actual changes

            for name, spec in stations.items():
                v1, v2 = 0, 0 # Default values if status bits aren't defined
                reg1, bit1 = spec.get("status_1", (None, None))
                reg2, bit2 = spec.get("status_2", (None, None))

                if reg1 is not None:
                    idx1 = reg1 - min_reg
                    if 0 <= idx1 < len(rr_status.registers):
                        v1 = (rr_status.registers[idx1] >> bit1) & 1

                if reg2 is not None:
                    idx2 = reg2 - min_reg
                    if 0 <= idx2 < len(rr_status.registers):
                        v2 = (rr_status.registers[idx2] >> bit2) & 1

                current_station_statuses[name] = {"status_1": v1, "status_2": v2, "ts": now}

                prev_v1 = previous_station_statuses[name]["status_1"]
                prev_v2 = previous_station_statuses[name]["status_2"]

                if v1 != prev_v1 or v2 != prev_v2:
                    station_changes_detected[name] = {
                        "status_1": {"old": prev_v1, "new": v1},
                        "status_2": {"old": prev_v2, "new": v2}
                    }

                # Update previous values for next cycle
                previous_station_statuses[name]["status_1"] = v1
                previous_station_statuses[name]["status_2"] = v2

            # 3. PROCESS STATION STATUS CHANGES AND READ BARCODES
            any_set_updated = False

            for station_name, changes in station_changes_detected.items():
                station_spec = stations[station_name]
                barcode_1_read = None
                barcode_2_read = None

                # Process status_1 change (e.g., workpiece A arrives)
                if changes["status_1"].get("old") == 0 and changes["status_1"].get("new") == 1:
                    logger.info(f"⬆️ Rising edge detected for {station_name} status_1.")
                    if "barcode_block_1" in station_spec:
                        bc1_start, bc1_count = station_spec["barcode_block_1"]
                        rr_bc1 = await client.read_holding_registers(address=bc1_start, count=bc1_count)
                        if not rr_bc1.isError():
                            barcode_1_read = decode_string(rr_bc1.registers)
                            logger.info(f"🔢 Read barcode 1 '{barcode_1_read}' at {station_name}.")
                        else:
                            logger.error(f"Failed to read barcode 1 for {station_name}: {rr_bc1}")

                    # Immediately clear the PLC bit for status_1
                    reg1, bit1 = station_spec["status_1"]
                    rr_clear = await client.read_holding_registers(address=reg1, count=1)
                    if not rr_clear.isError() and rr_clear.registers:
                        current = rr_clear.registers[0]
                        new = current & ~(1 << bit1)  # Clear the bit
                        await client.write_register(reg1, new)
                        logger.debug(f"Cleared {station_name} status_1 bit.")
                    else:
                        logger.error(f"Failed to read/clear {station_name} status_1 bit: {rr_clear}")

                # Process status_2 change (e.g., workpiece B arrives for stations with two parts)
                if changes["status_2"].get("old") == 0 and changes["status_2"].get("new") == 1:
                    logger.info(f"⬆️ Rising edge detected for {station_name} status_2.")
                    if "barcode_block_2" in station_spec:
                        bc2_start, bc2_count = station_spec["barcode_block_2"]
                        rr_bc2 = await client.read_holding_registers(address=bc2_start, count=bc2_count)
                        if not rr_bc2.isError():
                            barcode_2_read = decode_string(rr_bc2.registers)
                            logger.info(f"🔢 Read barcode 2 '{barcode_2_read}' at {station_name}.")
                        else:
                            logger.error(f"Failed to read barcode 2 for {station_name}: {rr_bc2}")

                    # Immediately clear the PLC bit for status_2
                    reg2, bit2 = station_spec["status_2"]
                    rr_clear = await client.read_holding_registers(address=reg2, count=1)
                    if not rr_clear.isError() and rr_clear.registers:
                        current = rr_clear.registers[0]
                        new = current & ~(1 << bit2)  # Clear the bit
                        await client.write_register(reg2, new)
                        logger.debug(f"Cleared {station_name} status_2 bit.")
                    else:
                        logger.error(f"Failed to read/clear {station_name} status_2 bit: {rr_clear}")

                # --- Handle Loading Station Special Case for Set Creation ---
                if station_name == "loading_station":
                    if barcode_1_read:
                        pending_load['bcA'] = barcode_1_read
                    if barcode_2_read:
                        pending_load['bcB'] = barcode_2_read

                    if pending_load['bcA'] and pending_load['bcB']:
                        # Create a combined set_id for the pair
                        full_set_id = f"{pending_load['bcA']}|{pending_load['bcB']}"
                        if full_set_id not in seen_set_ids:
                            new_set = {
                                "set_id": full_set_id,
                                "barcodes": [pending_load['bcA'], pending_load['bcB']],
                                "progress": {
                                    "loading_station": {
                                        "status_1": 1, "status_2": 1, "ts": now, "latched": True,
                                        "barcode_1": pending_load['bcA'], "barcode_2": pending_load['bcB']
                                    }
                                },
                                "created_ts": now,
                                "last_update": now,
                                "current_station": "loading_station"
                            }
                            active_sets.append(new_set)
                            seen_set_ids.add(full_set_id)
                            logger.info(f"📦 New set created: {full_set_id} at loading_station.")
                            any_set_updated = True
                        else:
                            logger.debug(f"Skipping duplicate set creation for {full_set_id} at loading_station.")
                        # Clear pending_load after processing
                        pending_load['bcA'] = None
                        pending_load['bcB'] = None

                # --- Update existing active_sets for other stations ---
                else: # For all stations other than loading_station
                    if barcode_1_read or barcode_2_read:
                        found_set = None
                        # Try to find an active set by the barcode(s)
                        for current_set in active_sets:
                            if (barcode_1_read and barcode_1_read in current_set["barcodes"]) or \
                               (barcode_2_read and barcode_2_read in current_set["barcodes"]):
                                found_set = current_set
                                break

                        if found_set:
                            # Update the found set's progress and current station
                            station_progress = found_set["progress"].setdefault(station_name, {})
                            if barcode_1_read and not station_progress.get("barcode_1"):
                                station_progress["status_1"] = 1
                                station_progress["barcode_1"] = barcode_1_read
                                station_progress["ts"] = now
                                station_progress["latched"] = True # Mark as latched
                                logger.debug(f"Updated set {found_set['set_id']} with barcode 1 at {station_name}")
                                any_set_updated = True
                            if barcode_2_read and not station_progress.get("barcode_2"):
                                station_progress["status_2"] = 1
                                station_progress["barcode_2"] = barcode_2_read
                                station_progress["ts_2"] = now
                                station_progress["latched_2"] = True # Mark as latched
                                logger.debug(f"Updated set {found_set['set_id']} with barcode 2 at {station_name}")
                                any_set_updated = True

                            # Update current_station for the set
                            # Prioritize the most "forward" station in the process
                            current_station_index = PROCESS_STATIONS.index(station_name)
                            if (found_set["current_station"] is None or
                                PROCESS_STATIONS.index(found_set["current_station"]) < current_station_index):
                                found_set["current_station"] = station_name
                                logger.info(f"Set {found_set['set_id']} moved to {station_name}.")
                                any_set_updated = True

                            found_set["last_update"] = now
                        else:
                            logger.warning(f"⚠️ Barcode(s) '{barcode_1_read or ''}' '{barcode_2_read or ''}' at {station_name} not associated with any active set. (Might be an unexpected entry or late detection)")

            # 4. PUBLISH REAL-TIME UPDATES FOR INDIVIDUAL SET CHANGES (if any)
            if any_set_updated and aio_producer:
                for current_set in active_sets:
                    set_payload = {
                        "set": current_set,
                        "type": "set_update",
                        "ts": now
                    }
                    logger.info(f"Publishing update for set {current_set['set_id']}")
                    sanitized_topic = sanitize_topic_name(f"{KAFKA_TOPIC_MACHINE_STATUS}.set", current_set['set_id'])
                    # Pass dict directly, serializer handles encoding
                    await aio_producer.send(sanitized_topic, value=set_payload)

                    # Pass dict directly
                    await aio_producer.send(KAFKA_TOPIC_MACHINE_STATUS, value={
                        "type": "set_update",
                        "set_id": current_set["set_id"],
                        "current_station": current_set.get("current_station"),
                        "ts": now
                    })

                # 5. PUBLISH ALL SETS PERIODICALLY (BATCH UPDATE)
                full_payload = {
                    "sets": active_sets,
                    "type": "full_update",
                    "ts": now
                }
                # Pass dict directly, serializer handles encoding
                await aio_producer.send_and_wait(KAFKA_TOPIC_MACHINE_STATUS, value=full_payload)
                logger.info(f"Published full update with {len(active_sets)} active sets")

            # 6. RETIRE COMPLETED SETS (unload_station status_1 = 1 OR status_2 = 1)
            before_count = len(active_sets)
            sets_to_remove = []
            for st in active_sets:
                unload_station_progress = st["progress"].get("unload_station", {})
                # A set is complete if EITHER barcode A or B has reached unload station
                if unload_station_progress.get("status_1") == 1 or unload_station_progress.get("status_2") == 1:
                    sets_to_remove.append(st["set_id"])

            if sets_to_remove:
                # Clear unload station bits AFTER identifying which sets to remove
                unload_spec = stations["unload_station"]
                reg1, bit1 = unload_spec.get("status_1", (None, None))
                reg2, bit2 = unload_spec.get("status_2", (None, None))

                # Clear status_1 if set
                if reg1 is not None:
                    rr1 = await client.read_holding_registers(address=reg1, count=1)
                    if not rr1.isError() and rr1.registers and ((rr1.registers[0] >> bit1) & 1):
                        current = rr1.registers[0]
                        new = current & ~(1 << bit1)
                        await client.write_register(reg1, new)
                        logger.debug("Cleared unload_station status_1 bit.")

                # Clear status_2 if set
                if reg2 is not None:
                    rr2 = await client.read_holding_registers(address=reg2, count=1)
                    if not rr2.isError() and rr2.registers and ((rr2.registers[0] >> bit2) & 1):
                        current = rr2.registers[0]
                        new = current & ~(1 << bit2)
                        await client.write_register(reg2, new)
                        logger.debug("Cleared unload_station status_2 bit.")

                active_sets = [st for st in active_sets if st["set_id"] not in sets_to_remove]
                for removed_set_id in sets_to_remove:
                    if removed_set_id in seen_set_ids:
                        seen_set_ids.remove(removed_set_id)
                logger.info(f"Retired {len(sets_to_remove)} completed sets: {sets_to_remove}")
                any_set_updated = True # Indicate a change for full update

        except Exception as e:
            logger.error(f"Error in read_specific_plc_data: {e}", exc_info=True)

        await asyncio.sleep(0.05) # 10Hz update rate



async def read_and_publish_per_section_loop(client: AsyncModbusTcpClient, interval_seconds=0.5):
    """
    Periodically reads tags from each configured section and publishes them
    to their respective Kafka topics.
    """
    topic_map = {
        "startup": KAFKA_TOPIC_STARTUP_STATUS,
        "auto": KAFKA_TOPIC_AUTO_STATUS,
        "io": KAFKA_TOPIC_IO_STATUS,
        "alarm": KAFKA_TOPIC_ALARM_STATUS,
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

                    if section == "alarm":
                        logger.info(f"Publishing alarm data to Kafka topic: {topic}")
                    #         alarm_time = now.split("T")[1]
                    #         alarm_date = now.split("T")[0]

                    #         for tag, is_active in section_data.items():
                    #             if tag == "ts" or not is_active:
                    #                 continue  # Skip non-alarms

                    #             # Use executor to run sync DB insert
                    #             await asyncio.get_event_loop().run_in_executor(
                    #                 None, insert_alarm_if_active, cur, alarm_date, alarm_time, tag
                    #             )

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
            await asyncio.sleep(0.5)
        except Exception as e:
            print(f"[AIOKafka Consumer] Error starting consumer: {e}. Retrying ({attempt + 1}/10)...")
            await asyncio.sleep(0.5)
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
    Consume login commands from the PLC_WRITE_COMMANDS topic and write to PLC register 3309:
      value=1 → user logged in
      value=0 → auto‐logout
      value=2 → manual logout
    """
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC_WRITE_COMMANDS,  # Change from LOGIN_TOPIC to KAFKA_TOPIC_WRITE_COMMANDS
        bootstrap_servers=KAFKA_BROKER,
        group_id="login-status-listener",
        value_deserializer=lambda b: json.loads(b.decode("utf-8"))
    )
    await consumer.start()
    logger.info(f"[LoginListener] consuming topic {KAFKA_TOPIC_WRITE_COMMANDS}")
    
    try:
        async for msg in consumer:
            command = msg.value
            section = command.get("section")
            tag_name = command.get("tag_name")
            
            # Only process login-related commands
            if section == "login" and tag_name == "login":
                value = command.get("value")
                request_id = command.get("request_id")
                
                if value is not None:
                    status = int(value)
                    logger.info(f"[LoginListener] got login status={status}, writing to PLC reg {LOGIN_REGISTER}")
                    
                    # Write to PLC register
                    wr = await client.write_register(LOGIN_REGISTER, status)
                    
                    # Send response
                    response = {
                        "request_id": request_id,
                        "status": "SUCCESS" if not wr.isError() else "FAILED",
                        "message": "Login status written successfully" if not wr.isError() else f"Login write error: {wr}",
                        "original_command": command,
                        "ts": time.strftime("%Y-%m-%dT%H:%M:%S")
                    }
                    
                    # Send response back to Kafka
                    if aio_producer:
                        await aio_producer.send(KAFKA_TOPIC_WRITE_RESPONSES, value=response)
                        
                    if wr.isError():
                        logger.error(f"[LoginListener] Write error: {wr}")
                    else:
                        logger.info(f"[LoginListener] Successfully wrote status {status} to register {LOGIN_REGISTER}")
    except Exception as e:
        logger.error(f"[LoginListener] Error: {e}")
    finally:
        await consumer.stop()
        logger.info("[LoginListener] stopped")


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
            logger.error("❌ Initial connection to PLC failed. Exiting.")
            return

        logger.info(f"✅ Connected to PLC at {PLC_HOST}:{PLC_PORT}")

    try:
        await init_aiokafka_producer()

        if not USE_SIMULATOR:
            # Login status listener
            asyncio.create_task(listen_for_login_status(client))

        if USE_SIMULATOR:
            # Simulators for testing
            asyncio.create_task(simulate_plc_data())
            asyncio.create_task(simulate_plc_write_responses())
        else:
            # Real PLC data reading and publishing
            asyncio.create_task(read_specific_plc_data(client))
            asyncio.create_task(read_and_publish_per_section_loop(client, interval_seconds=5))
            asyncio.create_task(kafka_write_consumer_loop(client))

        # Keep running
        await asyncio.Future()

    except Exception as e:
        logger.exception(f"Unexpected error in main loop: {e}")
    finally:
        if client and client.connected:
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