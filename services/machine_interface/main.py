import os,json,time,asyncio,logging,requests,struct,re
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

# In-memory tracking of all “in-flight” barcode-sets:
active_sets: list = []

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
KAFKA_TOPIC_OEE_STATUS = os.getenv("OEE_STATUS_TOPIC", "oee_status") # New topic for OEE data
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
                val = rr.bits[0] if not rr.isError() and rr.bits else None
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

# ─── Main PLC Data Reading and Publishing Loops ──────────────────────────────────

# async def read_specific_plc_data(client: AsyncModbusTcpClient):
#     """
#     1) Watch the two global barcode flags (3303, 3304). When they BOTH
#        go from 0→1, read the loading_station’s two barcode blocks,
#        form a new set, and append to active_sets.
#     2) Every cycle, read status_1/2 for _every_ station and attach to each
#        active set’s .progress[name] = {status_1, status_2, ts}.
#     3) Publish the full active_sets list as one payload.
#     4) Retire a set once its unload_station status_1 goes high.
#     """
#     global _load_seen, active_sets

#     cfg      = read_json_file("register_map.json")
#     stations = cfg.get("stations", {})
    
    
#     # Limit read size to reasonable chunk (Modbus protocol typically limits to 125 registers)
#     if read_count > 125:
#         logger.warning(f"Register range too large ({read_count}), using multiple batch reads")
#         use_multiple_batches = True
#     else:
#         use_multiple_batches = False
    
#     while True:
#         async def read_bit(reg, bit):
#             rr = await client.read_holding_registers(address=reg, count=1)
#             return 0 if rr.isError() or not rr.registers else (rr.registers[0] >> bit) & 1
#         # 0) ensure PLC connection
#         if not client.connected:
#             logger.warning("❌ PLC not connected—reconnecting…")
#             try:
#                 await client.connect()
#             except:
#                 await asyncio.sleep(2)
#                 continue
#             if not client.connected:
#                 await asyncio.sleep(2)
#                 continue

#         now = time.strftime("%Y-%m-%dT%H:%M:%S")

#         # 1) Read global barcode_flags
#         try:
#             rr1 = await client.read_holding_registers(address=BARCODE_FLAG_1, count=1)
#             rr2 = await client.read_holding_registers(address=BARCODE_FLAG_2, count=1)
#             flag1 = bool(rr1.registers[0]) if not rr1.isError() else False
#             flag2 = bool(rr2.registers[0]) if not rr2.isError() else False
#         except:
#             flag1 = flag2 = False

#         flag1 = flag2 = True
#         # 2) On rising edge of both flags → spawn a new set
#         if flag1 and flag2 and not _load_seen:
#             # read the two barcodes from your loading_station
#             ld = stations["loading_station"]
#             bcA = decode_string((await client.read_holding_registers(address=ld["barcode_block_1"][0], count=ld["barcode_block_1"][1])).registers)
#             print(bcA,"bcAbcA")
#             bcB = decode_string((await client.read_holding_registers(address=ld["barcode_block_2"][0], count=ld["barcode_block_2"][1])).registers)
#             set_id = f"{bcA}|{bcB}"
#             print(set_id,"setttt")
#             if bcA or bcB:  # At least one valid barcode
#                 set_id = f"{bcA}|{bcB}"
#                 print(f"Creating new set with ID: {set_id}")
#                 active_sets.append({
#                     "set_id":     set_id,
#                     "barcodes":   [bcA, bcB],
#                     "progress":   {},         
#                     "created_ts": now
#                 })
#             else:
#                 print("WARNING: Skipping set creation due to empty barcodes")
#         #     await client.write_register(BARCODE_FLAG_1, 0)
#         #     await client.write_register(BARCODE_FLAG_2, 0)
#         #     _load_seen = True

#         # # once both flags drop, allow next rising edge:
#         # if not (flag1 or flag2):
#             _load_seen = False

#         # 3) Read each station’s status_1/2 once per cycle
        

#         station_vals = {}
#         for name, spec in stations.items():
#             v1 = await read_bit(*spec["status_1"])
#             v2 = await read_bit(*spec["status_2"])
#             station_vals[name] = {"status_1": v1, "status_2": v2, "ts": now}

#         # 4) Attach those readings to every active set
#         for st in active_sets:
#             for name, vals in station_vals.items():
#                 st["progress"][name] = vals

#         # 5) Retire any set that’s done at unload_station
#         active_sets = [
#             st for st in active_sets
#             if st["progress"].get("unload_station", {}).get("status_1") != 1
#         ]

#         # 6) Publish the whole list in one shot
#         payload = {"sets": active_sets, "ts": now}
#         if aio_producer:
#             await aio_producer.send_and_wait(KAFKA_TOPIC_MACHINE_STATUS, value=payload)
#             logger.info(f"Published {len(active_sets)} active sets")
#         else:
#             logger.error("Kafka producer not ready; dropped machine_status")

#         await asyncio.sleep(1)

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
    global active_sets
    
    cfg = read_json_file("register_map.json")
    stations = cfg.get("stations", {})
    
    # Keep track of previous station statuses for edge detection
    previous_station_statuses = {}
    for station_name in stations.keys():
        previous_station_statuses[station_name] = {"status_1": 0, "status_2": 0}
    
    # Pre-calculate all register addresses for efficient batch reading
    all_registers = []
    for name, spec in stations.items():
        reg1, bit1 = spec["status_1"]
        reg2, bit2 = spec["status_2"]
        all_registers.extend([reg1, reg2])
    
    # Calculate optimal read range for status registers
    min_reg = min(all_registers)
    max_reg = max(all_registers)
    status_count = max_reg - min_reg + 1
    
    logger.info(f"Status register range: {min_reg}-{max_reg}, count: {status_count}")
    
    # Set to keep track of barcodes we've seen to avoid duplicates
    seen_barcodes = set()
    
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
                await asyncio.sleep(0.5)
                continue
            
            # 2. EXTRACT STATION STATUSES FROM BATCH DATA
            station_vals = {}
            station_changes = {}
            for name, spec in stations.items():
                reg1, bit1 = spec["status_1"]
                reg2, bit2 = spec["status_2"]
                
                idx1 = reg1 - min_reg
                idx2 = reg2 - min_reg
                
                # Make sure indices are within range
                if 0 <= idx1 < len(rr_status.registers) and 0 <= idx2 < len(rr_status.registers):
                    v1 = (rr_status.registers[idx1] >> bit1) & 1
                    v2 = (rr_status.registers[idx2] >> bit2) & 1
                    
                    # Detect status changes
                    prev_v1 = previous_station_statuses[name]["status_1"]
                    prev_v2 = previous_station_statuses[name]["status_2"]
                    
                    if v1 != prev_v1 or v2 != prev_v2:
                        station_changes[name] = {
                            "status_1": {"old": prev_v1, "new": v1},
                            "status_2": {"old": prev_v2, "new": v2}
                        }
                    
                    station_vals[name] = {"status_1": v1, "status_2": v2, "ts": now}
                    
                    # Update previous values for next cycle
                    previous_station_statuses[name]["status_1"] = v1
                    previous_station_statuses[name]["status_2"] = v2
            
            # 3. CHECK FOR LOADING STATION ACTIVATION (RISING EDGE)
            if "loading_station" in station_changes:
                changes = station_changes["loading_station"]
                # Rising edge detection (0->1) for status_1
                if changes["status_1"].get("old") == 0 and changes["status_1"].get("new") == 1:
                    logger.info("🔄 Loading station activated - checking for new barcode")
                    
                    # Read barcodes associated with loading station
                    ld = stations["loading_station"]
                    bc1_start, bc1_count = ld["barcode_block_1"]
                    bc2_start, bc2_count = ld["barcode_block_2"]
                    
                    rr_bc1 = await client.read_holding_registers(address=bc1_start, count=bc1_count)
                    rr_bc2 = await client.read_holding_registers(address=bc2_start, count=bc2_count)
                    
                    if not rr_bc1.isError() and not rr_bc2.isError():
                        bcA = decode_string(rr_bc1.registers)
                        bcB = decode_string(rr_bc2.registers)
                        
                        # Create a new set only if we have valid barcodes and haven't seen them
                        set_id = f"{bcA}|{bcB}"
                        if (bcA or bcB) and set_id not in seen_barcodes:
                            logger.info(f"🆕 New set detected with barcodes: {bcA}, {bcB}")
                            
                            # Add to tracking
                            new_set = {
                                "set_id": set_id,
                                "barcodes": [bcA, bcB],
                                "progress": {},
                                "created_ts": now,
                                "last_update": now,
                                "current_station": "loading_station"
                            }
                            active_sets.append(new_set)
                            seen_barcodes.add(set_id)
                            
                            # Limit active sets to most recent 3
                            if len(active_sets) > 3:
                                removed = active_sets.pop(0)
                                logger.info(f"⚠️ Removed oldest set {removed['set_id']} (limit: 3)")
                        else:
                            logger.warning(f"⚠️ Skipping duplicate or empty barcode set: {set_id}")
            
            # 4. UPDATE ALL ACTIVE SETS WITH CURRENT STATION STATUSES
            any_set_updated = False
            
            for set_idx, current_set in enumerate(active_sets):
                set_updated = False
                
                for name, vals in station_vals.items():
                    # Check if status changed for this station
                    current_vals = current_set.get("progress", {}).get(name, {"status_1": 0, "status_2": 0})
                    if vals["status_1"] != current_vals.get("status_1") or vals["status_2"] != current_vals.get("status_2"):
                        # Status changed, update with both status and associated barcode
                        set_updated = True
                        
                        # Read associated barcode for this station if status_1 changed from 0->1
                        if name in station_changes and station_changes[name]["status_1"].get("old") == 0 and vals["status_1"] == 1:
                            station_spec = stations[name]
                            if "barcode_block_1" in station_spec and "barcode_block_2" in station_spec:
                                bc1_start, bc1_count = station_spec["barcode_block_1"]
                                bc2_start, bc2_count = station_spec["barcode_block_2"]
                                
                                try:
                                    rr_bc1 = await client.read_holding_registers(address=bc1_start, count=bc1_count)
                                    rr_bc2 = await client.read_holding_registers(address=bc2_start, count=bc2_count)
                                    
                                    if not rr_bc1.isError() and not rr_bc2.isError():
                                        station_bcA = decode_string(rr_bc1.registers)
                                        station_bcB = decode_string(rr_bc2.registers)
                                        
                                        # Associate station-specific barcodes if they exist
                                        if station_bcA or station_bcB:
                                            vals["barcode_1"] = station_bcA
                                            vals["barcode_2"] = station_bcB
                                            logger.info(f"📋 Station {name} barcodes: {station_bcA}, {station_bcB}")
                                except Exception as e:
                                    logger.error(f"Error reading station barcodes: {e}")
                    
                    # Always update with latest status
                    current_set.setdefault("progress", {})[name] = vals
                
                # Mark station as current if status_1 is active
                for name, vals in current_set["progress"].items():
                    if vals["status_1"] == 1:
                        current_set["current_station"] = name
                        break
                
                # 5. REAL-TIME UPDATES - PUBLISH INDIVIDUAL SET CHANGES
                if set_updated:
                    current_set["last_update"] = now
                    any_set_updated = True
                    
                    # Send individual set update
                    if aio_producer:
                        set_payload = {
                            "set": current_set,
                            "type": "set_update",
                            "ts": now
                        }
                        
                        logger.info(f"Publishing update for set {current_set['set_id']}")
                        
                        # Send to both set-specific topic and general topic
                        sanitized_topic = sanitize_topic_name(f"{KAFKA_TOPIC_MACHINE_STATUS}.set", current_set['set_id'])
                        await aio_producer.send(sanitized_topic, value=set_payload)
                        
                        # Also send a notification to the general topic about this update
                        await aio_producer.send(KAFKA_TOPIC_MACHINE_STATUS, value={
                            "type": "set_update", 
                            "set_id": current_set["set_id"],
                            "current_station": current_set.get("current_station"),
                            "ts": now
                        })
            # 6. PUBLISH ALL SETS PERIODICALLY (BATCH UPDATE)
            if any_set_updated and aio_producer:
                full_payload = {
                    "sets": active_sets, 
                    "type": "full_update",
                    "ts": now
                }
                if aio_producer:
                    await aio_producer.send_and_wait(KAFKA_TOPIC_MACHINE_STATUS, value=full_payload)
                    logger.info(f"Published full update with {len(active_sets)} active sets")
                # Then for individual set updates (if any were updated):
                if any_set_updated and aio_producer:
                    for current_set in active_sets:
                        set_payload = {
                            "set": current_set,
                            "type": "set_update",
                            "ts": now
                        }
                        
                        # Use sanitized topic for individual updates
                        try:
                            sanitized_topic = sanitize_topic_name(
                                f"{KAFKA_TOPIC_MACHINE_STATUS}.set", 
                                current_set['set_id']
                            )
                            await aio_producer.send(sanitized_topic, value=set_payload)
                            
                            # Also send notification to main topic (this is what you're currently getting)
                            await aio_producer.send(KAFKA_TOPIC_MACHINE_STATUS, value={
                                "type": "set_update", 
                                "set_id": current_set["set_id"],
                                "current_station": current_set.get("current_station"),
                                "ts": now
                            })
                        except Exception as e:
                            logger.error(f"Error publishing set update: {e}")
            # 7. RETIRE COMPLETED SETS (unload_station status_1 = 1)
            before_count = len(active_sets)
            active_sets = [
                st for st in active_sets
                if st["progress"].get("unload_station", {}).get("status_1") != 1
            ]
            removed_count = before_count - len(active_sets)
            
            if removed_count > 0:
                logger.info(f"Retired {removed_count} completed sets")
                any_set_updated = True
            
            
            
        except Exception as e:
            logger.error(f"Error in read_specific_plc_data: {e}", exc_info=True)
        
        # Higher frequency for more responsive updates
        await asyncio.sleep(0.1)  # 10Hz update rate



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