import asyncio
from pymodbus.client.tcp import AsyncModbusTcpClient
from pymodbus.exceptions import ModbusException

PLC_HOST = "192.168.10.3"  # or your PLC IP
PLC_PORT = 502

# async def main():
#     client = AsyncModbusTcpClient(host=PLC_HOST, port=PLC_PORT)
#     print(client,"clientttttt")

#     # Establish connection
#     await client.connect()
#     if not client.connected:
#         print("❌ Could not connect to PLC.")
#         return

#     print(f"✅ Connected to PLC at {PLC_HOST}:{PLC_PORT}")

#     try:
#         # Read holding registers (example: address 100, 2 registers)
#         response = await client.read_holding_registers(address=2000, count=12)
#         registers = response.registers  # e.g., [12336, 16706, ...]

#     # Step 2: Convert to bytes (big-endian assumed; adjust if needed)
#         byte_data = b''.join(reg.to_bytes(2, byteorder='big') for reg in registers)
#         print("Barcode:", byte_data)
#     # Step 3: Decode bytes to string (assuming ASCII or UTF-8)
#         barcode = byte_data.decode('ascii').rstrip('\x00')  # remove null chars if padded


#         if not response.isError():
#             registers = response.registers
#             print("📦 Read registers:", registers)

#             # Convert registers (16-bit) to bytes
#             byte_data = b''.join(reg.to_bytes(2, byteorder='big') for reg in registers)

#             # Decode to string, strip null bytes and control characters
#             barcode = byte_data.decode('ascii', errors='ignore').strip().strip('\x00\r\n')

#             print("✅ Barcode:", barcode)
#         else:
#             print("⚠️ Read error:", response)

#     except ModbusException as e:
#         print("❌ Modbus Exception:", e)


#     except Exception as e:
#         print("❌ General Exception:", e)

#     finally:
#         client.close()
#         print("🔌 Connection closed.")

# ─── Main PLC Data Reading and Publishing Loops ──────────────────────────────────
async def read_specific_plc_data(client: AsyncModbusTcpClient):
    """
    1) Watch the two global barcode flags (3303, 3304). When they BOTH
       go from 0→1, read the loading_station’s two barcode blocks,
       form a new set, and append to active_sets.
    2) Every cycle, read status_1/2 for _every_ station and attach to each
       active set’s .progress[name] = {status_1, status_2, ts}.
    3) Publish the full active_sets list as one payload.
    4) Retire a set once its unload_station status_1 goes high.
    """
    global _load_seen, active_sets

    cfg      = read_json_file("register_map.json")
    stations = cfg.get("stations", {})
    
    async def read_bit(reg, bit):
        rr = await client.read_holding_registers(reg, 1)
        return 0 if rr.isError() or not rr.registers else (rr.registers[0] >> bit) & 1
    while True:
        # 0) ensure PLC connection
        if not client.connected:
            logger.warning("❌ PLC not connected—reconnecting…")
            try:
                await client.connect()
            except:
                await asyncio.sleep(2)
                continue
            if not client.connected:
                await asyncio.sleep(2)
                continue

        now = time.strftime("%Y-%m-%dT%H:%M:%S")

        # 1) Read global barcode_flags
        try:
            rr1 = await client.read_holding_registers(BARCODE_FLAG_1, 1,unit=1)
            rr2 = await client.read_holding_registers(BARCODE_FLAG_2, 1,unit=1)
            flag1 = bool(rr1.registers[0]) if not rr1.isError() else False
            flag2 = bool(rr2.registers[0]) if not rr2.isError() else False
        except:
            flag1 = flag2 = False

        # 2) On rising edge of both flags → spawn a new set
        if flag1 and flag2 and not _load_seen:
            # read the two barcodes from your loading_station
            ld = stations["loading_station"]
            bcA = decode_string((await client.read_holding_registers(ld["barcode_block_1"][0], ld["barcode_block_1"][1])).registers)
            bcB = decode_string((await client.read_holding_registers(ld["barcode_block_2"][0], ld["barcode_block_2"][1])).registers)
            set_id = f"{bcA}|{bcB}"
            active_sets.append({
                "set_id":     set_id,
                "barcodes":   [bcA, bcB],
                "progress":   {},         # will fill below
                "created_ts": now
            })
            await client.write_register(BARCODE_FLAG_1, 0, unit=1)
            await client.write_register(BARCODE_FLAG_2, 0, unit=1)
            _load_seen = True

        # once both flags drop, allow next rising edge:
        if not (flag1 or flag2):
            _load_seen = False

        # 3) Read each station’s status_1/2 once per cycle
        

        station_vals = {}
        for name, spec in stations.items():
            v1 = await read_bit(*spec["status_1"])
            v2 = await read_bit(*spec["status_2"])
            station_vals[name] = {"status_1": v1, "status_2": v2, "ts": now}

        # 4) Attach those readings to every active set
        for st in active_sets:
            for name, vals in station_vals.items():
                st["progress"][name] = vals

        # 5) Retire any set that’s done at unload_station
        active_sets = [
            st for st in active_sets
            if st["progress"].get("unload_station", {}).get("status_1") != 1
        ]

        # 6) Publish the whole list in one shot
        payload = {"sets": active_sets, "ts": now}
        if aio_producer:
            await aio_producer.send_and_wait(KAFKA_TOPIC_MACHINE_STATUS, value=payload)
            logger.info(f"Published {len(active_sets)} active sets")
        else:
            logger.error("Kafka producer not ready; dropped machine_status")

        await asyncio.sleep(1)



# Run the async function
if __name__ == "__main__":
    asyncio.run(main())
