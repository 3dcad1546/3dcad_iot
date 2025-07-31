#!/usr/bin/env python3
import os,json,time,asyncio
from typing import Dict
from collections import deque
from pymodbus.client.tcp import AsyncModbusTcpClient
from pymodbus.exceptions import ModbusException

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

# ─── CONFIG ────────────────────────────────────────────────────────────────
PLC_HOST = os.getenv("PLC_IP", "192.168.10.3")
PLC_PORT = int(os.getenv("PLC_PORT", "502"))
REGISTER_MAP = read_json_file("register_map.json")
MAX_REG_COUNT = 125  # Modbus limit
active_sets: list = []
pending_load = { 'bcA': None, 'bcB': None }

# ─── HELPERS ───────────────────────────────────────────────────────────────



def decode_string(words):
    raw = b''.join(
        (w & 0xFF).to_bytes(1, 'little') + ((w >> 8) & 0xFF).to_bytes(1, 'little')
        for w in words
    )
    return raw.decode('ascii', errors='ignore').rstrip('\x00')

def chunk_ranges(min_addr, max_addr, chunk_size=MAX_REG_COUNT):
    addr = min_addr
    while addr <= max_addr:
        cnt = min(chunk_size, max_addr - addr + 1)
        yield addr, cnt
        addr += cnt

# ─── CORE LOGIC ────────────────────────────────────────────────────────────
async def read_specific_plc_data_test(client: AsyncModbusTcpClient):
    """
    Advanced workpiece tracking system that:
      1) Bulk‐reads all status bits and all barcodes each cycle
      2) Detects 0→1 edges on status_1 / status_2
      3) Decodes the matching barcode from the same snapshot
      4) Publishes per‐set and full updates via Kafka
      5) Retires sets when unload station fires
    """
    global active_sets, pending_load, aio_producer # Assuming aio_producer might be used elsewhere

    PROCESS_STATIONS = [
        "loading_station", "xbot_1", "vision_1", "gantry_1",
        "xbot_2", "vision_2", "gantry_2", "vision_3", "unload_station"
    ]
    stations = REGISTER_MAP.get("stations", {})

    # --- 1) Precompute bulk‐read windows for statuses & barcodes ---
    all_status_addrs = {
        addr for spec in stations.values()
        for addr, _ in (spec.get("status_1", []), spec.get("status_2", []))
        if addr is not None
    }
    min_status = min(all_status_addrs)
    max_status = max(all_status_addrs)
    status_count = max_status - min_status + 1

    all_bc_addrs = []
    for spec in stations.values():
        for block in ("barcode_block_1", "barcode_block_2"):
            bc = spec.get(block)
            if bc:
                start, cnt = bc
                all_bc_addrs.extend(range(start, start + cnt))
    min_bc = min(all_bc_addrs)
    max_bc = max(all_bc_addrs)
    bc_count = max_bc - min_bc + 1

    # --- 2) Prepare previous‐status snapshot & seen‐sets ---
    prev = {name: {"status_1": 0, "status_2": 0} for name in stations}
    seen = {s["set_id"] for s in active_sets}

    while True:
        now = time.strftime("%Y-%m-%dT%H:%M:%S")
        any_update = False

        # A) Bulk‐read statuses
        rr_stat = await client.read_holding_registers(min_status, status_count)
        await asyncio.sleep(0.01)
        regs_stat = rr_stat.registers if not rr_stat.isError() else []

        # B) Bulk‐read barcodes
        rr_bc = await client.read_holding_registers(min_bc, bc_count)
        await asyncio.sleep(0.01)
        regs_bc = rr_bc.registers if not rr_bc.isError() else []

        # C) Per‐station edge detect + decode
        for st in PROCESS_STATIONS:
            spec = stations.get(st, {})
            # decode bit values
            addr1, bit1 = spec.get("status_1", (None, None))
            v1 = ((regs_stat[addr1 - min_status] >> bit1) & 1) if addr1 is not None else 0
            addr2, bit2 = spec.get("status_2", (None, None))
            v2 = ((regs_stat[addr2 - min_status] >> bit2) & 1) if addr2 is not None else 0

            # detect rising edges
            edge1 = prev[st]["status_1"] == 0 and v1 == 1
            edge2 = prev[st]["status_2"] == 0 and v2 == 1
            prev[st]["status_1"], prev[st]["status_2"] = v1, v2

            bc1 = bc2 = None
            if edge1 and spec.get("barcode_block_1"):
                start, cnt = spec["barcode_block_1"]
                slice_ = regs_bc[(start - min_bc):(start - min_bc + cnt)]
                bc1 = decode_string(slice_)
            if edge2 and spec.get("barcode_block_2"):
                start, cnt = spec["barcode_block_2"]
                slice_ = regs_bc[(start - min_bc):(start - min_bc + cnt)]
                bc2 = decode_string(slice_)

            # clear PLC bits only when edges fired
            if edge1:
                val = (await client.read_holding_registers(addr1, 1)).registers[0]
                await client.write_register(addr1, val & ~(1 << bit1))
            if edge2:
                val = (await client.read_holding_registers(addr2, 1)).registers[0]
                await client.write_register(addr2, val & ~(1 << bit2))

            # --- LOADING STATION: create new set once both barcodes seen ---
            if st == "loading_station":
                if bc1: pending_load['bcA'] = bc1
                if bc2: pending_load['bcB'] = bc2

                if pending_load['bcA'] and pending_load['bcB']:
                    sid = f"{pending_load['bcA']}|{pending_load['bcB']}"
                    if sid not in seen:
                        new_set = {
                            "set_id": sid,
                            "barcodes": [pending_load['bcA'], pending_load['bcB']],
                            "progress": {
                                "loading_station": {
                                    "status_1": 1, "status_2": 1,
                                    "barcode_1": pending_load['bcA'],
                                    "barcode_2": pending_load['bcB'],
                                    "ts": now, "latched": True
                                }
                            },
                            "current_station": "loading_station",
                            "created_ts": now,
                            "last_update": now
                        }
                        active_sets.append(new_set)
                        seen.add(sid)
                        any_update = True
                    pending_load['bcA'] = pending_load['bcB'] = None

            # --- OTHER STATIONS: update existing set if barcode matches ---
            elif bc1 or bc2:
                match = next(
                    (s for s in active_sets
                     if (bc1 and bc1 in s["barcodes"]) or (bc2 and bc2 in s["barcodes"])),
                    None
                )
                if match:
                    prog = match["progress"].setdefault(st, {})
                    if bc1 and not prog.get("barcode_1"):
                        prog.update({"status_1": 1, "barcode_1": bc1, "ts": now, "latched": True})
                        any_update = True
                    if bc2 and not prog.get("barcode_2"):
                        prog.update({"status_2": 1, "barcode_2": bc2, "ts": now, "latched_2": True})
                        any_update = True

                    # bump current_station if this is further along
                    new_idx = PROCESS_STATIONS.index(st)
                    old_idx = PROCESS_STATIONS.index(match["current_station"])
                    if new_idx > old_idx:
                        match["current_station"] = st
                        any_update = True
                    match["last_update"] = now

        # D) PUBLISH UPDATES if anything changed
        if any_update :
            # per‐set updates
            for s in active_sets:
                # Assuming this print statement simulates a publish operation
                print(
                    value={
                        "type": "set_update",
                        "set_id": s["set_id"],
                        "current_station": s["current_station"],
                        "ts": now
                    }
                )
            # full list
            print(
                value={"type": "full_update", "sets": active_sets, "ts": now}
            )

        # E) RETIRE COMPLETED SETS when unload fires
        completed = [
            s["set_id"] for s in active_sets
            if s["progress"].get("unload_station", {}).get("status_1") == 1
            or s["progress"].get("unload_station", {}).get("status_2") == 1
        ]
        if completed:
            us = stations["unload_station"]
            for stat in ("status_1", "status_2"):
                addr, bit = us.get(stat, (None, None))
                if addr is not None:
                    val = (await client.read_holding_registers(addr, 1)).registers[0]
                    await client.write_register(addr, val & ~(1 << bit))
            active_sets[:] = [s for s in active_sets if s["set_id"] not in completed]
            seen -= set(completed)
            # force a full update on retire
            # if aio_producer: # This condition might be intended for an actual Kafka producer
            print(
                value={"type": "full_update", "sets": active_sets, "ts": now}
            )

        # 10 Hz pacing
        await asyncio.sleep(0.1)

# ─── ENTRY POINT ───────────────────────────────────────────────────────────
async def main():
    client = AsyncModbusTcpClient(host=PLC_HOST, port=PLC_PORT)
    await client.connect()
    if not client.connected:
        print(f"[ERROR] Could not connect to PLC at {PLC_HOST}:{PLC_PORT}")
        return
    print(f"[OK] Connected to PLC at {PLC_HOST}:{PLC_PORT}\n")
    await read_specific_plc_data_test(client)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[STOPPED] by user")