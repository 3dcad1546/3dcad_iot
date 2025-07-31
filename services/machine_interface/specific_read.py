#!/usr/bin/env python3
import asyncio
import json
import os
import time
from pymodbus.client.tcp import AsyncModbusTcpClient
from pymodbus.exceptions import ModbusException

# ─── CONFIG ────────────────────────────────────────────────────────────────
PLC_HOST = os.getenv("PLC_IP", "192.168.10.3")
PLC_PORT = int(os.getenv("PLC_PORT", "502"))
REGISTER_MAP_FILE = "register_map.json"
MAX_REG_COUNT = 125  # Modbus limit

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
    # Load register map
    with open(REGISTER_MAP_FILE, 'r') as f:
        cfg = json.load(f)
    stations = cfg.get("stations", {})

    # Prepare pending_load here
    pending_load = {'bcA': None, 'bcB': None}

    # Precompute status window
    status_addrs = {
        addr
        for spec in stations.values()
        for addr,_ in (spec.get("status_1", []), spec.get("status_2", []))
        if addr is not None
    }
    min_status, max_status = min(status_addrs), max(status_addrs)
    status_count = max_status - min_status + 1
    print(f"[INIT] Status window: {min_status} → {max_status} ({status_count} regs)")

    # Precompute barcode window
    bc_addrs = set()
    for spec in stations.values():
        for blk in ("barcode_block_1", "barcode_block_2"):
            v = spec.get(blk)
            if v:
                start, cnt = v
                bc_addrs.update(range(start, start + cnt))
    min_bc, max_bc = min(bc_addrs), max(bc_addrs)
    print(f"[INIT] Barcode window: {min_bc} → {max_bc} ({len(bc_addrs)} regs)")

    # Prepare previous snapshot
    prev = {}
    try:
        rr0 = await client.read_holding_registers(min_status, count=status_count)
        regs0 = rr0.registers
        for name, spec in stations.items():
            a1,b1 = spec.get("status_1",(None,None))
            a2,b2 = spec.get("status_2",(None,None))
            v1 = (regs0[a1-min_status] >> b1) & 1 if a1 is not None else 0
            v2 = (regs0[a2-min_status] >> b2) & 1 if a2 is not None else 0
            prev[name] = {"status_1": v1, "status_2": v2}
        print("[INIT] Seeded previous status snapshot")
    except Exception as e:
        print(f"[WARN] Could not seed prev snapshot: {e}")
        prev = {name: {"status_1":0,"status_2":0} for name in stations}

    # Track seen sets
    active_sets = []
    seen_ids = set()

    print("[RUN] Entering main loop (CTRL+C to stop)...\n")
    while True:
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        any_update = False

        # 1) Bulk read status bits
        try:
            rr_stat = await client.read_holding_registers(min_status, count=status_count)
            regs_stat = rr_stat.registers
        except Exception as e:
            print(f"[ERROR] Status read failed: {e}")
            await asyncio.sleep(0.5)
            continue

        # 2) Bulk read barcode words (in chunks)
        bc_map = {}
        for addr, cnt in chunk_ranges(min_bc, max_bc):
            try:
                rr_bc = await client.read_holding_registers(addr, count=cnt)
                for i, val in enumerate(rr_bc.registers):
                    bc_map[addr + i] = val
            except Exception as e:
                print(f"[ERROR] Barcode read {addr}+{cnt} failed: {e}")

        # 3) Per-station edge detection & decode
        for st, spec in stations.items():
            a1,b1 = spec.get("status_1",(None,None))
            a2,b2 = spec.get("status_2",(None,None))

            v1 = ((regs_stat[a1 - min_status] >> b1) & 1) if a1 is not None else 0
            v2 = ((regs_stat[a2 - min_status] >> b2) & 1) if a2 is not None else 0

            edge1 = prev[st]["status_1"] == 0 and v1 == 1
            edge2 = prev[st]["status_2"] == 0 and v2 == 1
            prev[st]["status_1"], prev[st]["status_2"] = v1, v2

            bc1 = bc2 = None
            if edge1 and spec.get("barcode_block_1"):
                s0,cnt = spec["barcode_block_1"]
                words = [bc_map.get(addr,0) for addr in range(s0, s0+cnt)]
                bc1 = decode_string(words)
                print(f"[{now}] ↑ Edge1 @ {st} → BC1='{bc1}'")
            if edge2 and spec.get("barcode_block_2"):
                s0,cnt = spec["barcode_block_2"]
                words = [bc_map.get(addr,0) for addr in range(s0, s0+cnt)]
                bc2 = decode_string(words)
                print(f"[{now}] ↑ Edge2 @ {st} → BC2='{bc2}'")

            # clear bits
            if edge1:
                reg = await client.read_holding_registers(a1, count=1)
                await client.write_register(a1, reg.registers[0] & ~(1 << b1))
            if edge2:
                reg = await client.read_holding_registers(a2, count=1)
                await client.write_register(a2, reg.registers[0] & ~(1 << b2))

            # Loading station: new set
            if st == "loading_station":
                if bc1: pending_load['bcA'] = bc1
                if bc2: pending_load['bcB'] = bc2
                if pending_load.get('bcA') and pending_load.get('bcB'):
                    sid = f"{pending_load['bcA']}|{pending_load['bcB']}"
                    if sid not in seen_ids:
                        print(f"[{now}] 🔄 New set created: {sid}")
                        active_sets.append({"id": sid, "barcodes": [pending_load['bcA'], pending_load['bcB']]})
                        seen_ids.add(sid)
                        any_update = True
                    pending_load['bcA'] = pending_load['bcB'] = None

            # Other stations: update existing set
            elif bc1 or bc2:
                for s in active_sets:
                    if bc1 in s["barcodes"] or bc2 in s["barcodes"]:
                        print(f"[{now}] 🔄 Set {s['id']} @ {st} (BC1={bc1}, BC2={bc2})")
                        any_update = True
                        break

        # 4) Retire completed sets on unload
        removed = []
        for s in active_sets:
            spec = stations["unload_station"]
            u1,b1 = spec.get("status_1",(None,None))
            u2,b2 = spec.get("status_2",(None,None))
            if ((regs_stat[u1-min_status]>>b1)&1)==1 or ((regs_stat[u2-min_status]>>b2)&1)==1:
                removed.append(s["id"])
        if removed:
            for sid in removed:
                print(f"[{now}] ✅ Retired set: {sid}")
                active_sets = [x for x in active_sets if x["id"] != sid]
                seen_ids.discard(sid)
            any_update = True

        if not any_update:
            print(f"[{now}] — no changes")

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
