import os
import json
import time
import asyncio

from pymodbus.client.tcp import AsyncModbusTcpClient

# PLC connection settings (override via env variables if needed)
PLC_HOST = os.getenv("PLC_IP", "192.168.10.3")
PLC_PORT = int(os.getenv("PLC_PORT", "502"))

# Globals for tracking sets
active_sets = []
pending_load = {'bcA': None, 'bcB': None}

# Utility to decode Modbus register words into ASCII string
def decode_string(words):
    raw_bytes = b''.join([
        (w & 0xFF).to_bytes(1, 'little') + ((w >> 8) & 0xFF).to_bytes(1, 'little')
        for w in words
    ])
    return raw_bytes.decode('ascii', errors='ignore').rstrip("\x00")

async def read_specific_plc_data_test(client: AsyncModbusTcpClient):
    """
    Standalone test for bulk-read logic with debug prints:
      1) Bulk-read all status bits and barcode registers
      2) Detect rising edges and decode matching barcodes
      3) Print debug info each cycle
    """
    # Load register map
    with open('register_map.json', 'r') as f:
        cfg = json.load(f)
    stations = cfg.get('stations', {})
    PROCESS_STATIONS = list(stations.keys())

    # Precompute status window
    all_status = {addr for spec in stations.values()
                   for addr, _ in (spec.get('status_1', []), spec.get('status_2', []))
                   if addr is not None}
    min_status, max_status = min(all_status), max(all_status)
    status_count = max_status - min_status + 1

    # Precompute barcode range and chunk windows
    all_bc = []
    for spec in stations.values():
        for blk in ('barcode_block_1', 'barcode_block_2'):
            b = spec.get(blk)
            if b:
                start, cnt = b
                all_bc.extend(range(start, start+cnt))
    min_bc, max_bc = min(all_bc), max(all_bc)
    bc_total = max_bc - min_bc + 1

    # Create chunked windows respecting max 125 registers per read
    bc_windows = []
    max_regs = 125
    offset = 0
    while offset < bc_total:
        count = min(max_regs, bc_total - offset)
        bc_windows.append((min_bc + offset, count))
        offset += count

    # Seed previous snapshot
    prev = {}
    rr0 = await client.read_holding_registers(min_status, count=status_count)
    regs0 = rr0.registers
    for st in PROCESS_STATIONS:
        spec = stations[st]
        a1,b1 = spec.get('status_1',(None,None))
        a2,b2 = spec.get('status_2',(None,None))
        v1 = ((regs0[a1-min_status]>>b1)&1) if a1 is not None else 0
        v2 = ((regs0[a2-min_status]>>b2)&1) if a2 is not None else 0
        prev[st] = {'status_1': v1, 'status_2': v2}
    print('Seeded prev-status snapshot:', prev)

    seen = set()

    # Loop
    while True:
        now = time.strftime('%Y-%m-%dT%H:%M:%S')
        any_change = False

        # Bulk read statuses
        rr_stat = await client.read_holding_registers(min_status, count=status_count)
        regs_stat = rr_stat.registers if not rr_stat.isError() else []
        print(f"[{now}] Status snapshot ({min_status}-{max_status}): {regs_stat}")

        # Bulk read barcodes in chunks and build dict
        bc_map = {}
        for addr,cnt in bc_windows:
            rr_bc = await client.read_holding_registers(addr, count=cnt)
            if not rr_bc.isError():
                for i,val in enumerate(rr_bc.registers):
                    bc_map[addr+i] = val
        print(f"[{now}] Barcode addr map keys sample: {sorted(list(bc_map.keys()))[:5]}...")

        # Process each station
        for st in PROCESS_STATIONS:
            spec = stations[st]
            a1,b1 = spec.get('status_1',(None,None))
            a2,b2 = spec.get('status_2',(None,None))
            v1 = ((regs_stat[a1-min_status]>>b1)&1) if a1 is not None else 0
            v2 = ((regs_stat[a2-min_status]>>b2)&1) if a2 is not None else 0
            print(f"[{now}] {st}: v1={v1}, v2={v2}")

            edge1 = prev[st]['status_1']==0 and v1==1
            edge2 = prev[st]['status_2']==0 and v2==1
            prev[st]['status_1'], prev[st]['status_2'] = v1, v2

            bc1=bc2=None
            if edge1 and spec.get('barcode_block_1'):
                s0,cnt = spec['barcode_block_1']
                words=[bc_map.get(addr,0) for addr in range(s0,s0+cnt)]
                bc1 = decode_string(words)
                print(f"[{now}] Edge1 at {st}, decoded bc1: {bc1}")
            if edge2 and spec.get('barcode_block_2'):
                s0,cnt = spec['barcode_block_2']
                words=[bc_map.get(addr,0) for addr in range(s0,s0+cnt)]
                bc2 = decode_string(words)
                print(f"[{now}] Edge2 at {st}, decoded bc2: {bc2}")

            # clear bits
            if edge1:
                rr_c=await client.read_holding_registers(a1, count=1)
                await client.write_register(a1, rr_c.registers[0] & ~(1<<b1))
            if edge2:
                rr_c=await client.read_holding_registers(a2, count=1)
                await client.write_register(a2, rr_c.registers[0] & ~(1<<b2))

            # Loading station logic
            if st == 'loading_station':
                if bc1:
                    pending_load['bcA'] = bc1
                    print(f"[{now}] Loading station barcode1 detected: {bc1}")
                if bc2:
                    pending_load['bcB'] = bc2
                    print(f"[{now}] Loading station barcode2 detected: {bc2}")
                if pending_load['bcA'] and pending_load['bcB']:
                    sid = f"{pending_load['bcA']}|{pending_load['bcB']}"
                    if sid not in seen:
                        seen.add(sid)
                        active_sets.append({'set_id': sid, 'barcodes': [pending_load['bcA'], pending_load['bcB']]})
                        print(f"[{now}] New set created: {sid}")
                        any_change = True
                    pending_load['bcA'] = pending_load['bcB'] = None
            elif bc1 or bc2:
                for s in active_sets:
                    if (bc1 and bc1 in s.get('barcodes', [])) or (bc2 and bc2 in s.get('barcodes', [])):
                        print(f"[{now}] Set {s['set_id']} @ {st}, bc1={bc1}, bc2={bc2}")
                        any_change = True

        # retire all (demo)
        if active_sets:
            for s in active_sets:
                print(f"[{now}] Retiring set {s['set_id']}")
            active_sets.clear()
            any_change = True

        if not any_change:
            print(f"[{now}] No change detected this cycle")
        await asyncio.sleep(0.1)

async def main():
    client = AsyncModbusTcpClient(host=PLC_HOST, port=PLC_PORT)
    await client.connect()
    print(f"Connected to PLC at {PLC_HOST}:{PLC_PORT}")
    await read_specific_plc_data_test(client)

if __name__ == '__main__':
    asyncio.run(main())
