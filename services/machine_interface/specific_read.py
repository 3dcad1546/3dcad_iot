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
    Standalone test for bulk-read logic:
      1) Bulk-read all status bits and barcode registers
      2) Detect rising edges and decode matching barcodes
      3) Print new-set creation, station updates, and retire events
    """
    # Load register map from JSON in same folder
    with open('register_map.json', 'r') as f:
        cfg = json.load(f)
    stations = cfg.get('stations', {})
    PROCESS_STATIONS = [
        'loading_station', 'xbot_1', 'vision_1', 'gantry_1',
        'xbot_2', 'vision_2', 'gantry_2', 'vision_3', 'unload_station'
    ]

    # Precompute bulk windows
    all_status = {addr for spec in stations.values()
                   for addr, _ in (spec.get('status_1', []), spec.get('status_2', []))
                   if addr is not None}
    min_status, max_status = min(all_status), max(all_status)
    status_count = max_status - min_status + 1

    all_bc = []
    for spec in stations.values():
        for blk in ('barcode_block_1', 'barcode_block_2'):
            b = spec.get(blk)
            if b:
                start, cnt = b
                all_bc.extend(range(start, start+cnt))
    min_bc, max_bc = min(all_bc), max(all_bc)
    bc_count = max_bc - min_bc + 1

    # Seed previous statuses to avoid initial false edges
    prev = {}
    try:
        rr0 = await client.read_holding_registers(min_status, count=status_count)
        regs0 = rr0.registers
        for st in PROCESS_STATIONS:
            spec = stations.get(st, {})
            a1, b1 = spec.get('status_1', (None, None))
            a2, b2 = spec.get('status_2', (None, None))
            v1 = ((regs0[a1-min_status] >> b1) & 1) if a1 is not None else 0
            v2 = ((regs0[a2-min_status] >> b2) & 1) if a2 is not None else 0
            prev[st] = {'status_1': v1, 'status_2': v2}
        print('Seeded previous status snapshot')
    except Exception as e:
        print(f'Failed to seed status snapshot: {e}')
        prev = {st: {'status_1': 0, 'status_2': 0} for st in PROCESS_STATIONS}

    seen = {s['set_id'] for s in active_sets}

    # Main loop
    while True:
        now = time.strftime('%Y-%m-%dT%H:%M:%S')
        any_change = False

        # Bulk read statuses
        rr_stat = await client.read_holding_registers(min_status, count=status_count)
        regs_stat = rr_stat.registers if not rr_stat.isError() else []
        # Bulk read barcodes
        rr_bc = await client.read_holding_registers(min_bc, count=bc_count)
        regs_bc = rr_bc.registers if not rr_bc.isError() else []

        # Per-station processing
        for st in PROCESS_STATIONS:
            spec = stations.get(st, {})
            a1, b1 = spec.get('status_1', (None, None))
            a2, b2 = spec.get('status_2', (None, None))

            v1 = ((regs_stat[a1-min_status] >> b1) & 1) if a1 is not None else 0
            v2 = ((regs_stat[a2-min_status] >> b2) & 1) if a2 is not None else 0

            edge1 = prev[st]['status_1'] == 0 and v1 == 1
            edge2 = prev[st]['status_2'] == 0 and v2 == 1
            prev[st]['status_1'], prev[st]['status_2'] = v1, v2

            bc1 = bc2 = None
            if edge1 and spec.get('barcode_block_1'):
                s0, cnt = spec['barcode_block_1']
                bc1 = decode_string(regs_bc[(s0-min_bc):(s0-min_bc+cnt)])
            if edge2 and spec.get('barcode_block_2'):
                s0, cnt = spec['barcode_block_2']
                bc2 = decode_string(regs_bc[(s0-min_bc):(s0-min_bc+cnt)])

            # Clear bits
            if edge1:
                rr_clear = await client.read_holding_registers(a1, count=1)
                val = rr_clear.registers[0]
                await client.write_register(a1, val & ~(1 << b1))
            if edge2:
                rr_clear = await client.read_holding_registers(a2, count=1)
                val = rr_clear.registers[0]
                await client.write_register(a2, val & ~(1 << b2))

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
                        active_sets.append({'set_id': sid, 'barcodes': [pending_load['bcA'], pending_load['bcB']]})
                        seen.add(sid)
                        print(f"[{now}] New set created: {sid}")
                        any_change = True
                    pending_load['bcA'] = pending_load['bcB'] = None

            # Other stations update
            elif bc1 or bc2:
                match = next((s for s in active_sets
                              if (bc1 and bc1 in s['barcodes']) or (bc2 and bc2 in s['barcodes'])), None)
                if match:
                    print(f"[{now}] Set {match['set_id']} at station {st}, bc1={bc1}, bc2={bc2}")
                    any_change = True

        # Retire sets at unload station
        completed = []
        for s in active_sets:
            completed.append(s['set_id'])
        if completed:
            for sid in completed:
                print(f"[{now}] Retiring set {sid}")
            active_sets[:] = [s for s in active_sets if s['set_id'] not in completed]
            any_change = True

        if not any_change:
            print(f"[{now}] No change detected this cycle")

        await asyncio.sleep(0.1)

async def main():
    client = AsyncModbusTcpClient(host=PLC_HOST, port=PLC_PORT)
    await client.connect()
    if not client.connected:
        print(f"Failed to connect to PLC at {PLC_HOST}:{PLC_PORT}")
        return
    print(f"Connected to PLC at {PLC_HOST}:{PLC_PORT}")
    await read_specific_plc_data_test(client)

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print('Test terminated by user')
