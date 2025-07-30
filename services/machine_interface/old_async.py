# async def read_specific_plc_data(client: AsyncModbusTcpClient):
#     """
#     Advanced workpiece tracking system that:
#     1) Detects new sets based on station status transitions
#     2) Tracks up to 3 concurrent sets through the process
#     3) Reads each station's associated barcodes
#     4) Publishes real-time updates for status changes
#     5) Maintains proper barcode-status association
#     """
#     global active_sets,pending_load
#     PROCESS_STATIONS = [
#     "loading_station", "xbot_1", "vision_1", "gantry_1",
#     "xbot_2", "vision_2", "gantry_2", "vision_3", "unload_station"
#     ]
    
#     cfg = REGISTER_MAP
#     stations = cfg.get("stations", {})
    
#     # Keep track of previous station statuses for edge detection
#     previous_station_statuses = {}
#     for station_name in stations.keys():
#         previous_station_statuses[station_name] = {"status_1": 0, "status_2": 0}
    
#     # Pre-calculate all register addresses for efficient batch reading
#     all_registers = []
#     for name, spec in stations.items():
#         reg1, bit1 = spec["status_1"]
#         reg2, bit2 = spec["status_2"]
#         all_registers.extend([reg1, reg2])
    
#     # Calculate optimal read range for status registers
#     min_reg = min(all_registers)
#     max_reg = max(all_registers)
#     status_count = max_reg - min_reg + 1
    
#     logger.info(f"Status register range: {min_reg}-{max_reg}, count: {status_count}")
    
#     # Set to keep track of barcodes we've seen to avoid duplicates
#     seen_barcodes = set()
    
#     # Main processing loop
#     while True:
#         if not client.connected:
#             logger.warning("❌ PLC not connected—reconnecting…")
#             try:
#                 await client.connect()
#             except Exception as e:
#                 logger.error(f"Connection error: {e}")
#                 await asyncio.sleep(0.5)
#                 continue
        
#         try:
#             now = time.strftime("%Y-%m-%dT%H:%M:%S")
#             # print(now,"")
            
#             # 1. READ ALL STATUS REGISTERS IN ONE BATCH
#             rr_status = await client.read_holding_registers(address=min_reg, count=status_count)
#             if rr_status.isError():
#                 logger.error(f"Failed to read status registers: {rr_status}")
#                 await asyncio.sleep(0.5)
#                 continue
            
#             # 2. EXTRACT STATION STATUSES FROM BATCH DATA
#             station_vals = {}
#             station_changes = {}
#             for name, spec in stations.items():
#                 reg1, bit1 = spec["status_1"]
#                 reg2, bit2 = spec["status_2"]
                
#                 idx1 = reg1 - min_reg
#                 idx2 = reg2 - min_reg
                
#                 # Make sure indices are within range
#                 if 0 <= idx1 < len(rr_status.registers) and 0 <= idx2 < len(rr_status.registers):
#                     v1 = (rr_status.registers[idx1] >> bit1) & 1
#                     v2 = (rr_status.registers[idx2] >> bit2) & 1
                    
#                     # Detect status changes
#                     prev_v1 = previous_station_statuses[name]["status_1"]
#                     prev_v2 = previous_station_statuses[name]["status_2"]
                    
#                     if v1 != prev_v1 or v2 != prev_v2:
#                         station_changes[name] = {
#                             "status_1": {"old": prev_v1, "new": v1},
#                             "status_2": {"old": prev_v2, "new": v2}
#                         }
                    
#                     station_vals[name] = {"status_1": v1, "status_2": v2, "ts": now}
                    
#                     # Update previous values for next cycle
#                     previous_station_statuses[name]["status_1"] = v1
#                     previous_station_statuses[name]["status_2"] = v2
            
#             # 3. CHECK FOR LOADING STATION ACTIVATION (RISING EDGE)
#             if "loading_station" in station_changes:
#                 changes = station_changes["loading_station"]
                
#                 # Rising edge detection (0->1) for status_1
#                 if changes["status_1"].get("old") == 0 and changes["status_1"].get("new") == 1:
#                     # Only read barcode 1
#                     ld = stations["loading_station"]
#                     bc1_start, bc1_count = ld["barcode_block_1"]
#                     rr_bc1 = await client.read_holding_registers(address=bc1_start, count=bc1_count)
#                     bcA = decode_string(rr_bc1.registers) if not rr_bc1.isError() else ""
#                     pending_load['bcA'] = bcA
#                     # set_id = f"{bcA}|"
#                     # if bcA and set_id not in seen_barcodes:
#                     #     # ... create set with only bcA ...
#                     #     new_set = {
#                     #         "set_id": set_id,
#                     #         "barcodes": [bcA],
#                     #         "progress": {},
#                     #         "created_ts": now,
#                     #         "last_update": now,
#                     #         "current_station": "loading_station"
#                     #     }
#                     #     active_sets.append(new_set)
#                     #     seen_barcodes.add(set_id)
#                     # else:
#                     #     logger.warning(f"⚠️ Skipping duplicate or empty barcode set: {set_id}")
#                     ld = stations["loading_station"]
#                     reg1, bit1 = ld["status_1"]
#                     rr = await client.read_holding_registers(address=reg1, count=1)
#                     if not rr.isError() and rr.registers:
#                         current = rr.registers[0]
#                         new = current & ~(1 << bit1)  # Clear the bit
#                         await client.write_register(reg1, new)

#                 if changes["status_2"].get("old") == 0 and changes["status_2"].get("new") == 1:
#                     # Only read barcode 2
#                     ld = stations["loading_station"]
#                     bc2_start, bc2_count = ld["barcode_block_2"]
#                     rr_bc2 = await client.read_holding_registers(address=bc2_start, count=bc2_count)
#                     bcB = decode_string(rr_bc2.registers) if not rr_bc2.isError() else ""
#                     pending_load['bcB'] = bcB
#                     # if bcB and set_id not in seen_barcodes:
#                     #     # ... create set with only bcB ...
#                     #     new_set = {
#                     #         "set_id": set_id,
#                     #         "barcodes": [bcB],
#                     #         "progress": {},
#                     #         "created_ts": now,
#                     #         "last_update": now,
#                     #         "current_station": "loading_station"
#                     #     }
#                     #     active_sets.append(new_set)
#                     #     seen_barcodes.add(set_id)
#                     # else:
#                     #     logger.warning(f"⚠️ Skipping duplicate or empty barcode set: {set_id}")
#                     ld = stations["loading_station"]
#                     reg2, bit2 = ld["status_2"]
#                     rr = await client.read_holding_registers(address=reg2, count=1)
#                     if not rr.isError() and rr.registers:
#                         current = rr.registers[0]
#                         new = current & ~(1 << bit2)  # Clear the bit
#                         await client.write_register(reg2, new)
#                 if pending_load['bcA'] and pending_load['bcB']:
#                     full_id = f"{pending_load['bcA']}|{pending_load['bcB']}"
#                     if full_id in seen_barcodes:
#                         logger.debug(f"Skipping duplicate set {full_id}")
#                     else:
#                         active_sets.append({
#                         "set_id": full_id,
#                         "barcodes": [pending_load['bcA'], pending_load['bcB']],
#                         "progress": {},
#                         "created_ts": now,
#                         "last_update": now,
#                         "current_station": None
#                         })
#                         seen_barcodes.add(full_id)
#                         logger.info(f"📦 New set created: {full_id}")
#                     # clear the buffer
#                     pending_load['bcA'] = pending_load['bcB'] = None    
#             # 4. UPDATE ALL ACTIVE SETS WITH CURRENT STATION STATUSES
#             any_set_updated = False
            
#             for set_idx, current_set in enumerate(active_sets):
#                 set_updated = False
#                 for name, vals in station_vals.items():
#                     prev = current_set.get("progress", {}).get(name, {})
#                     prev_status_1 = prev.get("status_1", 0)
#                     prev_status_2 = prev.get("status_2", 0)
#                     prev_ts = prev.get("ts", None)
#                     prev_barcode_1 = prev.get("barcode_1", None)
#                     prev_barcode_2 = prev.get("barcode_2", None)
#                     prev_latched = prev.get("latched", False)

#                     # Rising edge: latch and record timestamp/barcode
#                     if vals["status_1"] == 1 and prev_status_1==0:
#                         vals["ts"] = now
#                         vals["latched"] = True
#                         # (barcode reading logic here, as in your code)
#                         set_updated = True
#                     # If already latched, keep the timestamp/barcode even if status_1 is now 0
#                         reg1, bit1 = stations[name]["status_1"]
#                         rr = await client.read_holding_registers(address=reg1, count=1)
#                         if not rr.isError() and rr.registers:
#                             current = rr.registers[0]
#                             new = current & ~(1 << bit1)  # Clear the bit
#                             await client.write_register(reg1, new)
#                     if vals["status_2"] == 1 and prev_status_2==0:
#                         vals["ts_2"] = now
#                         vals["latched_2"] = True
#                         set_updated = True
#                         reg2, bit2 = stations[name]["status_2"]
#                         rr = await client.read_holding_registers(address=reg2, count=1)
#                         if not rr.isError() and rr.registers:
#                             current = rr.registers[0]
#                             new = current & ~(1 << bit2)  # Clear the bit
#                             await client.write_register(reg2, new)
#                     elif prev_latched:
#                         vals["ts"] = prev_ts
#                         vals["latched"] = True
#                         if prev_barcode_1:
#                             vals["barcode_1"] = prev_barcode_1
#                         if prev_barcode_2:
#                             vals["barcode_2"] = prev_barcode_2
#                     else:
#                         # No edge, not latched: normal update
#                         if prev_ts:
#                             vals["ts"] = prev_ts
#                         vals["latched"] = False

#                     current_set.setdefault("progress", {})[name] = vals
                
#                 # Always set current_station: if none are active, set to last known or None
#                 active_station = None
#                 for name in PROCESS_STATIONS:
#                     vals = current_set["progress"].get(name, {})
#                     if vals.get("status_1") == 1 or vals.get("status_2") == 1:
#                         active_station = name
#                         break
#                 if active_station:
#                     current_set["current_station"] = active_station
#                 else:
#                     current_set["current_station"] = current_set.get("current_station", None)
                                
#                 # 5. REAL-TIME UPDATES - PUBLISH INDIVIDUAL SET CHANGES
#                 if set_updated:
#                     current_set["last_update"] = now
#                     any_set_updated = True
                    
#                     # Send individual set update
#                     if aio_producer:
#                         set_payload = {
#                             "set": current_set,
#                             "type": "set_update",
#                             "ts": now
#                         }
                        
#                         logger.info(f"Publishing update for set {current_set['set_id']}")
                        
#                         # Send to both set-specific topic and general topic
#                         sanitized_topic = sanitize_topic_name(f"{KAFKA_TOPIC_MACHINE_STATUS}.set", current_set['set_id'])
#                         await aio_producer.send(sanitized_topic, value=set_payload)
                        
#                         # Also send a notification to the general topic about this update
#                         await aio_producer.send(KAFKA_TOPIC_MACHINE_STATUS, value={
#                             "type": "set_update", 
#                             "set_id": current_set["set_id"],
#                             "current_station": current_set.get("current_station"),
#                             "ts": now
#                         })
#             # 6. PUBLISH ALL SETS PERIODICALLY (BATCH UPDATE)
#             if any_set_updated and aio_producer:
#                 full_payload = {
#                     "sets": active_sets, 
#                     "type": "full_update",
#                     "ts": now
#                 }
#                 if aio_producer:
#                     await aio_producer.send_and_wait(KAFKA_TOPIC_MACHINE_STATUS, value=full_payload)
#                     logger.info(f"Published full update with {len(active_sets)} active sets")
                
#             # 7. RETIRE COMPLETED SETS (unload_station status_1 = 1)
#             before_count = len(active_sets)
#             active_sets = [
#                 st for st in active_sets
#                 if st["progress"].get("unload_station", {}).get("status_1") != 1
#                 and st["progress"].get("unload_station", {}).get("status_2") != 1
#             ]
#             removed_count = before_count - len(active_sets)
            
#             if removed_count > 0:
#                 unload = stations["unload_station"]
#                 reg1, bit1 = unload["status_1"]
#                 reg2, bit2 = unload["status_2"]
#                 rr1 = await client.read_holding_registers(address=reg1, count=1)
#                 if not rr1.isError() and rr1.registers and ((rr1.registers[0] >> bit1) & 1):
#                     current = rr1.registers[0]
#                     new = current & ~(1 << bit1)
#                     await client.write_register(reg1, new)
#                 rr2 = await client.read_holding_registers(address=reg2, count=1)
#                 if not rr2.isError() and rr2.registers and ((rr2.registers[0] >> bit2) & 1):
#                     current = rr2.registers[0]
#                     new = current & ~(1 << bit2)
#                     await client.write_register(reg2, new)
#                 logger.info("Cleared unload_station status_1 and status_2 bits after retirement.")
#                 logger.info(f"Retired {removed_count} completed sets")
#                 any_set_updated = True
            
            
            
#         except Exception as e:
#             logger.error(f"Error in read_specific_plc_data: {e}", exc_info=True)
        
#         # Higher frequency for more responsive updates
#         await asyncio.sleep(0.05)  # 10Hz update rate
