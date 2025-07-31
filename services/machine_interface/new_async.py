global previous_station_statuses, active_sets, seen_set_ids, pending_load

    stations = REGISTER_MAP["stations"]

    # 1. Determine the contiguous block of status registers to read in one go
    all_status_registers = []
    for name, spec in stations.items():
        if "status_1" in spec:
            reg1, _ = spec["status_1"]
            all_status_registers.append(reg1)
        if "status_2" in spec:
            reg2, _ = spec["status_2"]
            all_status_registers.append(reg2)

    if not all_status_registers:
        logger.error("No status registers defined in the register map.")
        return

    min_status_reg = min(all_status_registers)
    max_status_reg = max(all_status_registers)
    status_registers_count = max_status_reg - min_status_reg + 1

    while True:
        now = datetime.now()
        current_station_statuses = {}
        station_changes_detected = {}

        try:
            # 2. Read all relevant status registers in a single Modbus call
            rr_status = await client.read_holding_registers(address=min_status_reg, count=status_registers_count, slave=1)

            if rr_status.isError():
                logger.error(f"Modbus Error reading status registers: {rr_status}")
                await asyncio.sleep(loop_interval)
                continue

            if not rr_status.registers:
                logger.warning("No registers received for status block.")
                await asyncio.sleep(loop_interval)
                continue

            # Iterate through each station to check its status bits
            for station_name, station_spec in stations.items():
                status_bits = {}
                if "status_1" in station_spec:
                    reg1, bit1 = station_spec["status_1"]
                    # Calculate index relative to the start of the bulk read
                    idx1 = reg1 - min_status_reg
                    if 0 <= idx1 < len(rr_status.registers):
                        v1 = (rr_status.registers[idx1] >> bit1) & 1
                        status_bits["status_1"] = v1
                    else:
                        logger.warning(f"Status 1 register {reg1} for {station_name} out of bulk read range.")

                if "status_2" in station_spec:
                    reg2, bit2 = station_spec["status_2"]
                    idx2 = reg2 - min_status_reg
                    if 0 <= idx2 < len(rr_status.registers):
                        v2 = (rr_status.registers[idx2] >> bit2) & 1
                        status_bits["status_2"] = v2
                    else:
                        logger.warning(f"Status 2 register {reg2} for {station_name} out of bulk read range.")

                current_station_statuses[station_name] = status_bits

                # Detect changes (rising edge)
                changes = {}
                prev_status = previous_station_statuses.get(station_name, {})

                if "status_1" in status_bits and "status_1" in prev_status:
                    if status_bits["status_1"] != prev_status["status_1"]:
                        changes["status_1"] = {"old": prev_status["status_1"], "new": status_bits["status_1"]}
                elif "status_1" in status_bits: # First scan, initialize previous
                    changes["status_1"] = {"old": 0, "new": status_bits["status_1"]} # Assume initial state is 0 for rising edge detection

                if "status_2" in status_bits and "status_2" in prev_status:
                    if status_bits["status_2"] != prev_status["status_2"]:
                        changes["status_2"] = {"old": prev_status["status_2"], "new": status_bits["status_2"]}
                elif "status_2" in status_bits: # First scan, initialize previous
                    changes["status_2"] = {"old": 0, "new": status_bits["status_2"]} # Assume initial state is 0 for rising edge detection

                if changes:
                    station_changes_detected[station_name] = changes

            # Update previous_station_statuses for the next loop
            previous_station_statuses = current_station_statuses.copy()

            # 3. Process station status changes and read barcodes
            any_set_updated = False

            for station_name, changes in station_changes_detected.items():
                station_spec = stations[station_name]
                
                is_rising_edge_1 = (changes.get("status_1", {}).get("old") == 0 and changes.get("status_1", {}).get("new") == 1)
                is_rising_edge_2 = (changes.get("status_2", {}).get("old") == 0 and changes.get("status_2", {}).get("new") == 1)

                # --- Handle loading_station specifically ---
                if station_name == "loading_station":
                    barcode_1_read = None
                    barcode_2_read = None

                    if is_rising_edge_1:
                        logger.info(f"⬆️ Rising edge detected for {station_name} status_1.") #
                        if "barcode_block_1" in station_spec: #
                            bc1_start, bc1_count = station_spec["barcode_block_1"] #
                            rr_bc1 = await client.read_holding_registers(address=bc1_start, count=bc1_count, slave=1) #
                            if not rr_bc1.isError() and rr_bc1.registers: #
                                barcode_1_read = decode_string(rr_bc1.registers) #
                                logger.info(f"🔢 Read barcode 1 '{barcode_1_read}' at {station_name}.") #
                            else:
                                logger.error(f"Failed to read barcode 1 for {station_name}: {rr_bc1}") #

                        # Clear PLC bit for status_1
                        reg1, bit1 = station_spec["status_1"] #
                        rr_clear = await client.read_holding_registers(address=reg1, count=1, slave=1) #
                        if not rr_clear.isError() and rr_clear.registers: #
                            current = rr_clear.registers[0] #
                            new = current & ~(1 << bit1)  # Clear the bit
                            await client.write_register(reg1, new, slave=1) #
                            logger.debug(f"Cleared {station_name} status_1 bit.") #
                        else:
                            logger.error(f"Failed to read/clear {station_name} status_1 bit: {rr_clear}") #

                    if is_rising_edge_2:
                        logger.info(f"⬆️ Rising edge detected for {station_name} status_2.") #
                        if "barcode_block_2" in station_spec: #
                            bc2_start, bc2_count = station_spec["barcode_block_2"] #
                            rr_bc2 = await client.read_holding_registers(address=bc2_start, count=bc2_count, slave=1) #
                            if not rr_bc2.isError() and rr_bc2.registers: #
                                barcode_2_read = decode_string(rr_bc2.registers) #
                                logger.info(f"🔢 Read barcode 2 '{barcode_2_read}' at {station_name}.") #
                            else:
                                logger.error(f"Failed to read barcode 2 for {station_name}: {rr_bc2}") #

                        # Clear PLC bit for status_2
                        reg2, bit2 = station_spec["status_2"] #
                        rr_clear = await client.read_holding_registers(address=reg2, count=1, slave=1) #
                        if not rr_clear.isError() and rr_clear.registers: #
                            current = rr_clear.registers[0] #
                            new = current & ~(1 << bit2)  # Clear the bit
                            await client.write_register(reg2, new, slave=1) #
                            logger.debug(f"Cleared {station_name} status_2 bit.") #
                        else:
                            logger.error(f"Failed to read/clear {station_name} status_2 bit: {rr_clear}") #

                    if barcode_1_read:
                        pending_load['bcA'] = barcode_1_read
                    if barcode_2_read:
                        pending_load['bcB'] = barcode_2_read

                    # Only create a new set if both barcodes are read for loading
                    if pending_load['bcA'] and pending_load['bcB']:
                        full_set_id = f"{pending_load['bcA']}|{pending_load['bcB']}"
                        if full_set_id not in seen_set_ids:
                            new_set = {
                                "set_id": full_set_id,
                                "barcodes": [pending_load['bcA'], pending_load['bcB']],
                                "progress": {}, # Initialize empty, then add loading_station
                                "created_ts": now,
                                "last_update": now,
                                "current_station": "loading_station"
                            }
                            # Add loading station progress explicitly
                            new_set["progress"]["loading_station"] = {
                                "status_1": 1, "status_2": 1, "ts": now, "latched": True,
                                "barcode_1": pending_load['bcA'], "barcode_2": pending_load['bcB']
                            }
                            active_sets.append(new_set)
                            seen_set_ids.add(full_set_id)
                            logger.info(f"📦 New set created: {full_set_id} at loading_station.")
                            any_set_updated = True
                        else:
                            logger.debug(f"Skipping duplicate set creation for {full_set_id} at loading_station.")
                        # Reset pending_load for the next new set
                        pending_load['bcA'] = None
                        pending_load['bcB'] = None

                # --- Handle all other stations ---
                else:
                    # Process status_1 change (e.g., workpiece A arrives)
                    if is_rising_edge_1:
                        logger.info(f"⬆️ Rising edge detected for {station_name} status_1.") #
                        barcode_1_read_current = None
                        if "barcode_block_1" in station_spec: #
                            bc1_start, bc1_count = station_spec["barcode_block_1"] #
                            rr_bc1 = await client.read_holding_registers(address=bc1_start, count=bc1_count, slave=1) #
                            if not rr_bc1.isError() and rr_bc1.registers: #
                                barcode_1_read_current = decode_string(rr_bc1.registers) #
                                logger.info(f"🔢 Read barcode 1 '{barcode_1_read_current}' at {station_name}.") #

                                # Find the active set by barcode
                                found_set_1 = next((s for s in active_sets if barcode_1_read_current in s["barcodes"]), None)
                                if found_set_1:
                                    station_progress = found_set_1["progress"].setdefault(station_name, {})
                                    station_progress["status_1"] = 1
                                    station_progress["barcode_1"] = barcode_1_read_current # Store the barcode
                                    station_progress["ts"] = now
                                    station_progress["latched"] = True
                                    logger.debug(f"Updated set {found_set_1['set_id']} with barcode 1 at {station_name}")
                                    any_set_updated = True

                                    # Update current_station for the set if it has moved forward
                                    try:
                                        current_station_index = PROCESS_STATIONS.index(station_name)
                                        if (found_set_1["current_station"] is None or
                                            PROCESS_STATIONS.index(found_set_1["current_station"]) < current_station_index):
                                            found_set_1["current_station"] = station_name
                                            logger.info(f"Set {found_set_1['set_id']} moved to {station_name}.")
                                    except ValueError:
                                        logger.warning(f"Station {station_name} not found in PROCESS_STATIONS list.")

                                    found_set_1["last_update"] = now
                                else:
                                    logger.warning(f"⚠️ Barcode '{barcode_1_read_current}' at {station_name} not associated with any active set. (Might be an unexpected entry or late detection)") #
                            else:
                                logger.error(f"Failed to read barcode 1 for {station_name}: {rr_bc1}") #
                        
                        # Immediately clear the PLC bit for status_1 (AFTER processing the event)
                        reg1, bit1 = station_spec["status_1"] #
                        rr_clear = await client.read_holding_registers(address=reg1, count=1, slave=1) #
                        if not rr_clear.isError() and rr_clear.registers: #
                            current = rr_clear.registers[0] #
                            new = current & ~(1 << bit1) # Clear the bit
                            await client.write_register(reg1, new, slave=1) #
                            logger.debug(f"Cleared {station_name} status_1 bit.") #
                        else:
                            logger.error(f"Failed to read/clear {station_name} status_1 bit: {rr_clear}") #

                    # Process status_2 change (e.g., workpiece B arrives)
                    if is_rising_edge_2:
                        logger.info(f"⬆️ Rising edge detected for {station_name} status_2.") #
                        barcode_2_read_current = None
                        if "barcode_block_2" in station_spec: #
                            bc2_start, bc2_count = station_spec["barcode_block_2"] #
                            rr_bc2 = await client.read_holding_registers(address=bc2_start, count=bc2_count, slave=1) #
                            if not rr_bc2.isError() and rr_bc2.registers: #
                                barcode_2_read_current = decode_string(rr_bc2.registers) #
                                logger.info(f"🔢 Read barcode 2 '{barcode_2_read_current}' at {station_name}.") #

                                # Find the active set by barcode
                                found_set_2 = next((s for s in active_sets if barcode_2_read_current in s["barcodes"]), None)
                                if found_set_2:
                                    station_progress = found_set_2["progress"].setdefault(station_name, {})
                                    station_progress["status_2"] = 1
                                    station_progress["barcode_2"] = barcode_2_read_current # Store the barcode
                                    station_progress["ts"] = now # Consistent timestamp
                                    station_progress["latched"] = True # Consistent latching
                                    logger.debug(f"Updated set {found_set_2['set_id']} with barcode 2 at {station_name}")
                                    any_set_updated = True

                                    # Update current_station for the set if it has moved forward
                                    try:
                                        current_station_index = PROCESS_STATIONS.index(station_name)
                                        if (found_set_2["current_station"] is None or
                                            PROCESS_STATIONS.index(found_set_2["current_station"]) < current_station_index):
                                            found_set_2["current_station"] = station_name
                                            logger.info(f"Set {found_set_2['set_id']} moved to {station_name}.")
                                    except ValueError:
                                        logger.warning(f"Station {station_name} not found in PROCESS_STATIONS list.")

                                    found_set_2["last_update"] = now
                                else:
                                    logger.warning(f"⚠️ Barcode '{barcode_2_read_current}' at {station_name} not associated with any active set. (Might be an unexpected entry or late detection)") #
                            else:
                                logger.error(f"Failed to read barcode 2 for {station_name}: {rr_bc2}") #

                        # Immediately clear the PLC bit for status_2 (AFTER processing the event)
                        reg2, bit2 = station_spec["status_2"] #
                        rr_clear = await client.read_holding_registers(address=reg2, count=1, slave=1) #
                        if not rr_clear.isError() and rr_clear.registers: #
                            current = rr_clear.registers[0] #
                            new = current & ~(1 << bit2) # Clear the bit
                            await client.write_register(reg2, new, slave=1) #
                            logger.debug(f"Cleared {station_name} status_2 bit.") #
                        else:
                            logger.error(f"Failed to read/clear {station_name} status_2 bit: {rr_clear}") #

            # 4. Cleanup/publish old active sets (optional, but good for memory management)
            # Remove sets that have completed all stations or have been inactive for too long
            # (You'll need to define what "completed" means, e.g., reaching unload_station)
            sets_to_remove = []
            for s in active_sets:
                if s["current_station"] == "unload_station" and s["progress"].get("unload_station", {}).get("latched"):
                    logger.info(f"✅ Set {s['set_id']} completed all stations and being removed.")
                    sets_to_remove.append(s)
                elif (now - s["last_update"]) > timedelta(minutes=5): # Example: inactive for 5 mins
                    logger.warning(f"⌛ Set {s['set_id']} inactive for too long, removing.")
                    sets_to_remove.append(s)

            for s in sets_to_remove:
                active_sets.remove(s)
                seen_set_ids.discard(s["set_id"]) # Also remove from seen_set_ids

            # 5. Log current active sets (for debugging/monitoring)
            if any_set_updated or sets_to_remove: # Only log if there were changes
                logger.info(f"Current Active Sets ({len(active_sets)}):")
                for s in active_sets:
                    station_history = ", ".join([f"{st}: {d.get('barcode_1', '')}" for st, d in s['progress'].items()])
                    logger.info(f"  - Set ID: {s['set_id']}, Current Station: {s['current_station']}, Last Update: {s['last_update'].strftime('%H:%M:%S')}, Progress: {{{station_history}}}")
            
        except Exception as e:
            logger.exception(f"An unexpected error occurred in read_specific_plc_data: {e}")

        await asyncio.sleep(loop_interval)
