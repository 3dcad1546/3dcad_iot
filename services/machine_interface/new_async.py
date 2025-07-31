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
