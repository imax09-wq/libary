from asyncio    import gather, run, sleep
from src.tick_db.db import DB_CON, load_depth, load_tas # <-- Import path updated
from json       import dumps, loads
from os         import walk
from src.tick_db.parsers import parse_depth, parse_depth_header, parse_tas, parse_tas_header, transform_tas, transform_depth # <-- Assuming parsers is also in src/tick_db
from sys        import argv
from time       import time
import os # Import os for path operations

# Moved parser constant imports here for clarity if needed globally
try:
    from src.tick_db.parsers import INTRADAY_HEADER_LEN, INTRADAY_REC_LEN, DEPTH_HEADER_LEN, DEPTH_REC_LEN
except ImportError as e:
     print(f"Fatal Error: Could not import necessary constants from parsers: {e}. Exiting.")
     exit(1)


CONFIG      = loads(open("./config.json").read())
CONTRACTS   = CONFIG["contracts"]
SLEEP_INT   = CONFIG["sleep_int"]
SC_ROOT     = CONFIG["sc_root"]

################
# TIME AND SALES
################

async def etl_tas_coro(
    con_id:     str,
    checkpoint: int,
    price_adj:  float,
    loop:       int
) -> tuple[str, int]: # <-- Added return type hint for clarity
    fn = os.path.join(SC_ROOT, "Data", f"{con_id}.scid") # Use os.path.join
    processed_count_in_run = 0 # Track records processed in this specific run/loop iteration
    try:
        # Use 'with' statement for automatic file closing
        with open(fn, "rb") as fd:
            parse_tas_header(fd)
            # Determine starting position based on checkpoint
            start_pos = INTRADAY_HEADER_LEN + checkpoint * INTRADAY_REC_LEN
            fd.seek(start_pos)

            while True:
                current_pos = fd.tell() # Remember position before reading
                parsed = parse_tas(fd, 0) # Parse from current position (checkpoint handled by initial seek)
                count = len(parsed)

                if count:
                    # Calculate the actual offset based on the starting position for logging
                    log_offset = (current_pos - INTRADAY_HEADER_LEN) // INTRADAY_REC_LEN
                    print(f"[TAS] {con_id}: Processing {count} new records starting after offset {log_offset}.")
                    parsed_transformed = transform_tas(parsed, price_adj) # Renamed variable
                    load_tas(con_id, parsed_transformed)
                    processed_count_in_run += count # Accumulate processed count for this run
                else:
                    if not loop: # Only print "no new records" once in one-shot mode
                         print(f"[TAS] {con_id}: No new records found after offset {checkpoint + processed_count_in_run}.")
                    # In loop mode, it will just sleep and try again

                if loop:
                    await sleep(SLEEP_INT)
                    # For the next read in the loop, the file handle fd remains at the end of the last read
                else:
                    break # Exit loop if not in loop mode
    except FileNotFoundError:
        print(f"Error: Time & Sales file not found for contract '{con_id}' at {fn}. Skipping.")
        # Return original checkpoint if file not found
        return (con_id, checkpoint)
    except Exception as e: # Catch other potential errors during processing
        print(f"Error processing TAS for {con_id}: {e}")
        return (con_id, checkpoint) # Return original checkpoint on error

    # Return the updated checkpoint (original + newly processed)
    return (con_id, checkpoint + processed_count_in_run)

async def etl_tas(loop: int):
    coros = []
    for con_id, dfn in CONTRACTS.items():
        if dfn.get("tas", False): # Check if 'tas' key exists and is true
            checkpoint  = dfn.get("checkpoint_tas", 0) # Use get with default
            price_adj   = dfn.get("price_adj", 1.0)   # Use get with default
            coros.append(etl_tas_coro(con_id, checkpoint, price_adj, loop))
        else:
            print(f"[TAS] Skipping {con_id} as 'tas' is not enabled in config.")


    if not coros:
         print("[TAS] No contracts configured for Time & Sales processing.")
         return

    results = await gather(*coros)
    for res in results:
        if res: # Ensure result is not None or empty if errors occurred
            con_id, checkpoint = res
            # Ensure the contract still exists in CONFIG before updating (though it should)
            if con_id in CONFIG["contracts"]:
                CONFIG["contracts"][con_id]["checkpoint_tas"] = checkpoint
            else:
                 print(f"Warning: Contract {con_id} not found in CONFIG after processing TAS. Checkpoint not saved.")


##############
# MARKET DEPTH
##############

async def etl_depth_coro(
    con_id:     str,
    file:       str,
    checkpoint: int, # This is the starting record offset for THIS file
    price_adj:  float,
    loop:       int # Loop only applies if this is the *last* file chronologically
) -> tuple[str, int]: # Returns (filename, final_record_offset_in_this_file)
    fn = os.path.join(SC_ROOT, "Data", "MarketDepthData", file) # Use os.path.join
    processed_count_in_run = 0 # Track records processed in this specific run/loop iteration for this file
    try:
        # Use 'with' statement for automatic file closing
        with open(fn, "rb") as fd:
            parse_depth_header(fd)
            # Determine starting position based on checkpoint for this file
            start_pos = DEPTH_HEADER_LEN + checkpoint * DEPTH_REC_LEN
            fd.seek(start_pos)

            while True:
                current_pos = fd.tell() # Remember position before reading
                parsed = parse_depth(fd, 0) # Parse from current position
                count = len(parsed)

                if count:
                     # Calculate the actual offset based on the starting position for logging
                    log_offset = (current_pos - DEPTH_HEADER_LEN) // DEPTH_REC_LEN
                    print(f"[Depth] {con_id} (file {file}): Processing {count} new records starting after offset {log_offset}.")
                    parsed_transformed = transform_depth(parsed, price_adj) # Renamed variable
                    load_depth(con_id, parsed_transformed)
                    processed_count_in_run += count # Accumulate processed count for this run
                else:
                     if not loop: # Only print "no new records" once if not the looping file
                         print(f"[Depth] {con_id} (file {file}): No new records found after offset {checkpoint + processed_count_in_run}.")
                     # In loop mode for the last file, it will just sleep and try again

                if loop:
                    await sleep(SLEEP_INT)
                    # File handle remains at the end for the next loop iteration
                else:
                    break # Exit loop if not in loop mode or not the last file

    except FileNotFoundError:
        # This specific file was not found, which might be okay if checking multiple dates
        print(f"Warning: Depth file not found: {fn} for contract '{con_id}'. Skipping this specific file.")
        # Return original checkpoint (0 or value passed in) as no records were processed from this file
        return (file, checkpoint)
    except Exception as e:
        print(f"Error processing Depth for {con_id}, file {file}: {e}")
         # Return original checkpoint as processing failed for this file
        return (file, checkpoint)

     # Return the final record offset reached *in this file* (original + newly processed)
    return (file, checkpoint + processed_count_in_run)


async def etl_depth(loop: int):
    # Use os.path.join for cross-platform compatibility
    depth_data_path_os = os.path.join(SC_ROOT, "Data", "MarketDepthData")
    try:
        if not os.path.isdir(depth_data_path_os):
             print(f"Error: Market depth directory not found: {depth_data_path_os}")
             return # Exit if directory doesn't exist

        _, _, files = next(walk(depth_data_path_os))
    except StopIteration:
        print(f"Info: No files found in {depth_data_path_os}")
        files = [] # Ensure files is an empty list if directory is empty

    all_contract_coros = {} # Store coros per contract

    for con_id, dfn in CONTRACTS.items():
        if not dfn.get("depth", False): # Check if 'depth' processing is enabled
             print(f"[Depth] Skipping {con_id} as 'depth' is not enabled in config.")
             continue

        price_adj       = dfn.get("price_adj", 1.0)
        checkpoint_info = dfn.get("checkpoint_depth", {}) # Use get with default
        checkpoint_date = checkpoint_info.get("date", "") # Use get with default (e.g., "2025-04-01")
        checkpoint_rec  = checkpoint_info.get("rec", 0)   # Use get with default (record offset for checkpoint_date file)

        contract_files_to_parse = [] # List of (filename, starting_offset) tuples
        if checkpoint_date: # If we have a specific date checkpoint
             print(f"[Depth] {con_id}: Looking for files on or after date '{checkpoint_date}' with record checkpoint {checkpoint_rec}.")
        else:
             print(f"[Depth] {con_id}: Looking for all available depth files (no date checkpoint).")


        # Filter and sort relevant files for this contract
        relevant_files_sorted = []
        for file in files:
            if not file.endswith(".depth"):
                continue
            base_name = file[:-6]
            try:
                symbol_str, date_str = base_name.rsplit(".", 1)
                if not (len(date_str) == 10 and date_str[4] == '-' and date_str[7] == '-'): continue # Basic format check
                if symbol_str == con_id:
                    relevant_files_sorted.append((date_str, file)) # Store (date, filename) for sorting
            except ValueError:
                continue # Skip files with unexpected naming

        relevant_files_sorted.sort() # Sort by date string YYYY-MM-DD

        # Determine which files need processing based on checkpoint
        processing_needed = False
        for date_str, file in relevant_files_sorted:
            if date_str > checkpoint_date:
                 # This file is newer than the checkpoint date, process from beginning
                 contract_files_to_parse.append((file, 0))
                 processing_needed = True
            elif date_str == checkpoint_date:
                 # This file matches the checkpoint date, process from the checkpoint record offset
                 contract_files_to_parse.append((file, checkpoint_rec))
                 processing_needed = True
                 # Don't break here, subsequent files on the same date might exist? Unlikely but possible.
                 # Let's assume one file per day, but logic handles > case anyway.

        if not processing_needed and checkpoint_date:
             print(f"[Depth] No new depth files found for symbol {con_id} on or after date '{checkpoint_date}'.")
             continue
        elif not relevant_files_sorted:
              print(f"[Depth] No depth files found at all for symbol {con_id} in {depth_data_path_os}.")
              continue


        coros = [] # Coroutines for this specific contract's files
        last_file_for_contract = contract_files_to_parse[-1][0] if contract_files_to_parse else None

        for i, (file, start_offset) in enumerate(contract_files_to_parse):
            is_last_file = (file == last_file_for_contract)
            # Loop mode is enabled only for the chronologically last file *if* global loop is on
            mode = loop if is_last_file else 0
            print(f"[Depth] Adding job for {con_id}: File '{file}', StartOffset: {start_offset}, LoopMode: {mode}")
            coros.append(etl_depth_coro(con_id, file, start_offset, price_adj, mode))

        if coros:
             all_contract_coros[con_id] = {
                  "coros": coros,
                  "last_file": last_file_for_contract # Store the name of the last file intended for processing
             }

    # --- Run all coroutines after setting them up ---
    if not all_contract_coros:
         print("[Depth] No contracts configured or no new files found for depth processing.")
         return

    final_checkpoints = {} # Store the final checkpoint for each contract's last file

    for con_id, data in all_contract_coros.items():
         print(f"[Depth] Processing {len(data['coros'])} file(s) for {con_id}...")
         results = await gather(*data["coros"]) # Gather results for one contract at a time

         if results and data["last_file"]:
             # Find the result corresponding to the last file processed for this contract
             last_file_result = None
             for res_file, res_offset in results:
                  if res_file == data["last_file"]:
                       last_file_result = (res_file, res_offset)
                       break

             if last_file_result:
                 final_checkpoint_rec = last_file_result[1] # Get the final record offset from the last file's result

                 # Get the date from the last processed file's name
                 last_file_base = data["last_file"][:-6] # remove .depth
                 try:
                     new_checkpoint_date = last_file_base.rsplit(".", 1)[-1]

                     # Store the final checkpoint date and record count for this contract
                     final_checkpoints[con_id] = {
                         "date": new_checkpoint_date,
                         "rec": final_checkpoint_rec
                     }
                 except ValueError:
                      print(f"Warning: Could not extract date from last processed depth file '{data['last_file']}' for {con_id}. Checkpoint not fully updated.")
                      # Keep old date, update record count if possible
                      final_checkpoints[con_id] = {
                           "date": CONFIG["contracts"][con_id].get("checkpoint_depth", {}).get("date", ""),
                           "rec": final_checkpoint_rec
                      }
             else:
                  print(f"Warning: Could not find result for the expected last file '{data['last_file']}' for contract {con_id}.")

    # --- Update CONFIG with final checkpoints ---
    for con_id, checkpoint_data in final_checkpoints.items():
        if con_id in CONFIG["contracts"]:
            # Ensure 'checkpoint_depth' exists before updating
            if "checkpoint_depth" not in CONFIG["contracts"][con_id]:
                 CONFIG["contracts"][con_id]["checkpoint_depth"] = {} # Create if missing
            CONFIG["contracts"][con_id]["checkpoint_depth"]["date"] = checkpoint_data["date"]
            CONFIG["contracts"][con_id]["checkpoint_depth"]["rec"] = checkpoint_data["rec"]
            print(f"[Depth] Updated checkpoint for {con_id}: Date='{checkpoint_data['date']}', Rec={checkpoint_data['rec']}")
        else:
             print(f"Warning: Contract {con_id} not found in CONFIG after processing depth. Checkpoint not saved.")


async def main():
    start = time()
    loop_mode = 0 # Default to one-shot mode
    if len(argv) > 1:
        try:
            loop_mode = int(argv[1])
            if loop_mode not in [0, 1]:
                 raise ValueError("Loop mode must be 0 or 1.")
            print(f"Running in {'continuous' if loop_mode == 1 else 'one-shot'} mode.")
        except ValueError as e:
            print(f"Error: Invalid argument for loop mode. {e}. Defaulting to one-shot (0).")
            loop_mode = 0
    else:
         print("Info: No loop mode argument provided. Defaulting to one-shot (0). Use 'python etl.py 1' for continuous mode.")


    # Run TAS and Depth processing concurrently
    # Use try-except around gather to catch potential errors during concurrent execution
    try:
        await gather(etl_tas(loop_mode), etl_depth(loop_mode))
    except Exception as e:
        print(f"Error during concurrent execution of TAS/Depth tasks: {e}")


    # Save updated configuration
    try:
        with open("./config.json", "w") as fd:
            fd.write(dumps(CONFIG, indent=2))
        print("Configuration file updated with latest checkpoints.")
    except IOError as e:
         print(f"Error writing updated config file: {e}")

    # Commit and close DB connection
    try:
        DB_CON.commit()
        print("Database changes committed.")
    except Exception as e:
         print(f"Error committing database changes: {e}")
    finally:
         try:
              DB_CON.close()
              print("Database connection closed.")
         except Exception as e:
              print(f"Error closing database connection: {e}")


    print(f"ETL process finished. Elapsed time: {time() - start:.2f} seconds.")


if __name__ == "__main__":
    # Check if essential config keys exist
    if not SC_ROOT or not isinstance(CONTRACTS, dict):
         print("Error: 'sc_root' or 'contracts' missing/invalid in config.json. Exiting.")
         exit(1) # Use exit(1) to indicate error

    run(main())