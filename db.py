# db.py (Added import os and WAL mode)

import sqlite3
import logging
import os # <-- IMPORT ADDED HERE
from json import loads
from typing import List

# Basic logging setup for this module (optional but helpful)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load config and DB Path
try:
    # Assuming config.json is in the parent directory relative to src/tick_db
    # This path might need adjustment depending on where etl.py runs from
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, "..", "..", "config.json") # Go up two levels for config.json
    if not os.path.exists(config_path):
         # Fallback: Check if config.json is in the current working directory (where etl.py might be run from)
         config_path = os.path.join(os.getcwd(), "config.json")
         if not os.path.exists(config_path):
             raise FileNotFoundError(f"config.json not found relative to db.py ({os.path.join(script_dir, '..', '..')}) or current working directory ({os.getcwd()})")

    logger.info(f"Loading configuration from: {config_path}")
    CONFIG = loads(open(config_path, "r").read())
    DB_PATH = CONFIG["db_path"]
except FileNotFoundError as e:
    logger.error(f"Could not find config.json: {e}")
    raise
except KeyError:
    logger.error(f"db_path not found in config.json: {config_path}")
    raise
except Exception as e:
     logger.error(f"Error loading config for db.py: {e}")
     raise


# --- Create Global Connection and Enable WAL ---
DB_CON = None # Initialize as None
try:
    logger.info(f"Connecting to database: {DB_PATH}")
    # Add a short timeout for the writer connection as well
    DB_CON = sqlite3.connect(DB_PATH, timeout=1.0) # e.g., 1 second timeout
    logger.info("Executing PRAGMA journal_mode=WAL;")
    DB_CON.execute("PRAGMA journal_mode=WAL;")
    # Verify WAL mode (optional)
    # mode = DB_CON.execute("PRAGMA journal_mode;").fetchone()
    # logger.info(f"Database journal mode set to: {mode}")
except sqlite3.Error as e:
    logger.error(f"Failed to connect to database or set WAL mode: {e}")
    # If connection failed, subsequent calls using DB_CON will fail
    raise # Stop if DB connection fails fundamentally

# --- END WAL Modification ---


TOUCHED = set() # Stores tables checked/created in this session


def load_depth(con_id: str, rs: List):
    """ Creates depth table if needed and inserts records """
    if not DB_CON:
        logger.error("Database connection (DB_CON) not available in load_depth.")
        return # Cannot proceed without connection

    table_name = f"{con_id}_depth"

    try:
        # Check and Create Table if not exists
        if table_name not in TOUCHED:
            DB_CON.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    timestamp   INTEGER PRIMARY KEY, -- Added PRIMARY KEY
                    command     INTEGER,
                    flags       INTEGER,
                    num_orders  INTEGER,
                    price       REAL,
                    qty         INTEGER
                );
                """
            )
            # Create index for faster querying by timestamp (essential for live polling)
            DB_CON.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_ts ON {table_name}(timestamp);")
            TOUCHED.add(table_name) # Mark as checked for this session

        # Insert records
        statement = f"""
            INSERT OR IGNORE INTO {table_name} ( -- Use IGNORE to skip duplicates if timestamp is PRIMARY KEY
                timestamp, command, flags, num_orders, price, qty
            )
            VALUES (?, ?, ?, ?, ?, ?);
        """
        cursor = DB_CON.cursor()
        cursor.executemany(statement, rs)
        DB_CON.commit() # Commit changes after inserting
        logger.debug(f"Inserted/Ignored {len(rs)} records into {table_name}.")

    except sqlite3.Error as e:
        logger.error(f"Database error during load_depth for {table_name}: {e}")
        try: DB_CON.rollback() # Rollback on error
        except Exception as rb_e: logger.error(f"Rollback failed: {rb_e}")


def load_tas(con_id: str, rs: List):
    """ Creates time & sales table if needed and inserts records """
    if not DB_CON:
        logger.error("Database connection (DB_CON) not available in load_tas.")
        return # Cannot proceed without connection

    table_name = f"{con_id}_tas"

    try:
        # Check and Create Table if not exists
        if table_name not in TOUCHED:
            DB_CON.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    timestamp   INTEGER, -- Consider adding (timestamp, price, side) PRIMARY KEY if needed
                    price       REAL,
                    qty         INTEGER,
                    side        INTEGER
                );
                """
            )
            # Create index for faster querying by timestamp (essential for live polling)
            DB_CON.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_ts ON {table_name}(timestamp);")
            TOUCHED.add(table_name) # Mark as checked for this session

        # Insert records
        statement = f"""
            INSERT INTO {table_name} ( -- Consider OR IGNORE if duplicates are an issue
                timestamp, price, qty, side
            )
             VALUES (?, ?, ?, ?);
        """
        cursor = DB_CON.cursor()
        cursor.executemany(statement, rs)
        DB_CON.commit() # Commit changes after inserting
        logger.debug(f"Inserted {len(rs)} records into {table_name}.")

    except sqlite3.Error as e:
        logger.error(f"Database error during load_tas for {table_name}: {e}")
        try: DB_CON.rollback() # Rollback on error
        except Exception as rb_e: logger.error(f"Rollback failed: {rb_e}")


# Optional: Function to close the global connection if needed on script exit
# This should ideally be called explicitly by the script using this module (e.g., etl.py)
# Using atexit with global variables can sometimes be tricky.
def close_global_db():
    global DB_CON
    if DB_CON:
        logger.info("Closing global database connection.")
        try:
            DB_CON.close()
        except Exception as e:
             logger.error(f"Error closing database connection: {e}")
        finally:
             DB_CON = None

# Example: If etl.py imports this module as 'db_utils', it could call db_utils.close_global_db() at the end.