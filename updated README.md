Below is an extensively updated README.md that you can use to document and guide users through setting up, running, and troubleshooting the Sierra Chart ETL pipeline.

---

# Sierra Chart Tick & Depth ETL Pipeline

This repository contains a set of Python tools to build and maintain a local SQLite database from Sierra Chart time & sales (tick) data and market depth records. The pipeline extracts data from Sierra Chart’s data files, transforms it according to user-defined parameters, and loads it into a structured SQLite database for further analysis, backtesting, or live monitoring.

> **Note:** Sierra Chart does not store time & sales data directly; instead, it records intraday data files (.scid) that can be interpreted as time & sales when configured to record one trade per bar. This repository also processes market depth data stored in .depth files.

---

## Table of Contents

- [Features](#features)
- [Repository Structure](#repository-structure)
- [Prerequisites](#prerequisites)
- [Sierra Chart Configuration](#sierra-chart-configuration)
  - [Time & Sales (Tick) Data](#time--sales-tick-data)
  - [Market Depth Data](#market-depth-data)
  - [Intraday File Update List](#intraday-file-update-list)
- [Configuration File (config.json)](#configuration-file-configjson)
- [ETL Pipeline (etl.py)](#etl-pipeline-etlpy)
  - [One-shot Mode vs. Continuous Mode](#one-shot-mode-vs-continuous-mode)
  - [Logging and Monitoring](#logging-and-monitoring)
- [Additional Utilities](#additional-utilities)
  - [Symbol File List Generator (update_file_list.py)](#symbol-file-list-generator-update_file_listpy)
  - [Synchronized Iterator (sym_it.py)](#synchronized-iterator-sym_itpy)
- [Troubleshooting](#troubleshooting)
- [License](#license)

---

## Features

- **ETL for Time & Sales:**  
  Parse Sierra Chart intraday (.scid) files to extract trade records (timestamp, price, quantity, side) and load them into a SQLite table.

- **ETL for Market Depth:**  
  Parse market depth (.depth) files to extract order book update events (timestamp, command, flags, number of orders, price, quantity) and store them in a separate table.

- **Continuous Monitoring:**  
  Run the ETL pipeline in continuous mode (live mode) to periodically poll the data files for new records and update the database.

- **Robust Filename Parsing & Checkpointing:**  
  Automatically handle filename variations and maintain checkpoints in the configuration to process only new data.

- **Logging:**  
  Extensive console logging helps you monitor processing activity, including record counts and offsets.

---

## Repository Structure

- **config.json:**  
  The configuration file that defines the contracts to process, file paths, and processing parameters.

- **db.py:**  
  Contains functions to create SQLite tables and insert records for time & sales and market depth.

- **etl.py:**  
  The main ETL (Extract, Transform, Load) script. This script reads Sierra Chart data files, applies transformations (like price adjustment), and loads the data into the SQLite database. It supports both one-shot and continuous (live) modes.

- **parsers.py:**  
  Contains parsing functions for the intraday (.scid) and market depth (.depth) file formats based on Sierra Chart’s documentation.

- **update_file_list.py:**  
  A utility to generate a list of contract symbols based on patterns, which can be used with Sierra Chart’s Intraday File Update List for bulk downloads.

- **sym_it.py:**  
  A synchronized iterator that combines time & sales and depth data for a given symbol and date, enabling sequential processing of market events.

- **README.md:**  
  This file.

---

## Prerequisites

- **Sierra Chart:**  
  Installed and configured with an active data feed that provides tick and market depth data.

- **Python 3:**  
  The pipeline uses Python 3. No additional packages are required beyond the Python standard library (except for numpy, which is used in the parsers).

- **SQLite:**  
  SQLite is used as the database. (It is integrated with Python via the `sqlite3` module.)

---

## Sierra Chart Configuration

### Time & Sales (Tick) Data

1. **Configure Tick-by-Tick Data:**
   - In Sierra Chart, go to **Global Settings » Data/Trade Service Settings**.
   - Set **Intraday Data Storage Time Unit** to **1 Tick**.
   - This will record each trade as a separate record in the `.scid` files.

2. **Verify Data Files:**
   - Open an intraday chart or check the Sierra Chart Data folder (usually `C:/SierraChart/Data/`).
   - The time & sales file should be named like `ESM25_FUT_CME.scid` (if you are processing the ESM25_FUT_CME contract).

### Market Depth Data

1. **Enable Depth Recording:**
   - In **Global Settings » Symbol Settings**, find your symbol and set **Record Market Depth Data** to “Yes.”
   - Ensure that Sierra Chart is connected so that it can write depth data to files.

2. **Historical Depth:**
   - Sierra Chart stores depth files in the `MarketDepthData` subfolder (e.g., `C:/SierraChart/Data/MarketDepthData/`).
   - Each depth file is named in the format: `Symbol.YYYY-MM-DD.depth` (e.g., `ESM25_FUT_CME.2025-03-24.depth`).

### Intraday File Update List

For bulk tracking without opening multiple charts:
- Use Sierra Chart’s **Intraday File Update List** to list all the symbols you want tracked.
- You can use the provided `update_file_list.py` to generate this list.

---

## Configuration File (config.json)

The `config.json` file specifies:
- **Contracts:**  
  A dictionary of contracts to process. Each contract entry includes:
  - **checkpoint_tas:** Record offset for time & sales.
  - **checkpoint_depth:** An object with `date` and `rec` (record offset) for depth data.
  - **price_adj:** The price adjustment multiplier (based on Sierra Chart’s “Real Time Price Multiplier”).
  - **tas / depth:** Booleans to enable processing of time & sales and/or depth data.

- **db_path:**  
  Path to the SQLite database file.

- **sc_root:**  
  Root folder of your Sierra Chart installation (used to locate the data files).

- **sleep_int:**  
  Sleep interval (in seconds) between polling cycles in continuous mode.  
  _Example config.json:_

```json
{
  "contracts": {
    "ESM25_FUT_CME": {
      "checkpoint_tas": 0,
      "checkpoint_depth": {
        "date": "",
        "rec": 0
      },
      "price_adj": 0.01,
      "tas": true,
      "depth": true
    }
  },
  "db_path": "C:\\Users\\endeg\\Downloads\\tick_db-master (1)\\tick_db-master\\tick.db",
  "sc_root": "C:/SierraChart",
  "sleep_int": 0.01
}
```

---

## ETL Pipeline (etl.py)

The main ETL script (`etl.py`) does the following:
- **Time & Sales Processing:**  
  Opens the `.scid` file, reads the header, parses records from a given offset (checkpoint), applies a price adjustment, and writes them into a SQLite table.
  
- **Market Depth Processing:**  
  Iterates through the files in the `MarketDepthData` folder matching the contract symbol, parses the header and records, applies transformations, and loads the data into a depth table.

- **Checkpointing:**  
  After processing, it updates `config.json` with the new checkpoint offsets and last processed depth file date. This ensures that on subsequent runs, only new records are processed.

### One-shot Mode vs. Continuous Mode

- **One-shot Mode (`python etl.py 0`):**  
  Reads and processes all new data once, then exits.
  
- **Continuous Mode (`python etl.py 1`):**  
  Runs indefinitely, polling the files at intervals defined by `sleep_int`.  
  Use logging messages to monitor activity.

### Logging and Monitoring

The updated version of `etl.py` includes extensive logging:
- Logs the number of records processed from each file.
- Indicates when no new records are found.
- Updates checkpoints are logged in the console.
  
This provides real-time feedback that the live pipeline is active. You can verify that new data is being processed when Sierra Chart writes updates to the files.

---

## Additional Utilities

### Symbol File List Generator (update_file_list.py)

This script helps you generate a list of contract symbols based on predefined patterns. You can run it with start and end contract expiry codes (e.g., `python update_file_list.py N23 Z23`) to produce a list. This list can then be pasted into Sierra Chart’s **Intraday File Update List**.

### Synchronized Iterator (sym_it.py)

The `sym_it.py` utility provides a synchronized iterator that merges time & sales and market depth events into a single chronological stream. This can be useful for:
- Reconstructing the order book alongside trades.
- Feeding data into a backtesting engine.
  
It supports methods like `set_ts()` to reset the starting timestamp and allows slicing of the event stream.

---

## Troubleshooting

- **No New Records:**  
  - Verify that Sierra Chart is connected to a live data feed.
  - Check file modified timestamps in `C:/SierraChart/Data/` and `.../MarketDepthData/`.
  - Ensure the instrument is actively trading.

- **Filename Mismatch:**  
  - Confirm that the contract name in `config.json` matches the file names. For example, if the files are named `ESM25_FUT_CME.scid` and `ESM25_FUT_CME.YYYY-MM-DD.depth`, the contract key must be `ESM25_FUT_CME`.
  
- **Database Verification:**  
  - Use an SQLite browser to inspect the tables and verify that data is being appended over time.
  
- **Logging:**  
  - The log messages printed to the console help you see how many records are processed. If you see "No new records found" repeatedly and you expect activity, double-check the Sierra Chart data feed and file updates.
  
- **Process Cancellation:**  
  - In continuous mode, you can stop the process with Ctrl+C. The script handles interruptions gracefully.

---

## License

*Include your license information here (e.g., MIT License).*

---

This updated README should give you (and any future users) a clear and thorough overview of the project, how to set it up, run the ETL pipeline in both one-shot and continuous modes, and how to troubleshoot common issues. If you have further questions or need additional modifications, feel free to ask!