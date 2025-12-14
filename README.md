# Festo SmartLine – OPC UA Data Collection Service

## Overview

This repository contains a **Python-based OPC UA data collection service** developed for the **Festo 5G Smart Lab / SmartLine** production system.

The service connects to multiple OPC UA-enabled production modules, continuously reads operational and production data, and stores this data in a **local PostgreSQL database** for further analysis, visualization, and KPI calculation.

The system is designed to run **headless on a Raspberry Pi** and start automatically on boot.

---

## Key Features

- Connects to multiple OPC UA servers (one per production module)
- Periodically reads process and production data
- Stores structured time-series data in PostgreSQL
- Modular and extensible architecture
- Designed for continuous operation (24/7 data collection)
- Acts as a data source for KPI calculation and Power BI dashboards

---

## System Architecture

1. **OPC UA Clients**  
   Each production module exposes data via OPC UA.

2. **Data Collection Service**  
   - Connects to all configured modules  
   - Reads data at fixed intervals  
   - Normalizes and timestamps values  

3. **PostgreSQL Database**  
   - Stores raw and processed data  
   - Serves as the single source of truth  

4. **Downstream Systems**  
   - KPI calculation scripts  
   - Power BI dashboards  
   - Data export tools  

---

## Requirements

### Software

- Python **3.10+**
- PostgreSQL **14+**
- Network access to OPC UA-enabled modules

### Python Dependencies

Install dependencies using:

```
pip install -r requirements.txt
```

---

## Database Setup

- PostgreSQL must be installed and running
- The database service should start automatically on boot
- Connection credentials are read from a local credentials file

Ensure the PostgreSQL user has permission to:
- Create tables
- Insert and update records

---

## Running the Service

To start the data collection manually:

```
python main.py
```

For production deployment, it is recommended to configure the script as a **systemd service** so it starts automatically when the Raspberry Pi boots.

---

## Deployment Notes

- Intended for **headless Raspberry Pi** operation
- Tested with Ubuntu Server
- Stable network and power supply recommended for long-term operation
- Designed for continuous background execution

---

## Notes & Scope

- This repository focuses **only on data collection**
- KPI calculation and visualization are handled in **separate repositories**
- Some configuration and naming conventions originate from early project stages but remain functional

---
