# Festo SmartLine – OPC UA Data Collector
This service collects real-time production data from the Festo 5G Smart Lab using OPC UA and stores it in a local SQL database on a Raspberry Pi. It runs continuously and serves as the data acquisition layer for KPI computation and further analytics.

### Features
- Connects to OPC UA server on the production line
- Reads and timestamps process values
- Stores raw data in a local SQL database
- Automatic reconnection and error handling
- Designed for 24/7 operation on Raspberry Pi
