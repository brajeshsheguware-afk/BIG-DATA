# AMAZON PRIME VIDEO LOG MONITORING SYSTEM USING BIG DATA ANALYTICS

## PROJECT OVERVIEW

This project simulates a real-time log monitoring system for Amazon Prime Video using Python. It creates three backend servers that continuously generate streaming-related logs. A multi-threaded Log Harvester collects these logs through TCP sockets, validates them using Regular Expressions (Regex), converts them into structured payloads, partitions them dynamically, and stores them as binary files. A Binary Log Reader is used to decode the stored binary logs back into a readable format.

---

## FEATURES

- Real-Time Log Monitoring
- Multi-threaded Log Harvesting
- TCP Socket Communication
- Custom Socket Buffer Slicing
- Regex-Based Log Validation
- Structured Payload Creation
- Dynamic Binary Partitioning
- Binary Log Storage
- Binary Log Reader
- Live Log Statistics

---

## TECHNOLOGIES USED

- Python 3
- Socket Programming
- Multithreading
- Regular Expressions (Regex)
- Struct Module
- Binary File Handling
- OS Module

---

## PROJECT STRUCTURE

```text
AmazonPrime_LogMonitoring/

│── log_server_simulator.py
│── log_harvester_daemon.py
│── read_binary_logs.py
│── README.md
│
└── prime_partitions/
```

---

## SIMULATED SERVERS

- Prime Login Server
- Prime Streaming Server
- Prime Content Server

Each server continuously generates streaming logs and sends them to the Log Harvester.

---

## PROJECT WORKFLOW

```text
Prime Backend Servers
        │
        ▼
TCP Socket Communication
        │
        ▼
Multi-threaded Log Harvester
        │
        ▼
Socket Stream Buffer Slicing
        │
        ▼
Regex Validation
        │
        ▼
Structured Payload Creation
        │
        ▼
Dynamic Binary Partitioning
        │
        ▼
Binary Storage (.bin)
        │
        ▼
Binary Log Reader
```

---

## HOW TO RUN

### Step 1: Start the Log Server

```bash
python log_server_simulator.py
```

### Step 2: Start the Log Harvester

Open another terminal and run:

```bash
python log_harvester_daemon.py
```

### Step 3: Read Stored Binary Logs

```bash
python read_binary_logs.py prime_partitions/prime-login-server_INFO.bin
```

Replace the filename with any available binary partition.

---

## SAMPLE OUTPUT

```text
2026-07-10 10:20:15 | INFO | prime-login-server | User logged in successfully

2026-07-10 10:20:19 | WARNING | prime-stream-server | Streaming quality reduced due to network fluctuation

2026-07-10 10:20:24 | ERROR | prime-content-server | Failed to load movie metadata
```

---

## PROJECT MODULES

### 1. log_server_simulator.py

- Simulates Amazon Prime backend servers
- Generates streaming-related logs
- Sends logs through TCP sockets

### 2. log_harvester_daemon.py

- Connects to all simulated servers
- Reads log streams in real time
- Performs socket slicing
- Validates logs using Regex
- Creates structured payloads
- Stores logs in binary partition files

### 3. read_binary_logs.py

- Reads binary partition files
- Decodes binary records
- Displays readable log information

---

## BIG DATA ANALYTICS CONCEPTS USED

- Real-Time Data Ingestion
- Stream Processing
- TCP Socket Communication
- Multi-threading
- Regular Expression Validation
- Dynamic Data Partitioning
- Binary File Storage
- Log Aggregation

---

## FUTURE ENHANCEMENTS

- Web Dashboard for Live Monitoring
- Cloud-Based Log Storage
- Database Integration
- Log Search and Filtering
- Email Alert System
- Performance Analytics
- AI-Based Anomaly Detection

---

## AUTHOR

K.BRAJESH SHEGUWARE
II-BCA-'B'

---
