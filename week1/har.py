"""
log_harvester_daemon.py
-----------------------
Amazon Prime Log Harvester Daemon

This program:

1. Opens TCP socket connections to all Amazon Prime servers
2. Collects log streams in real time
3. Performs custom socket slicing
4. Validates logs using Regular Expressions
5. Creates structured payloads
6. Stores logs into partitioned binary files

Run log_server_simulator.py FIRST,
then run this file.
"""

import socket
import threading
import re
import struct
import os
import time
from collections import defaultdict

# ==========================================================
# Amazon Prime Servers
# ==========================================================

BRANCHES = [
    ("prime-login-server", 9101),
    ("prime-stream-server", 9102),
    ("prime-content-server", 9103),
]

HOST = "127.0.0.1"

PARTITION_DIR = "prime_partitions"

# ==========================================================
# Regular Expression Validation
# ==========================================================

LOG_PATTERN = re.compile(
    r"^(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s*\|\s*"
    r"(?P<level>INFO|WARNING|ERROR|DEBUG)\s*\|\s*"
    r"(?P<service>[\w\-]+)\s*\|\s*"
    r"(?P<message>.+)$"
)

# ==========================================================
# Binary Encoding
# ==========================================================

LEVEL_CODE = {
    "DEBUG": 0,
    "INFO": 1,
    "WARNING": 2,
    "ERROR": 3,
}

CODE_LEVEL = {v: k for k, v in LEVEL_CODE.items()}

# ==========================================================
# Dynamic Partition Management
# ==========================================================

partition_files = {}

partition_locks = defaultdict(threading.Lock)

master_lock = threading.Lock()

stats_lock = threading.Lock()

stats = defaultdict(int)


def get_partition_file(service, level):
    """
    Creates one binary partition file
    for every (Server + Log Level) combination.
    """

    key = (service, level)

    with master_lock:

        if key not in partition_files:

            os.makedirs(PARTITION_DIR, exist_ok=True)

            filename = os.path.join(
                PARTITION_DIR,
                f"{service}_{level}.bin"
            )

            partition_files[key] = open(filename, "ab")

            print(f"[Partition Created] {filename}")

        return partition_files[key]


# ==========================================================
# Binary Record Builder
# ==========================================================

def encode_record(timestamp, level, service, message):

    timestamp_bytes = timestamp.encode("ascii").ljust(19, b" ")[:19]

    level_byte = LEVEL_CODE[level]

    service_bytes = service.encode("utf-8")

    message_bytes = message.encode("utf-8")

    header = struct.pack(
        "!19sBH",
        timestamp_bytes,
        level_byte,
        len(service_bytes)
    )

    middle = struct.pack(
        "!H",
        len(message_bytes)
    )

    return (
        header
        + service_bytes
        + middle
        + message_bytes
    )
    # ==========================================================
# Store Structured Payload
# ==========================================================

def write_payload(record):
    """
    Converts the structured payload into a binary record
    and writes it to the correct partition file.
    """

    binary_record = encode_record(
        record["timestamp"],
        record["level"],
        record["service"],
        record["message"]
    )

    # Prefix record length (4 bytes)
    length_prefix = struct.pack("!I", len(binary_record))

    key = (record["service"], record["level"])

    partition_file = get_partition_file(
        record["service"],
        record["level"]
    )

    with partition_locks[key]:
        partition_file.write(length_prefix + binary_record)
        partition_file.flush()


# ==========================================================
# Validate Log and Build Payload
# ==========================================================

def process_line(raw_line, branch_name):
    """
    Validates each log using Regex.
    Valid logs become structured payloads.
    Invalid logs are rejected.
    """

    match = LOG_PATTERN.match(raw_line)

    if not match:

        with stats_lock:
            stats[(branch_name, "REJECTED")] += 1

        return

    payload = {

        "timestamp": match.group("timestamp"),

        "level": match.group("level"),

        "service": match.group("service"),

        "message": match.group("message")

    }

    write_payload(payload)

    with stats_lock:

        stats[(branch_name, payload["level"])] += 1


# ==========================================================
# Harvest Logs from One Server
# ==========================================================

def harvest_from_branch(branch_name, port):
    """
    Connects to one Amazon Prime server and continuously
    reads incoming bytes.

    Performs custom socket slicing by buffering incoming
    data until a complete log line ('\\n') is received.
    """

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    sock.connect((HOST, port))

    print(f"[{branch_name}] Connected on Port {port}")

    buffer = b""

    try:

        while True:

            chunk = sock.recv(4096)

            if not chunk:
                print(f"[{branch_name}] Connection closed.")
                break

            # Add received bytes into buffer
            buffer += chunk

            # Extract complete log lines
            while b"\n" in buffer:

                line_bytes, buffer = buffer.split(b"\n", 1)

                try:
                    line = line_bytes.decode("utf-8").strip()

                except UnicodeDecodeError:
                    continue

                if line:
                    process_line(line, branch_name)

    finally:

        sock.close()
    # ==========================================================
# Live Statistics Dashboard
# ==========================================================

def print_stats_periodically():
    """
    Displays live ingestion statistics every 3 seconds.
    """

    while True:

        time.sleep(3)

        with stats_lock:

            if not stats:
                continue

            print("\n" + "=" * 60)
            print("          AMAZON PRIME LIVE LOG DASHBOARD")
            print("=" * 60)

            for (branch, level), count in sorted(stats.items()):
                print(f"{branch:25s} {level:10s} : {count}")

            print("=" * 60 + "\n")


# ==========================================================
# Main Program
# ==========================================================

if __name__ == "__main__":

    print("=" * 65)
    print("        AMAZON PRIME LOG HARVESTER DAEMON")
    print("=" * 65)

    threads = []

    # One thread for each Amazon Prime server
    for branch_name, port in BRANCHES:

        worker = threading.Thread(
            target=harvest_from_branch,
            args=(branch_name, port),
            daemon=True
        )

        worker.start()

        threads.append(worker)

    # Live statistics thread
    stats_thread = threading.Thread(
        target=print_stats_periodically,
        daemon=True
    )

    stats_thread.start()

    print("\nHarvester is running...")
    print("Collecting logs from all Amazon Prime servers.")
    print("Press Ctrl+C to stop.\n")

    try:

        while True:
            time.sleep(1)

    except KeyboardInterrupt:

        print("\nStopping Log Harvester...")

        for file_handle in partition_files.values():
            file_handle.close()

        print("All partition files closed successfully.")
        print("Amazon Prime Log Harvester stopped.")