"""
log_server_simulator.py
-----------------------
Pretends to be 3 high-velocity Amazon Prime backend servers.

1. Prime Login Server
2. Prime Stream Server
3. Prime Content Server

Each server acts like a small TCP server. Once the Log Harvester
connects, it continuously streams log messages at random intervals.

Run this file FIRST.
"""

import socket
import threading
import random
import time
from datetime import datetime

# ==========================================================
# Simulated Amazon Prime Servers (Modified Port Numbers)
# ==========================================================

BRANCHES = [
    ("prime-login-server", 9101),
    ("prime-stream-server", 9102),
    ("prime-content-server", 9103),
]

# Available Log Levels
LEVELS = [
    "INFO",
    "WARNING",
    "ERROR",
    "DEBUG"
]

# Sample Movie Titles
MOVIES = [
    "The Boys",
    "Reacher",
    "Mirzapur",
    "Panchayat",
    "Fallout",
    "The Tomorrow War",
    "Jack Ryan",
    "Citadel"
]

# Available Streaming Qualities
QUALITIES = [
    "SD",
    "HD",
    "Full HD",
    "4K"
]

# Subscription Plans
PLANS = [
    "Prime Monthly",
    "Prime Annual",
    "Student Prime"
]

# Indian Cities
LOCATIONS = [
    "Chennai",
    "Bangalore",
    "Hyderabad",
    "Mumbai",
    "Delhi",
    "Pune",
    "Kolkata",
    "Ahmedabad"
]

# ==========================================================
# Message Templates
# ==========================================================

MESSAGE_TEMPLATES = {

    "INFO": [

        "User#{uid} logged in successfully from {city}",

        "User#{uid} started streaming '{movie}' in {quality}",

        "'{movie}' added to watchlist by User#{uid}",

        "Playback completed for '{movie}'",

        "Subscription upgraded to {plan}"

    ],

    "WARNING": [

        "Buffering detected while streaming '{movie}'",

        "Streaming quality automatically reduced to {quality}",

        "High streaming traffic detected in {city}",

        "Content synchronization taking longer than expected"

    ],

    "ERROR": [

        "Streaming interrupted for '{movie}'",

        "Subscription validation failed for User#{uid}",

        "Content server timeout while loading '{movie}'",

        "Playback failed due to network issue"

    ],

    "DEBUG": [

        "Cache miss while loading '{movie}'",

        "Retrying content request for '{movie}'",

        "Background metadata synchronization completed",

        "Session token refreshed for User#{uid}"

    ]
}
# ==========================================================
# Build One Log Entry
# ==========================================================

def build_log_line(branch_name):
    """Creates one well-formed Amazon Prime log line."""

    level = random.choice(LEVELS)

    uid = random.randint(1000, 9999)
    movie = random.choice(MOVIES)
    city = random.choice(LOCATIONS)
    quality = random.choice(QUALITIES)
    plan = random.choice(PLANS)

    message = random.choice(MESSAGE_TEMPLATES[level]).format(
        uid=uid,
        movie=movie,
        city=city,
        quality=quality,
        plan=plan
    )

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Pipe-separated format (used by regex in the harvester)
    return f"{timestamp} | {level} | {branch_name} | {message}\n"


# ==========================================================
# Handle Connected Harvester
# ==========================================================

def handle_client(conn, branch_name):
    """Continuously sends log lines to the connected harvester."""

    print(f"[{branch_name}] Harvester connected. Streaming logs...")

    try:
        while True:

            log_line = build_log_line(branch_name)

            conn.sendall(log_line.encode("utf-8"))

            # Simulate high-speed streaming logs
            time.sleep(random.uniform(0.05, 0.40))

            # Occasionally send a corrupted log
            if random.random() < 0.05:
                conn.sendall(b"CORRUPTED_LOG_ENTRY\n")

    except (BrokenPipeError, ConnectionResetError):

        print(f"[{branch_name}] Harvester disconnected.")

    finally:

        conn.close()


# ==========================================================
# Run One Amazon Prime Server
# ==========================================================

def run_branch_server(branch_name, port):

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    server_socket.setsockopt(
        socket.SOL_SOCKET,
        socket.SO_REUSEADDR,
        1
    )

    server_socket.bind(("127.0.0.1", port))

    server_socket.listen(1)

    print(f"[{branch_name}] Listening on Port {port}")

    while True:

        conn, addr = server_socket.accept()

        client_thread = threading.Thread(
            target=handle_client,
            args=(conn, branch_name),
            daemon=True
        )

        client_thread.start()


# ==========================================================
# Main Program
# ==========================================================

if __name__ == "__main__":

    print("=" * 60)
    print("        AMAZON PRIME LOG SERVER SIMULATOR")
    print("=" * 60)

    threads = []

    for branch_name, port in BRANCHES:

        thread = threading.Thread(
            target=run_branch_server,
            args=(branch_name, port),
            daemon=True
        )

        thread.start()

        threads.append(thread)

    print("\nAll Amazon Prime servers are running.")
    print("Waiting for Log Harvester to connect...\n")

    try:
        while True:
            time.sleep(1)

    except KeyboardInterrupt:

        print("\nShutting down simulator.")