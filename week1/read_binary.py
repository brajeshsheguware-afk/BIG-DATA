"""
read_binary_logs.py
-------------------

Reads one binary partition file created by the Amazon Prime
Log Harvester and converts it back into human-readable logs.

Usage:

python read_binary_logs.py prime_partitions/prime-stream-server_ERROR.bin

"""

import struct
import sys

# ==========================================================
# Binary Level Mapping
# ==========================================================

LEVEL_CODE = {
    "DEBUG": 0,
    "INFO": 1,
    "WARNING": 2,
    "ERROR": 3,
}

CODE_LEVEL = {v: k for k, v in LEVEL_CODE.items()}


# ==========================================================
# Read Binary Records
# ==========================================================

def read_records(filepath):

    with open(filepath, "rb") as file:

        data = file.read()

    offset = 0

    records = []

    while offset < len(data):

        # First 4 bytes contain record length
        (record_length,) = struct.unpack_from("!I", data, offset)

        offset += 4

        record = data[offset: offset + record_length]

        offset += record_length

        # Read Timestamp, Level and Service Length

        timestamp_bytes, level_byte, service_length = struct.unpack_from(
            "!19sBH",
            record,
            0
        )

        position = 22

        service = record[
            position:
            position + service_length
        ].decode("utf-8")

        position += service_length

        (message_length,) = struct.unpack_from(
            "!H",
            record,
            position
        )

        position += 2

        message = record[
            position:
            position + message_length
        ].decode("utf-8")

        records.append({

            "timestamp": timestamp_bytes.decode("ascii").strip(),

            "level": CODE_LEVEL[level_byte],

            "service": service,

            "message": message

        })

    return records


# ==========================================================
# Main Program
# ==========================================================

if __name__ == "__main__":

    if len(sys.argv) != 2:

        print(
            "Usage:\n"
            "python read_binary_logs.py "
            "prime_partitions/<partition_file>.bin"
        )

        sys.exit(1)

    file_path = sys.argv[1]

    decoded_logs = read_records(file_path)

    print("\n" + "=" * 65)
    print("        AMAZON PRIME BINARY LOG VIEWER")
    print("=" * 65)

    print(f"\nDecoded {len(decoded_logs)} log records:\n")

    for log in decoded_logs:

        print(
            f"{log['timestamp']} | "
            f"{log['level']} | "
            f"{log['service']} | "
            f"{log['message']}"
        )