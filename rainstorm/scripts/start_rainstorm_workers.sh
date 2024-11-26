#!/bin/bash

# Path to worker.py on the remote server
WORKER_SCRIPT_PATH="/path/to/worker.py"

# Default port for the gRPC server
PORT=5000

# Validate input arguments
if [ $# -lt 4 ]; then
  echo "Usage: $0 <mapping_dict> <src_file> <dest_file> <server_list...>"
  exit 1
fi

MAPPING_DICT=$1
SRC_FILE=$2
DEST_FILE=$3
shift 3  # Remove first three arguments to process server list

# Check if server list is provided
if [ $# -eq 0 ]; then
  echo "No servers specified. Exiting."
  exit 1
fi

# Loop over the provided servers
for SERVER in "$@"; do
  echo "Starting worker.py on $SERVER"

  # Run the worker script on the remote server
  ssh "$SERVER" << EOF
    python3 "$WORKER_SCRIPT_PATH" \
      --port "$PORT" \
      --mapping "$MAPPING_DICT" \
      --src_file "$SRC_FILE" \
      --dest_file "$DEST_FILE" &
EOF
done

echo "All specified workers have been started."
