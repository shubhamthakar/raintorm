#!/bin/bash

# Path to worker.py on the remote server
WORKER_SCRIPT_PATH="/path/to/worker.py"

# Port to run the gRPC server on
PORT=5000

# Parameters for the worker script
MAPPING_DICT="/path/to/mapping.json"
SRC_FILE="/path/to/src_file"
DEST_FILE="/path/to/dest_file"

# Check if server list is passed as arguments
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
