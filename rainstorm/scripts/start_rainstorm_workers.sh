#!/bin/bash

# Path to worker.py on the remote server
WORKER_SCRIPT_PATH="/home/chaskar2/distributed-logger/rainstorm/worker.py"

# Validate input arguments
if [ $# -lt 5 ]; then
  echo "Usage: $0 <mapping_dict_json> <src_file> <dest_file> <ports_list> <server_list...>"
  exit 1
fi

MAPPING_DICT_JSON=$1  # JSON string for mapping
SRC_FILE=$2
DEST_FILE=$3
PORTS=$4  # Comma-separated list of ports
shift 4  # Remove first four arguments to process server list

IFS=',' read -r -a PORT_ARRAY <<< "$PORTS"  # Convert ports string to array

# Check if server list is provided
if [ $# -eq 0 ]; then
  echo "No servers specified. Exiting."
  exit 1
fi

# Loop over the provided servers and assign ports
i=0
for SERVER in "$@"; do
  PORT=${PORT_ARRAY[$i]:-5000}  # Use default port 5000 if no port specified
  echo "Starting worker.py on $SERVER with port $PORT"

  # Run the worker script on the remote server
  ssh -n "$SERVER" "nohup python3 $WORKER_SCRIPT_PATH \
    --port $PORT \
    --mapping '$MAPPING_DICT_JSON' \
    --src_file $SRC_FILE \
    --dest_file $DEST_FILE > /dev/null 2>&1 &" &

  i=$((i + 1))
done

echo "All specified workers have been started."
