#!/bin/bash

# Define the name or path of the Python script to stop
PYTHON_SCRIPT_NAME="/home/chaskar2/distributed-logger/SWIM/process.py"

# Check if at least one node ID is provided
if [ $# -eq 0 ]; then
  echo "Usage: $0 <node_id_1> <node_id_2> ... <node_id_N>"
  exit 1
fi

# Loop over the provided node IDs
for node_id in "$@"; do
  HOST="fa24-cs425-69$node_id.cs.illinois.edu"
  echo "Connecting to $HOST and stopping the Python script..."

  # SSH into each host and kill the processes running the Python script
  ssh "$HOST" "pkill -f 'python .*hyDFS.py.*'"
  ssh "$HOST" "pkill -f 'python3 .*worker.py.*'"

  echo "Python script stopped on $HOST"
done

