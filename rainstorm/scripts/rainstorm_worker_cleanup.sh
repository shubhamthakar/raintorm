#!/bin/bash

# Define the name or partial name of the Python script to stop
PYTHON_SCRIPT_PARTIAL_NAME="worker.py"

# Loop over host-01 to host-10
for i in $(seq -w 01 10); do
  HOST="fa24-cs425-69$i.cs.illinois.edu"
  echo "Connecting to $HOST and stopping the Python script..."

  # SSH into each host and kill the processes running the Python script
  ssh "$HOST" "sudo pkill -f 'python3 .*${PYTHON_SCRIPT_PARTIAL_NAME}.*'" && \
  echo "Python script stopped on $HOST" || \
  echo "No matching process found on $HOST"
done

