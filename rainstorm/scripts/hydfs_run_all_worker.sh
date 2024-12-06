#!/bin/bash

# Define the path to the Python script on the remote machines
PYTHON_SCRIPT_PATH="/home/chaskar2/distributed-logger/rainstorm/hyDFS.py"

# Loop over host-01 to host-10
for i in $(seq -w 02 10); do
  HOST="fa24-cs425-69$i.cs.illinois.edu"
  echo "Connecting to $HOST and running the Python script..."

  # SSH into each host and run the Python script
  ssh "$HOST" "nohup python -u '$PYTHON_SCRIPT_PATH' --proto_period 20 --ping_timeout 10 --drop_percent 0 --suspicion > 'hyDFS_process_log_$i' 2>&1 &"

  # Check if ssh command was successful
  if [ $? -ne 0 ]; then
    echo "Error: SSH command failed on $HOST" >&2
    exit 1
  fi

  echo "Python script executed on $HOST"
done

