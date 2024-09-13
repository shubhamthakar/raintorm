#!/bin/bash

# Define the path to the Python script on the remote machines
PYTHON_SCRIPT_PATH="/home/chaskar2/distributed-logger/socket_server.py"

# Loop over host-01 to host-10
for i in $(seq -w 01 10); do
  HOST="fa24-cs425-69$i.cs.illinois.edu"
  echo "Connecting to $HOST and running the Python script..."
  
  # SSH into each host and run the Python script
  ssh "$HOST" "nohup python '$PYTHON_SCRIPT_PATH' > 'socket_server_log_$i' 2>&1 &" 
  
  echo "Python script executed on $HOST"
done

