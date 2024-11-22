#!/bin/bash

# Define the name or path of the Python script to stop
PYTHON_SCRIPT_NAME="/home/chaskar2/distributed-logger/hyDFS/hyDFS.py"
# Loop over host-01 to host-10
for i in $(seq -w 01 10); do
  HOST="fa24-cs425-69$i.cs.illinois.edu"
  echo "Connecting to $HOST and stopping the Python script..."

  # SSH into each host and kill the processes running the Python script
  ssh "$HOST" "sudo pkill -f 'python .*hyDFS.py.*'"

  echo "Python script stopped on $HOST"
done
