#!/bin/bash

# Define the path to the directory containing files to delete
TARGET_DIR="/home/chaskar2/distributed-logger/hyDFS/filesystem"

# Loop over host-01 to host-10
for i in $(seq -w 01 10); do
  HOST="fa24-cs425-69$i.cs.illinois.edu"
  echo "Connecting to $HOST and deleting files in $TARGET_DIR..."

  # SSH into each host and delete all files in the target directory
  ssh "$HOST" "rm -f '$TARGET_DIR'/*"

  # Check if the ssh and delete command were successful
  if [ $? -ne 0 ]; then
    echo "Error: Failed to delete files on $HOST" >&2
    exit 1
  fi

  echo "All files in $TARGET_DIR deleted on $HOST"
done

