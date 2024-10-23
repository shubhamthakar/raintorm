#!/bin/bash


# Loop over host-01 to host-10
for i in $(seq -w 01 10); do
  HOST="fa24-cs425-69$i.cs.illinois.edu"
  echo "Connecting to $HOST and running the Python script..."

  # SSH into each host and run the Python script
  ssh "$HOST" "pip install msgpack"
  if [ $? -eq 0 ]; then
    echo "Successfully installed msgpack on $server"
  else
    echo "Failed to install msgpack on $server"
  fi

done
