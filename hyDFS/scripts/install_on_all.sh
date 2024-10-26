#!/bin/bash

# Loop over host-01 to host-10
for i in $(seq -w 01 10); do
  HOST="fa24-cs425-69$i.cs.illinois.edu"
  echo "Connecting to $HOST and setting up the environment..."

  # SSH into each host and install pip if not already installed
  ssh "$HOST" "sudo yum install -y python3-pip"
  
  if [ $? -eq 0 ]; then
    echo "Successfully installed pip on $HOST"
    
    # Install msgpack using pip
    ssh "$HOST" "pip3 install msgpack"
    
    if [ $? -eq 0 ]; then
      echo "Successfully installed msgpack on $HOST"
    else
      echo "Failed to install msgpack on $HOST"
    fi
  else
    echo "Failed to install pip on $HOST"
  fi

done
