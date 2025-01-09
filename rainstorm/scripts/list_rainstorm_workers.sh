#!/bin/bash

# Define the name or partial name of the Python script to check
PYTHON_SCRIPT_PARTIAL_NAME="worker.py"

# Initialize an empty list to store hosts running the script
RUNNING_HOSTS=()

# Loop over host-01 to host-10
for i in $(seq -w 02 10); do
  HOST="fa24-cs425-69$i.cs.illinois.edu"
  echo "Checking $HOST for processes running $PYTHON_SCRIPT_PARTIAL_NAME..."

  # SSH into each host and check for processes running the script
  ssh "$HOST" "pgrep -f 'worker.py'" && \
  RUNNING_HOSTS+=("$HOST") && \
  echo "Process found on $HOST" || \
  echo "No matching process found on $HOST"
done

# Display the list of hosts running the script
if [ ${#RUNNING_HOSTS[@]} -gt 0 ]; then
  echo "Hosts running $PYTHON_SCRIPT_PARTIAL_NAME:"
  printf '%s\n' "${RUNNING_HOSTS[@]}"
else
  echo "No hosts are running $PYTHON_SCRIPT_PARTIAL_NAME."
fi

