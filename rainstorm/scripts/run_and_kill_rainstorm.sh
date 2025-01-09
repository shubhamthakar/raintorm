#!/bin/bash

# Define the first Python script and its arguments
FIRST_SCRIPT="python /home/chaskar2/distributed-logger/rainstorm/rainstorm.py ./temp_files_py/app2_op1 ./temp_files_py/app2_op2 'Punched Telespar' '' TrafficSigns_1000.csv output_TrafficSigns_1000_app2_failure_1 3"

# Define the second Python script and its arguments
SECOND_SCRIPT="python home/chaskar2/distributed-logger/rainstorm/scripts/stop_process.py 07 09"

# Execute the first Python script
echo "Executing the first Python script..."
$FIRST_SCRIPT

# Check if the first script execution was successful
if [ $? -ne 0 ]; then
  echo "Error: First script execution failed." >&2
  exit 1
fi

# Wait for 1.5 seconds before executing the next script
sleep 1.5

# Execute the second Python script
echo "Executing the second Python script..."
$SECOND_SCRIPT

# Check if the second script execution was successful
if [ $? -ne 0 ]; then
  echo "Error: Second script execution failed." >&2
  exit 1
fi

echo "Both scripts executed successfully."

