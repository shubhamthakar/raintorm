#!/bin/bash

# Check if the correct number of arguments is provided
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <source_folder> <target_folder>"
    exit 1
fi

SOURCE_FOLDER=$1        # /home/chaskar2/distributed-logger/rainstorm/temp_files
TARGET_FOLDER=$2        # /home/chaskar2/distributed-logger/rainstorm/exe_files

# Create the target folder if it does not exist
if [ ! -d "$TARGET_FOLDER" ]; then
    mkdir -p "$TARGET_FOLDER"
    if [ $? -ne 0 ]; then
        echo "Failed to create target folder $TARGET_FOLDER"
        exit 1
    fi
fi

for i in $(seq -w 1 10); do
    HOSTNAME="fa24-cs425-69${i}.cs.illinois.edu"
    echo "Copying files to $HOSTNAME..."
    
    # Create the target folder on the target hostname if it does not exist
    ssh "$HOSTNAME" "mkdir -p $TARGET_FOLDER"
    
    # Check if the mkdir command was successful
    if [ $? -ne 0 ]; then
        echo "Failed to create target folder on $HOSTNAME"
        continue
    fi
    
    # Copy all files from the source folder to the target folder on the target hostname
    scp -r "$SOURCE_FOLDER"/* "$HOSTNAME":"$TARGET_FOLDER"
    
    # Check if the scp command was successful
    if [ $? -eq 0 ]; then
        echo "Files copied successfully to $HOSTNAME:$TARGET_FOLDER"
    else
        echo "Failed to copy files to $HOSTNAME:$TARGET_FOLDER"
    fi
done
