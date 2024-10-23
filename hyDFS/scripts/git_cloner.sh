#!/bin/bash

# Define the repo URL and target directory where you want to clone or update the repo
PAT_FILE="/home/chaskar2/git-pat-token.txt"

# Read username and token from the text file
USERNAME=$(cut -d':' -f1 "$PAT_FILE")
TOKEN=$(cut -d':' -f2 "$PAT_FILE")

REPO_URL="https://${USERNAME}:${TOKEN}@gitlab.engr.illinois.edu/sthakar3/distributed-logger.git"
TARGET_DIR="/home/chaskar2/distributed-logger"

# Loop over host-01 to host-10
for i in $(seq -w 01 10); do
  HOST="fa24-cs425-69$i.cs.illinois.edu"
  echo "Connecting to $HOST..."

  # Check if the target directory exists
  ssh "$HOST" "if [ -d '$TARGET_DIR' ]; then
      echo 'Directory exists, pulling latest changes...';
      cd '$TARGET_DIR' && sudo git config --global --add safe.directory /home/chaskar2/distributed-logger && sudo git remote set-url origin $REPO_URL && sudo git fetch origin && sudo git reset --hard origin/HyDFS;
    else
      echo 'Directory does not exist, cloning repo...';
      sudo git clone -b HyDFS '$REPO_URL' '$TARGET_DIR';
    fi"

  echo "Done with $HOST"
done
