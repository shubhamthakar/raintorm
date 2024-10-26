import paramiko
import pandas as pd
import os
import getpass

# Replace with the actual log file path on the remote machines
LOG_FILE_PATH = "/home/sthakar3/swim_protocol.log"

def fetch_logs(hostname, username, password):
    """Fetch logs from a remote machine using SSH."""
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname, username=username, password=password)

        # Run the command to read the log file
        stdin, stdout, stderr = client.exec_command(f"cat {LOG_FILE_PATH}")
        logs = stdout.read().decode('utf-8').strip().splitlines()

        # Prefix each log line with the hostname
        logs_with_hostname = [f"{hostname}: {log}" for log in logs]

        client.close()
        return logs_with_hostname
    except Exception as e:
        print(f"Error fetching logs from {hostname}: {e}")
        return []

def process_and_sort_logs(logs):
    """Sort logs by timestamp and display them."""
    # Assume log format: <timestamp> <log_message>
    # Example: 2024-09-28 14:35:00 Some log message

    # Create a list to store tuples of (timestamp, log_message)
    log_entries = []

    for log in logs:
        # Split the log into timestamp and message (assuming the first two parts are the timestamp)
        try:
            hostname_log = log.split(':', 1)
            if len(hostname_log) == 2:
                hostname, log_message = hostname_log
                parts = log_message.split(maxsplit=2)
                timestamp = " ".join(parts[:2])
                message = parts[2] if len(parts) > 2 else ""
                log_entries.append((timestamp, hostname, message))
        except Exception as e:
            print(f"Error processing log: {log}, Error: {e}")

    # Convert log_entries to a pandas DataFrame for sorting
    df = pd.DataFrame(log_entries, columns=["timestamp", "hostname", "log"])
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')

    # Drop rows where timestamp could not be parsed
    df.dropna(subset=["timestamp"], inplace=True)

    # Sort logs by timestamp
    df.sort_values(by="timestamp", inplace=True)

    # Display sorted logs with timestamp
    for _, row in df.iterrows():
        print(f"[{row['timestamp']}] {row['hostname']}: {row['log']}")

if __name__ == "__main__":
    # Get password securely from user
    password = getpass.getpass(prompt='Enter your password: ')

    # Define the list of machines (hostnames/IPs) to fetch logs from
    machines = [
        {"hostname": f"fa24-cs425-69{i:02d}.cs.illinois.edu", "username": "sthakar3", "password": password}
        for i in range(1, 11)
    ]

    all_logs = []

    # Fetch logs from each machine
    for machine in machines:
        logs = fetch_logs(machine['hostname'], machine['username'], machine['password'])
        all_logs.extend(logs)

    # Process and sort the logs by timestamp before displaying
    process_and_sort_logs(all_logs)