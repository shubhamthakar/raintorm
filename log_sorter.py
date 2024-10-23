import re
from datetime import datetime

# Function to parse the log file and extract log entries with hostnames
def parse_logs(log_file):
    logs = []
    current_hostname = None

    # Regular expressions to match hostname, number of lines, and log entries
    hostname_re = re.compile(r"^Hostname:\s*(.*)$")
    num_lines_re = re.compile(r"^Num of lines matched:\s*(\d+)$")
    log_entry_re = re.compile(r"^(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})\s*-\s*(?P<message>.*)$")

    with open(log_file, 'r') as f:
        for line in f:
            # Check for hostname
            hostname_match = hostname_re.match(line)
            if hostname_match:
                current_hostname = hostname_match.group(1)
                continue
            
            # Check for number of matched lines
            num_lines_match = num_lines_re.match(line)
            if num_lines_match:
                num_lines = int(num_lines_match.group(1))
                # If 0 lines matched, skip further processing for this hostname
                if num_lines == 0:
                    current_hostname = None
                continue

            # If there are logs, parse them
            if current_hostname:
                log_entry_match = log_entry_re.match(line)
                if log_entry_match:
                    timestamp = log_entry_match.group('timestamp')
                    message = log_entry_match.group('message')
                    # Append a tuple (timestamp, hostname, message) to the logs list
                    logs.append((timestamp, current_hostname, message))

    return logs

# Function to sort logs by timestamp
def sort_logs(logs):
    # Sort logs by timestamp (first element of each tuple)
    return sorted(logs, key=lambda x: datetime.strptime(x[0], '%Y-%m-%d %H:%M:%S,%f'))

# Function to output the combined logs
def write_combined_logs(sorted_logs, output_file):
    with open(output_file, 'w') as f:
        for log in sorted_logs:
            timestamp, hostname, message = log
            f.write(f"[{hostname}] {timestamp} - {message}\n")

if __name__ == "__main__":
    input_log_file = 'output.log'  # Path to your log file
    output_log_file = 'combined_sorted_output.log'  # Path for output

    # Parse the logs from the file
    logs = parse_logs(input_log_file)

    # Sort the logs by timestamp
    sorted_logs = sort_logs(logs)

    # Write the combined and sorted logs to the output file
    write_combined_logs(sorted_logs, output_log_file)

    print(f"Combined and sorted logs written to {output_log_file}")

