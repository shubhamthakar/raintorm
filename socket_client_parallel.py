import socket
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from hosts import hosts_dict
import time

# Define the port and global output variables
port = 12345
output_dict = {}
total_lines_matched = 0

# Function to write output to the terminal and log file
def write_output(host_name, lines_matched, grep_output):
    print(f'Hostname: {host_name}')
    print(f'Num of lines matched: {lines_matched}')
    # print(grep_output)

    with open('output.log', 'w') as log_file:
        print(f'Hostname: {host_name}', file=log_file)
        print(f'Num of lines matched: {lines_matched}', file=log_file)
        print(f'{grep_output}', file=log_file)
        print("-" * 50, file=log_file)  # Separator for logs

# Function to process socket connection for a single host
def process_host(host_name, port, grep_command):
    grep_output = ""
    lines_matched = 0
    try:
        # Create and connect the socket
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((host_name, port))
        s.sendall(",".join(grep_command).encode())

        # Receive data from the server
        while True:
            data = s.recv(2)
            if not data:
                break
            grep_output += data.decode()

        # Count the number of lines matched
        lines_matched = len(grep_output.split('\n'))-1 if grep_output else 0

    except Exception as e:
        print(f'Error connecting to host {host_name}: {e}')
        grep_output = ""
        lines_matched = 0

    finally:
        s.close()

    # Log output and return result
    write_output(host_name, lines_matched, grep_output)
    return host_name, grep_output, lines_matched

# Function to run socket connections in parallel
def run_parallel_socket_calls(hosts_dict, port, grep_command):
    global total_lines_matched
    with ThreadPoolExecutor(max_workers=10) as executor:
        # Submit tasks to the thread pool
        futures = [executor.submit(process_host, host_name, port, grep_command) for _, host_name in hosts_dict.items()]

        # Process results as they complete
        for future in as_completed(futures):
            host_name, grep_output, lines_matched = future.result()
            output_dict[host_name] = grep_output
            total_lines_matched += lines_matched
    return output_dict

# Main function to initiate the parallel execution
def main():
    grep_command = sys.argv[1:]
    
    if not grep_command:
        print("Please provide the grep command as arguments.")
        sys.exit(1)

    start_time = time.time()
    out = run_parallel_socket_calls(hosts_dict, port, grep_command)
    print(out)
    end_time = time.time()

    execution_time = end_time - start_time
    print("Execution time:", execution_time, "seconds")

    # Print final results
    print("Total lines matched:", total_lines_matched)

if __name__ == '__main__':
    main()

