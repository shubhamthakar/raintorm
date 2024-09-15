import socket
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from hosts import hosts_dict
import time

port = 12345
output_dict = {}
total_lines_matched = 0

def write_output(host_name, lines_matched, grep_output):
    print(f'Hostname: {host_name}')
    print(f'Num of lines matched: {lines_matched}')

    with open('output.log', 'a') as log_file:
        print(f'Hostname: {host_name}', file=log_file)
        print(f'Num of lines matched: {lines_matched}', file=log_file)
        print(f'{grep_output}', file=log_file)
        print("-" * 50, file=log_file)  # Separator for logs

def process_host(host_name, port, grep_command):
    grep_output = ""
    lines_matched = 0
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((host_name, port))
        s.sendall(",".join(grep_command).encode())

        while True:
            data = s.recv(2)
            if not data:
                break
            grep_output += data.decode()

        lines_matched = len(grep_output.split('\n'))-1 if grep_output else 0

    except Exception as e:
        print(f'Error connecting to host {host_name}: {e}')
        grep_output = ""
        lines_matched = 0

    finally:
        s.close()

    write_output(host_name, lines_matched, grep_output)
    return host_name, grep_output, lines_matched

def run_parallel_socket_calls(hosts_dict, port, grep_command):
    global total_lines_matched

    with open('output.log', 'w') as log_file:
        log_file.truncate()

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(process_host, host_name, port, grep_command) for _, host_name in hosts_dict.items()]

        for future in as_completed(futures):
            host_name, grep_output, lines_matched = future.result()
            output_dict[host_name] = grep_output
            total_lines_matched += lines_matched
    return output_dict

def main():
    grep_command = sys.argv[1:]
    
    if not grep_command:
        print("Please provide the grep command as arguments.")
        sys.exit(1)

    start_time = time.time()
    out = run_parallel_socket_calls(hosts_dict, port, grep_command)
    end_time = time.time()

    execution_time = end_time - start_time
    print("Execution time:", execution_time, "seconds")

    print("Total lines matched:", total_lines_matched)

if __name__ == '__main__':
    main()

