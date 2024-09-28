import socket
import json
import concurrent.futures
import argparse
import os

def request_membership_list(process_ip):
    """Connect to the process and request the membership list."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as client_socket:
        # Construct the message
        message = {
            'type': 'list_mem'
        }

        # Send the message to the specified process
        client_socket.sendto(json.dumps(message).encode('utf-8'), (process_ip, 5000))

        # Wait for the response
        try:
            # Set a timeout for the response
            client_socket.settimeout(2)
            data, addr = client_socket.recvfrom(1024)
            response = json.loads(data.decode('utf-8'))
            print("Received membership list:", response['data'])
            print("Current mode:", response['mode'])
        except socket.timeout:
            print("No response received from the process.")

def send_switch_modes_message(server_ip, server_port):
    message = {
        'type': 'switch_modes'
    }
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as client_socket:
            client_socket.sendto(json.dumps(message).encode('utf-8'), (server_ip, server_port))
            print(f"Sent 'switch_modes' message to {server_ip}:{server_port}")
    except Exception as e:
        print(f"Failed to send message to {server_ip}:{server_port}. Error: {e}")

def send_to_multiple_servers(server_list):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(send_switch_modes_message, server['ip'], server['port']) for server in server_list]
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Error occurred: {e}")

def leave_process():
    """Terminate the process.py using pkill."""
    os.system("pkill -f 'python .*process.py.*'")
    print("Send termination signal to process")

def main():
    # Set up argument parsing
    parser = argparse.ArgumentParser(description='Utility for communicating with the process.')
    parser.add_argument('--command', type=str, help='The command to execute (e.g., list_mem)')

    # Parse the arguments
    args = parser.parse_args()

    # Get the host IP from the socket
    process_ip = socket.gethostname()

    # Execute the appropriate function based on the command
    if args.command == 'list_mem':
        request_membership_list(process_ip)
    elif args.command == 'switch_modes':
        # List of 10 servers with their IP and port information
        servers = [{'ip': f'fa24-cs425-69{i:02d}.cs.illinois.edu', 'port': 5000} for i in range(1, 11)]
        send_to_multiple_servers(servers)
    elif args.command == 'leave':
        leave_process()
    else:
        print(f"Unknown command: {args.command}")



if __name__ == "__main__":
    main()
    
    

    

