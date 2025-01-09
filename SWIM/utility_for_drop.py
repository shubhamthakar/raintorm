import socket
import json
import concurrent.futures
import argparse
import os
import time

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

def request_membership_list_from_all_nodes(server_list):
    """Query the membership list from all nodes one by one and print each node's membership list."""
    max_membership_size = 0
    for server in server_list:
        print(f"Requesting membership list from {server['ip']}:{server['port']}...")
        try:
            membership_size = request_membership_list(server['ip'], server['port'])
            if membership_size > max_membership_size:
                max_membership_size = membership_size
        except Exception as e:
            print(f"Error occurred while requesting from {server['ip']}:{server['port']}: {e}")
    return max_membership_size

def request_membership_list(process_ip, process_port):
    """Connect to the process and request the membership list."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as client_socket:
        # Construct the message
        message = {
            'type': 'list_mem'
        }

        # Send the message to the specified process
        client_socket.sendto(json.dumps(message).encode('utf-8'), (process_ip, process_port))

        # Wait for the response
        try:
            # Set a timeout for the response
            client_socket.settimeout(5)
            data, addr = client_socket.recvfrom(2048)
            response = json.loads(data.decode('utf-8'))

            # Print the membership list in a formatted way as soon as it is received
            print(f"\nMembership list for node {process_ip}:{process_port}:")
            print(f"{'Node ID':<40}{'Status':<10}{'Incarnation Number':<10}")
            print("-" * 60)
            for entry in response['data']:
                print(f"{entry['node_id']:<40}{entry['status']:<10}{entry['inc_num']:<10}")
            
            print(f"\nCurrent mode: {response['mode']}\n")
            return len(response['data'])  # Return the number of nodes in the list
        except socket.timeout:
            print(f"No response received from the node {process_ip}:{process_port}.")
            return 0
        except json.JSONDecodeError:
            print(f"Invalid JSON response from node {process_ip}:{process_port}.")
            return 0

def main():
    # List of 10 servers with their IP and port information
    servers = [{'ip': f'fa24-cs425-69{i:02d}.cs.illinois.edu', 'port': 5000} for i in range(1, 11)]
    
    # Run indefinitely every 20 seconds
    while True:
        max_nodes = request_membership_list_from_all_nodes(servers)
        print(f"\nMaximum number of nodes in any membership list: {max_nodes}\n")
        
        # Wait for 20 seconds before the next run
        time.sleep(20)

if __name__ == "__main__":
    main()

