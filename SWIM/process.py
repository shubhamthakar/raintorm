import socket
import threading
import time
import json
import logging
import signal
import select
import queue
import random
import argparse
import random

class Process:

    def __init__(self, ip, port, introducer_ip, introducer_port, protocol_period=5, ping_timeout=1, drop_percent=0):
        self.protocol_period = protocol_period
        self.drop_percent = drop_percent
        self.ping_timeout = ping_timeout
        self.ip = ip
        self.port = port
        self.node_id = ''
        self.suspicion_flag = True
        self.introducer_ip = introducer_ip
        self.introducer_port = introducer_port
        self.membership_list = []  # List of dicts with 'node_id' and 'status'
        self.timer_dict = {}  # Dictionary to track suspicion start times
        self.log_file = 'swim_protocol.log'
        self.shutdown_flag = threading.Event()  # Used to signal when to stop the listener thread
        self.ack_queue = queue.Queue()  # Queue for ack messages intended for ping_node
        self.init_logging()
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.server_socket.bind((self.ip, self.port))
        self.server_socket.setblocking(False)  # Make socket non-blocking
        self.listen_thread = threading.Thread(target=self.listen_for_messages)
        self.listen_thread.start()

        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

        # Start failure detection thread
        self.failure_detection_thread = threading.Thread(target=self.failure_detection)
        self.failure_detection_thread.start()

    def failure_detection(self):
        """Failure detection by pinging nodes in a round-robin fashion, shuffling after each full iteration, skipping self and dead nodes."""
        current_index = 0  # Keep track of the index for round-robin iteration
        suspicion_timeout = 2*self.protocol_period  # Example suspicion timeout, adjust as needed

        while not self.shutdown_flag.is_set():
            # If the membership list is empty, sleep for the protocol period and continue
            if not self.membership_list:
                time.sleep(self.protocol_period)
                continue

            # If we've reached the end of the list, shuffle it and reset the index
            if current_index >= len(self.membership_list):
                random.shuffle(self.membership_list)  # Shuffle the list after each full round
                current_index = 0

            # Select the next node in round-robin fashion
            target_node = self.membership_list[current_index]
            current_index += 1

            # Extract IP and port from the node_id
            target_ip, target_port, _ = target_node['node_id'].split('_')

            # Skip if this node is the current node (self) or if the node is marked as DEAD
            if (target_ip == self.ip and int(target_port) == self.port) or target_node['status'] == 'DEAD':
                continue

            start_time = time.time()  # Record the start time of the protocol period
            self.log(f"Pinging {target_ip}:{target_port}")

            # Ping the selected node and handle response based on suspicion flag
            if self.ping_node(target_ip, int(target_port)):
                if target_node['status'] == 'SUSPECT':
                    self.log(f"Node {target_ip}:{target_port} was suspect, marking as LIVE")
                    self.update_node_status(target_node['node_id'], 'LIVE')
                    self.timer_dict.pop(target_node['node_id'], None)  # Remove suspicion timer if node is alive
                else:
                    self.log(f"Node {target_ip}:{target_port} is alive")
            else:
                # No ACK received
                if self.suspicion_flag:
                    # If node is already suspect, check the timer
                    if target_node['status'] == 'SUSPECT':
                        suspicion_start_time = self.timer_dict.get(target_node['node_id'], time.time())
                        # If suspicion timeout is exceeded, mark as DEAD
                        if time.time() - suspicion_start_time > suspicion_timeout:
                            self.log(f"Node {target_ip}:{target_port} exceeded suspicion timeout, marking as DEAD")
                            self.update_node_status(target_node['node_id'], 'DEAD')
                    else:
                        # Mark node as SUSPECT and store suspicion start time
                        self.log(f"Node {target_ip}:{target_port} did not respond, marking as SUSPECT")
                        self.update_node_status(target_node['node_id'], 'SUSPECT')
                        self.timer_dict[target_node['node_id']] = time.time()
                else:
                    # If suspicion flag is off, directly mark as DEAD
                    self.log(f"Node {target_ip}:{target_port} did not respond, marking as DEAD")
                    self.update_node_status(target_node['node_id'], 'DEAD')

            # Handle suspicion timeout checks
            if self.suspicion_flag:
                current_time = time.time()
                for node in self.membership_list:
                    if node['status'] == 'SUSPECT':
                        suspicion_start_time = self.timer_dict.get(node['node_id'])
                        if suspicion_start_time and current_time - suspicion_start_time > suspicion_timeout:
                            self.log(f"Node {node['node_id']} exceeded suspicion timeout, marking as DEAD")
                            self.update_node_status(node['node_id'], 'DEAD')
                            self.timer_dict.pop(node['node_id'], None)  # Remove suspicion timer

            # Calculate the elapsed time and sleep for the remaining time in the protocol period
            elapsed_time = time.time() - start_time
            remaining_time = self.protocol_period - elapsed_time
            if remaining_time > 0:
                time.sleep(remaining_time)  # Sleep for the remaining time to complete the protocol period


    def init_logging(self):
        logging.basicConfig(filename=self.log_file, level=logging.INFO,
                            format='%(asctime)s - %(message)s')

    def log(self, message):
        logging.info(message)
        print(message)

    def ping_node(self, node_ip, node_port):
        """Send a ping to the target node and wait for an acknowledgment."""
        if random.uniform(0, 100) < self.drop_percent:
            self.log(f"Dropped ping to {node_ip}:{node_port} (drop_percent={self.drop_percent})")
            return False
        ping_message = {
            'type': 'ping',
            'membership_list': self.membership_list  # Include the current membership list
        }
        self.server_socket.sendto(json.dumps(ping_message).encode('utf-8'), (node_ip, node_port))

        # Wait for ack to be put in the ack queue
        try:
            ack = self.ack_queue.get(timeout=self.ping_timeout)  # Block until an ack is received or timeout
            if ack['node_ip'] == node_ip and ack['node_port'] == node_port:
                return True
        except queue.Empty:
            return False

    def send_ack(self, addr):
        """Send an acknowledgment message in response to a ping."""
        if random.uniform(0, 100) < self.drop_percent:
            self.log(f"Dropped ack to {addr} (drop_percent={self.drop_percent})")
            return  # Simulate the ack being dropped
        ack_message = {
            'type': 'ack',
            'membership_list': self.membership_list  # Include the current membership list
        }
        self.server_socket.sendto(json.dumps(ack_message).encode('utf-8'), addr)
        self.log(f"Sent ack to {addr}")

    def update_node_status(self, node_id, status):
        """Update the status of a node in the membership list."""

        for node in self.membership_list:
            if node['node_id'] == node_id:
                node['status'] = status
                self.log(f"Updated status of {node_id} to {status}")
                break
        self.log(self.membership_list)

    def listen_for_messages(self):
        self.log(f"Node started, listening on {self.ip}:{self.port}")
        while not self.shutdown_flag.is_set():
            ready = select.select([self.server_socket], [], [], 1)  # Check if socket has any data to read
            if ready[0]:
                try:
                    data, addr = self.server_socket.recvfrom(1024)
                    message = json.loads(data.decode('utf-8'))
                    hostname, _, _ = socket.gethostbyaddr(addr[0])
                    new_add = (hostname, addr[1])
                    self.handle_message(message, new_add)
                except socket.herror:
                    self.handle_message(message, addr)
                except Exception as e:
                    self.log(f"Error receiving message: {e}")
                    break

    def handle_message(self, message, addr):
        if message['type'] == 'ping':
            self.reconcile_membership_list(message['membership_list'])
            self.send_ack(addr)
        elif message['type'] == 'ack':
            self.reconcile_membership_list(message['membership_list'])
            self.log(f"Received ack from {addr}")
            self.ack_queue.put({'node_ip': addr[0], 'node_port': addr[1]})  # Place ack in the ack queue
        elif message['type'] == 'join_request':
            self.handle_join_request(addr)
        elif message['type'] == 'membership_list':
            self.handle_membership_list(message['data'], message['node_id'])
        elif message['type'] == 'new_node':
            self.handle_new_node(message['data'])
        elif message['type'] == 'switch_modes':  # New case for 'switch_modes'
            self.switch_modes()
            self.log(f"Switched suspicion mode, suspicion_flag is now {self.suspicion_flag}")
        elif message['type'] == 'list_mem':
            self.send_membership_list_response(addr)
        else:
            self.log(f"Unknown message type received from {addr}")

    def send_membership_list_response(self, addr):
        """Send the current membership list to the requester, excluding DEAD nodes."""
        # Filter the membership list to exclude nodes with 'DEAD' status
        filtered_membership_list = [
            node for node in self.membership_list if node['status'] != 'DEAD'
        ]

        membership_response = {
            'type': 'membership_list_response',
            'data': filtered_membership_list,  # Send the filtered membership list
            'mode': 'Ping+Ack+S' if self.suspicion_flag else 'Ping+Ack'

        }

        self.server_socket.sendto(json.dumps(membership_response).encode('utf-8'), addr)
        self.log(f"Sent membership list response to {addr} with {len(filtered_membership_list)} active nodes")

    def switch_modes(self):
        """Method to switch the suspicion_flag value."""
        self.suspicion_flag = not self.suspicion_flag

    def reconcile_membership_list(self, received_list):
        """Reconcile the received membership list with the local membership list."""
        for received_node in received_list:
            local_node = next((n for n in self.membership_list if n['node_id'] == received_node['node_id']), None)

            if local_node:
                # If suspicion flag is on
                if self.suspicion_flag:
                    local_inc_num = local_node.get('inc_num', 0)
                    received_inc_num = received_node.get('inc_num', 0)

                    # Special case: if the received list contains a suspect status for the current node, mark itself as LIVE and increment its incarnation number
                    if received_node['node_id'] == f"{self.ip}_{self.port}_{int(time.time())}" and received_node['status'] == 'SUSPECT':
                        self.update_node_status(received_node['node_id'], 'LIVE')
                        local_node['inc_num'] += 1  # Increment incarnation number
                        self.log(f"Self node was marked SUSPECT, updating to LIVE and incrementing incarnation number")

                    # 1) Alive {node_l, inc = i} overrides Suspect {node_l, inc = j} if i > j
                    # If I get alive message with greater incarnation number, update suspect status to alive
                    elif local_node['status'] == 'SUSPECT' and received_node['status'] == 'LIVE' and received_inc_num > local_inc_num:
                        self.update_node_status(local_node['node_id'], 'LIVE')
                        local_node['inc_num'] = received_inc_num  # Update incarnation number
                        self.log(f"Updated node {local_node['node_id']} to LIVE with higher incarnation number {received_inc_num}")

                    # 2) Suspect {node_l, inc = i} overrides Alive {node_l, inc = j} if i >= j
                    # If I get suspect message with greater or equal incarnation number, update alive status to suspect
                    elif local_node['status'] == 'LIVE' and received_node['status'] == 'SUSPECT' and received_inc_num >= local_inc_num:
                        self.update_node_status(local_node['node_id'], 'SUSPECT')
                        local_node['inc_num'] = received_inc_num  # Update incarnation number
                        self.timer_dict[local_node['node_id']] = time.time()  # Start suspicion timer
                        self.log(f"Updated node {local_node['node_id']} to SUSPECT with equal or higher incarnation number {received_inc_num}")

                    # Suspect {node_l, inc = i} overrides Suspect {node_l, inc = j} if i >= j
                    # Update the suspicion timestamp and incarnation number
                    elif local_node['status'] == 'SUSPECT' and received_node['status'] == 'SUSPECT' and received_inc_num > local_inc_num:
                        local_node['inc_num'] = received_inc_num  # Update incarnation number
                        self.timer_dict[local_node['node_id']] = time.time()  # Refresh suspicion timer
                        self.log(f"Updated suspicion timer for node {local_node['node_id']} with equal or higher incarnation number {received_inc_num}")

                    # 3) Dead overrides anything
                    elif received_node['status'] == 'DEAD' and local_node['status'] != 'DEAD':
                        self.update_node_status(local_node['node_id'], 'DEAD')
                        self.log(f"Node {local_node['node_id']} marked as DEAD from received membership list")

                # If suspicion flag is off, just mark DEAD as the priority
                else:
                    if local_node['status'] == 'LIVE' and received_node['status'] == 'DEAD':
                        self.update_node_status(local_node['node_id'], 'DEAD')
                        self.log(f"Node {local_node['node_id']} marked as DEAD with suspicion flag off")

            else:
                # If the node is not in the local list, add it
                self.membership_list.append(received_node)
                self.log(f"Added new node from received membership list: {received_node}")



    def handle_join_request(self, addr):
        new_node_ip, new_node_port = addr
        new_node_id = f"{new_node_ip}_{new_node_port}_{int(time.time())}"
        new_node_info = {'node_id': new_node_id, 'status': 'LIVE', 'inc_num': 0}
        self.log(f"New join request received from {new_node_ip}:{new_node_port}")
        self.notify_all_nodes(new_node_info)
        self.membership_list.append(new_node_info)
        self.send_membership_list(new_node_ip, new_node_port, new_node_id)

    def handle_membership_list(self, membership_list, node_id):
        self.node_id = node_id
        self.membership_list = membership_list
        self.log(f"Received updated membership list: {self.membership_list}")

    def handle_new_node(self, new_node_info):
        self.membership_list.append(new_node_info)
        self.log(f"New node added to membership list: {new_node_info}")

    def send_join_request(self):
        if self.introducer_ip and self.introducer_port:
            join_message = {'type': 'join_request'}
            self.server_socket.sendto(json.dumps(join_message).encode('utf-8'),
                                      (self.introducer_ip, self.introducer_port))
            self.log(f"Sent join request to introducer at {self.introducer_ip}:{self.introducer_port}")
        else:
            self.log("No introducer IP and port provided")

    def send_membership_list(self, node_ip, node_port, node_id):
        membership_message = {
            'type': 'membership_list',
            'data': self.membership_list,
            'node_id': node_id
        }
        self.server_socket.sendto(json.dumps(membership_message).encode('utf-8'), (node_ip, node_port))
        self.log(f"Sent membership list to {node_ip}:{node_port}")

    def notify_all_nodes(self, new_node_info):
        notification_message = {
            'type': 'new_node',
            'data': new_node_info
        }

        for node in self.membership_list:
            node_ip, node_port, _ = node['node_id'].split('_')
            if (node_ip, int(node_port)) != (self.ip, self.port):  # Skip self
                self.server_socket.sendto(json.dumps(notification_message).encode('utf-8'), (node_ip, int(node_port)))
                self.log(f"Notified {node_ip}:{node_port} of new node {new_node_info}")

    def shutdown(self, signum, frame):
        """ Graceful shutdown function """
        self.log("Shutting down...")
        self.shutdown_flag.set()  # Signal the listener thread to stop
        self.listen_thread.join()  # Wait for the listener thread to finish
        self.server_socket.close()  # Close the socket
        self.log("Node shut down gracefully.")

if __name__ == "__main__":
    # Example: Start a node at IP 127.0.0.1, port 5000, with introducer at 127.0.0.1:4000
    parser = argparse.ArgumentParser(description="Start a SWIM protocol node.")
    parser.add_argument('--proto_period', type=int, required=True, help='The protocol period of SWIM')
    parser.add_argument('--ping_timeout', type=int, required=True, help='Ping timeout for SWIM')
    parser.add_argument('--drop_percent', type=int, required=True, help='Packet drop percentage')
    args = parser.parse_args()

    node = Process(socket.gethostname(), 5000, 'fa24-cs425-6901.cs.illinois.edu', 5000, args.proto_period, args.ping_timeout, args.drop_percent)
    if node.introducer_ip:
        node.send_join_request()
