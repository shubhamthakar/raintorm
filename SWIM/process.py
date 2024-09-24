import socket
import threading
import time
import json
import logging
import signal
import select
import queue
import random

class Process:
    PROTOCOL_PERIOD = 1  # Time between pings (in seconds)
    PING_TIMEOUT = 1  # Timeout for receiving an ack (in seconds)

    def __init__(self, ip, port, introducer_ip=None, introducer_port=None):
        self.ip = ip
        self.port = port
        self.introducer_ip = introducer_ip
        self.introducer_port = introducer_port
        self.membership_list = []  # List of dicts with 'node_id' and 'status'
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
        """Failure detection by periodically pinging random nodes."""
        while not self.shutdown_flag.is_set():
            if not self.membership_list:
                time.sleep(self.PROTOCOL_PERIOD)
                continue

            start_time = time.time()  # Record the start time of the protocol period

            eligible_nodes = [node for node in self.membership_list if f"{self.ip}_{self.port}_" not in node['node_id']]
            print('eligible_nodes', eligible_nodes)
            if not eligible_nodes:  # If there are no eligible nodes to ping, skip this round
                time.sleep(self.PROTOCOL_PERIOD)
                continue

            target_node = random.choice(eligible_nodes)

            target_ip, target_port, _ = target_node['node_id'].split('_')
            self.log(f"Pinging {target_ip}:{target_port}")

            if self.ping_node(target_ip, int(target_port)):
                self.log(f"Node {target_ip}:{target_port} is alive")
                self.update_node_status(target_node['node_id'], 'LIVE')
            else:
                self.log(f"Node {target_ip}:{target_port} did not respond. Sending indirect pings.")
                indirect_acks = self.send_indirect_pings(target_node)

                if indirect_acks:
                    self.log(f"Node {target_ip}:{target_port} is alive through indirect ping")
                    self.update_node_status(target_node['node_id'], 'LIVE')
                else:
                    self.log(f"Node {target_ip}:{target_port} did not respond to indirect pings. Marking as DEAD")
                    self.update_node_status(target_node['node_id'], 'DEAD')

            elapsed_time = time.time() - start_time  # Calculate the elapsed time during the ping process
            remaining_time = self.PROTOCOL_PERIOD - elapsed_time  # Calculate the remaining time in the protocol period

            if remaining_time > 0:
                time.sleep(remaining_time)  # Sleep only for the remaining time to complete the protocol period


    def send_indirect_pings(self, target_node):
        """Send indirect ping requests to random nodes."""
        eligible_nodes = [node for node in self.membership_list if f"{self.ip}_{self.port}_" not in node['node_id']]
        indirect_nodes = random.sample([node for node in eligible_nodes if node != target_node], min(3, max(0, len(self.membership_list) - 2)))
        target_ip, target_port, _ = target_node['node_id'].split('_')

        # Send indirect pings to all selected nodes
        for node in indirect_nodes:
            node_ip, node_port, _ = node['node_id'].split('_')
            self.log(f"Sending indirect ping to {node_ip}:{node_port} to check {target_ip}:{target_port}")

            indirect_ping_message = {
                'type': 'indirect_ping',
                'target_ip': target_ip,
                'target_port': target_port
            }
            self.server_socket.sendto(json.dumps(indirect_ping_message).encode('utf-8'), (node_ip, int(node_port)))

        # Wait for an indirect_ack from any of the helper nodes
        success = False
        start_time = time.time()
        while time.time() - start_time < self.PING_TIMEOUT:
            try:
                ack = self.ack_queue.get(timeout=self.PING_TIMEOUT - (time.time() - start_time))
                if ack['type'] == 'indirect_ack' and ack['target_ip'] == target_ip and ack['target_port'] == int(target_port):
                    self.log(f"Received indirect ack from {ack['node_ip']}:{ack['node_port']} for {target_ip}:{target_port}")
                    success = True
                    break
            except queue.Empty:
                continue

        return success


    def ping_node(self, node_ip, node_port):
        """Send a ping to the target node and wait for an acknowledgment."""
        ping_message = {'type': 'ping'}
        self.server_socket.sendto(json.dumps(ping_message).encode('utf-8'), (node_ip, node_port))

        # Wait for ack to be put in the ack queue
        try:
            ack = self.ack_queue.get(timeout=self.PING_TIMEOUT)  # Block until an ack is received or timeout
            if ack['node_ip'] == node_ip and ack['node_port'] == node_port:
                return True
        except queue.Empty:
            return False


    # New Methods for Indirect Pings
    def handle_indirect_ping(self, message, addr):
        """
        Handle an indirect ping by pinging the target node on behalf of another node.
        addr: caller address
        """
        target_ip = message['target_ip']
        target_port = int(message['target_port'])

        self.log(f"Received indirect ping request to ping {target_ip}:{target_port} from {addr}")

        if self.ping_node(target_ip, target_port):
            # If the target node responds, send an indirect acknowledgment back to the requester
            indirect_ack_message = {
                'type': 'indirect_ack',
                'target_ip': target_ip,
                'target_port': target_port
            }
            self.server_socket.sendto(json.dumps(indirect_ack_message).encode('utf-8'), addr)
            self.log(f"Sent indirect ack for {target_ip}:{target_port} to {addr}")
        else:
            self.log(f"Indirect ping to {target_ip}:{target_port} failed")


    def handle_indirect_ack(self, message, addr):
        """Handle an indirect acknowledgment, updating the membership list."""
        target_ip = message['target_ip']
        target_port = int(message['target_port'])
        self.log(f"Received indirect ack from {addr} for {target_ip}:{target_port}")

        # Update the membership list to mark the node as live
        node_id = f"{target_ip}_{target_port}_..."
        self.update_node_status(node_id, 'LIVE')



    def init_logging(self):
        logging.basicConfig(filename=self.log_file, level=logging.INFO,
                            format='%(asctime)s - %(message)s')

    def log(self, message):
        logging.info(message)
        print(message)

        
    def send_ack(self, addr):
        """Send an acknowledgment message in response to a ping."""
        ack_message = {'type': 'ack'}
        if addr[0] != 'fa24-cs425-6901.cs.illinois.edu':
            self.server_socket.sendto(json.dumps(ack_message).encode('utf-8'), addr)
            self.log(f"Sent ack to {addr}")

    def update_node_status(self, node_id, status):
        """Update the status of a node in the membership list."""
        for node in self.membership_list:
            if node['node_id'] == node_id:
                node['status'] = status
                self.log(f"Updated status of {node_id} to {status}")
                break

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
            self.send_ack(addr)

        elif message['type'] == 'ack':
            self.log(f"Received ack from {addr}")
            self.ack_queue.put({
                'node_ip': addr[0],
                'node_port': addr[1],
                'type': 'ack'
            })  # Place ack in the ack queue

        elif message['type'] == 'indirect_ping':
            self.log(f"Received indirect ping request from {addr} to ping {message['target_ip']}:{message['target_port']}")
            self.handle_indirect_ping(message, addr)  # Handle the indirect ping request

        elif message['type'] == 'indirect_ack':
            self.log(f"Received indirect ack from {addr} for {message['target_ip']}:{message['target_port']}")
            self.ack_queue.put({
                'node_ip': addr[0],
                'node_port': addr[1],
                'target_ip': message['target_ip'],
                'target_port': message['target_port'],
                'type': 'indirect_ack'
            })  # Place indirect ack in the ack queue

        elif message['type'] == 'join_request':
            self.handle_join_request(addr)

        elif message['type'] == 'membership_list':
            self.handle_membership_list(message['data'])

        elif message['type'] == 'new_node':
            self.handle_new_node(message['data'])

        else:
            self.log(f"Unknown message type received from {addr}")

    def handle_join_request(self, addr):
        new_node_ip, new_node_port = addr
        new_node_id = f"{new_node_ip}_{new_node_port}_{int(time.time())}"
        new_node_info = {'node_id': new_node_id, 'status': 'LIVE'}
        self.log(f"New join request received from {new_node_ip}:{new_node_port}")
        self.notify_all_nodes(new_node_info)
        self.membership_list.append(new_node_info)
        self.send_membership_list(new_node_ip, new_node_port)

    def handle_membership_list(self, membership_list):
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

    def send_membership_list(self, node_ip, node_port):
        membership_message = {
            'type': 'membership_list',
            'data': self.membership_list
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
    node = Process(socket.gethostname(), 5000, 'fa24-cs425-6901.cs.illinois.edu', 5000)
    if node.introducer_ip:
        node.send_join_request()

