import socket
import threading
import time
import json
import logging
import signal
import random
import traceback
import queue
import select

class Process:
    PROTOCOL_PERIOD = 3  # Time between pings (in seconds)
    PING_TIMEOUT = 1  # Timeout for receiving an ack (in seconds)

    def _init_(self, ip, port, introducer_ip=None, introducer_port=None):
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

            target_node = random.choice(self.membership_list)
            target_ip, target_port, _ = target_node['node_id'].split('_')
            self.log(f"Pinging {target_ip}:{target_port}")

            if self.ping_node(target_ip, int(target_port)):
                self.log(f"Node {target_ip}:{target_port} is alive")
                self.update_node_status(target_node['node_id'], 'LIVE')
            else:
                self.log(f"Node {target_ip}:{target_port} did not respond, marking as DEAD")
                self.update_node_status(target_node['node_id'], 'DEAD')

            elapsed_time = time.time() - start_time  # Calculate the elapsed time during the ping process
            remaining_time = self.PROTOCOL_PERIOD - elapsed_time  # Calculate the remaining time in the protocol period

            if remaining_time > 0:
                time.sleep(remaining_time)  # Sleep only for the remaining time to complete the protocol period


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

    def listen_for_messages(self):
        self.log(f"Node started, listening on {self.ip}:{self.port}")
        while not self.shutdown_flag.is_set():
            try:
                self.server_socket.settimeout(1)  # Set a timeout so the thread can check the flag periodically
                data, addr = self.server_socket.recvfrom(1024)
                hostname, _, _ = socket.gethostbyaddr(addr[0])
                new_add = (hostname, addr[1])
                message = json.loads(data.decode('utf-8'))
                self.handle_message(message, new_add)
            except socket.timeout:
                continue
            except socket.herror:
                self.handle_message(message, addr)
            except Exception as e:
                self.log(f"Error receiving message: {e}")
                break
            ready = select.select([self.server_socket], [], [], 1)  # Check if socket has any data to read
            if ready[0]:
                try:
                    data, addr = self.server_socket.recvfrom(1024)
                    message = json.loads(data.decode('utf-8'))
                    self.handle_message(message, addr)
                except Exception as e:
                    self.log(f"Error receiving message: {e}")
                    break

    def handle_message(self, message, addr):
        if message['type'] == 'ping':
            self.send_ack(addr)
        elif message['type'] == 'ack':
            self.log(f"Received ack from {addr}")
            self.ack_queue.put({'node_ip': addr[0], 'node_port': addr[1]})  # Place ack in the ack queue
        elif message['type'] == 'join_request':
            self.handle_join_request(addr)
        elif message['type'] == 'membership_list':
            self.handle_membership_list(message['data'])
        elif message['type'] == 'new_node':
            self.handle_new_node(message['data'])
        else:
            self.log(f"Unknown message type received from {addr}")

    def send_ack(self, addr):
        """Send an acknowledgment message in response to a ping."""
        ack_message = {'type': 'ack'}
        self.server_socket.sendto(json.dumps(ack_message).encode('utf-8'), addr)
        self.log(f"Sent ack to {addr}")

    def update_node_status(self, node_id, status):
        """Update the status of a node in the membership list."""
        for node in self.membership_list:
            if node['node_id'] == node_id:
                node['status'] = status
                self.log(f"Updated status of {node_id} to {status}")
                break

    def init_logging(self):
        logging.basicConfig(filename=self.log_file, level=logging.INFO,
                            format='%(asctime)s - %(message)s')

    def log(self, message):
        logging.info(message)
        print(message)

    def handle_join_request(self, addr):
        new_node_ip, new_node_port = addr
        new_node_id = f"{new_node_ip}{new_node_port}{int(time.time())}"
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
        self.failure_detection_thread.join()  # Wait for the failure detection thread to finish
        self.server_socket.close()  # Close the socket
        self.log("Node shut down gracefully.")

if __name__ == "__main__":
    node = Process(socket.gethostname(), 5000, 'fa24-cs425-6901.cs.illinois.edu', 5000)
    if node.introducer_ip:
        node.send_join_request()