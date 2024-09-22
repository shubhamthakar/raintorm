import socket
import threading
import time
import json
import logging
import signal

class Process:
    def __init__(self, ip, port, introducer_ip=None, introducer_port=None):
        self.ip = ip
        self.port = port
        self.introducer_ip = introducer_ip
        self.introducer_port = introducer_port
        self.membership_list = []  # List of dicts with 'node_id' and 'status'
        self.log_file = 'swim_protocol.log'
        self.shutdown_flag = threading.Event()  # Used to signal when to stop the listener thread
        self.init_logging()
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.server_socket.bind((self.ip, self.port))
        self.listen_thread = threading.Thread(target=self.listen_for_messages)
        self.listen_thread.start()

        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

    def init_logging(self):
        logging.basicConfig(filename=self.log_file, level=logging.INFO,
                            format='%(asctime)s - %(message)s')

    def log(self, message):
        logging.info(message)
        print(message)

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

    def handle_message(self, message, addr):
        if message['type'] == 'join_request':
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
