# How to assign nodes 
import socket
import hashlib
from process import Process
import socket
import msgpack
import threading
import select
import signal
import os
import re
from collections import defaultdict
import logging


class RingNode:
    def __init__(self):
        """
        Initialize the RingNode with a ring_id based on the host_name and pass it along with other params to Process.
        """
        self.host_name = socket.gethostname()
        self.hydfs_host_port = 5001

        # Logging
        self.log_file = '/home/chaskar2/distributed-logger/hyDFS/logs/hydfs.logs'
        self.init_logging()

        self.ring_id = self.hash_string(self.host_name)
        self.process = Process(socket.gethostname(), 5000, 'fa24-cs425-6901.cs.illinois.edu', 5000, False, 20, 10, 0, self.ring_id)
        
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host_name, self.hydfs_host_port))
        self.server_socket.listen()
        self.server_socket.setblocking(False)

        self.listen_thread = threading.Thread(target=self.listen_for_messages)
        self.listen_thread.start()
        
        self.fs_directory = "/home/chaskar2/distributed-logger/hyDFS/filesystem"
        self.local_directory = ""


        # Set up signal handlers for graceful shutdown
        self.shutdown_flag = threading.Event() 
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

        # Socket message handling variables
        self.inputs = [self.server_socket]
        self.outputs = []
        self.data_buffer = defaultdict(lambda: b"")
        self.client_socket_map = {}

        # Ack Tracking
        # key: client_name, file_name, action
        self.acktracker = defaultdict(lambda: 0)

        # Quorum
        self.quorum_size = 3

        self.send_join_request()


    def init_logging(self):
        logging.basicConfig(filename=self.log_file, level=logging.INFO,
                            format='%(asctime)s - %(message)s')
        self.log("Logging initialized") 

    def log(self, message):
        logging.info(message)
        print(message)
    
    def listen_for_messages(self):
        self.log(f"Server listening on {self.host_name}:{self.hydfs_host_port}")

        while not self.shutdown_flag.is_set():
            readable, writable, exceptional = select.select(self.inputs, self.outputs, self.inputs, 5)

            # Handle readable sockets
            for s in readable:
                if s is self.server_socket:
                    # Accept new client connection
                    client_socket, client_address = self.server_socket.accept()
                    client_socket.setblocking(False)
                    self.inputs.append(client_socket)
                    self.data_buffer[client_socket] = b""
                    self.log(f"Connection from {client_address}")
                else:
                    # Read data from an existing client
                    data = s.recv(4096)
                    if data:
                        self.data_buffer[s] += data

                        # Check if we have received the complete dictionary with "<EOF>"
                        if b"<EOF>" in self.data_buffer[s]:
                            # Extract the complete data before <EOF>
                            complete_data, _, _ = self.data_buffer[s].partition(b"<EOF>")
                            file_info = msgpack.unpackb(complete_data)
                            action = file_info.get("action")

                            # Only create socket mapping when message is received from client
                            if action in ["create", "get", "append", "merge"]:
                                client_name = file_info["client_name"]
                                self.client_socket_map[client_name] = s

                            self.handle_message(file_info, s)

                            # Add to outputs list if there's a response to be sent
                            # if s not in self.outputs:
                            #     self.outputs.append(s)
                    else:
                        # Client disconnected unexpectedly
                        self.log("Client disconnected")
                        self.inputs.remove(s)
                        if s in self.client_socket_map:
                            del self.client_socket_map[s]
                        s.close()
                        if s in self.data_buffer:
                            del self.data_buffer[s]

            # Handle writable sockets
            for s in writable:
                if s in self.data_buffer and self.data_buffer[s]:
                    # Send all data from data_buffer and clear it
                    try:
                        s.sendall(self.data_buffer[s])
                        self.log(f"Sent data_buffer message {self.data_buffer[s]}")
                        self.data_buffer[s] = b""
                    except Exception as e:
                        self.log(f"Error sending data: {e}")
                        self.outputs.remove(s)
                        s.close()
                else:
                    # No more data to send, remove from outputs
                    self.outputs.remove(s)

            # Handle exceptional sockets
            for s in exceptional:
                self.log(f"Handling exceptional condition for {s.getpeername()}")
                self.inputs.remove(s)
                if s in self.outputs:
                    self.outputs.remove(s)
                s.close()
                if s in self.data_buffer:
                    del self.data_buffer[s]


                        


    def handle_message(self, file_info, client_socket):

        action = file_info.get("action")

        if action == "create":
            self.create_file(file_info, client_socket)
        
        elif action == "write":
            self.write_file(file_info, client_socket)

        elif action == "get":
            self.get_file(file_info, client_socket)

        elif action == "read":
            self.read_file(file_info, client_socket)
        
        elif action == "ack":
            self.acknowledge(file_info)
        
        else:
            self.log(f"Unknown action: {action}")


    # Sends write request to all the replicas
    def create_file(self, file_info, client_socket):
        # Step 1: Get all replicas
        all_replicas = self.get_all_replicas(file_info["filename"])

        # Step 2: Parse each node_id to get hostname and port
        for replica in all_replicas:
            node_id = replica["node_id"]
            
            # Extract hostname and port using regex
            match = re.match(r"^(.*?)_(\d+)_.*$", node_id)
            if not match:
                self.log(f"Error parsing node_id {node_id}")
                continue
            
            host = match.group(1)
            port = self.hydfs_host_port

            # Format write request
            file_info_create = {
            "client_name": file_info["client_name"],
            "action": "write",
            "filename": file_info["filename"],
            "content": file_info["content"]
            }

            # Step 3: Send a create request to each replica
            self.send_request(host, port, file_info_create)


    # Sends read request to all the replicas
    def get_file(self, file_info, client_socket):
        # Step 1: Get all replicas
        all_replicas = self.get_all_replicas(file_info["filename"])

        # Step 2: Parse each node_id to get hostname and port
        for replica in all_replicas:
            node_id = replica["node_id"]

            # Extract hostname and port using regex
            match = re.match(r"^(.*?)_(\d+)_.*$", node_id)
            if not match:
                self.log(f"Error parsing node_id {node_id}")
                continue

            host = match.group(1)
            port = self.hydfs_host_port

            # Format get request
            file_info_get = {
                "client_name": file_info["client_name"],
                "action": "read",
                "filename": file_info["filename"]
            }

            self.send_request(host, port, file_info_get)



    # Writes the files after receiving a  "write" request (handle_message)
    def write_file(self, file_info, client_socket):
        filename = file_info["filename"]
        file_content = file_info.get("content", b"")  # Default to empty content if not provided

        # Construct the full file path
        file_path = os.path.join(self.fs_directory, filename)

        # Prepare acknowledgment message indicating the file already exists
        ack_message = {
            "client_name": file_info["client_name"],
            "action": "ack",
            "filename": filename,
        }

        # Check if the file already exists
        if os.path.exists(file_path):
            self.log(f"File '{filename}' already exists. Not writing to it.")
            ack_message["status"] = "file_exists"

        else:
            with open(file_path, "wb") as file:
                file.write(file_content)

            self.log(f"File '{filename}' written successfully.")
            ack_message["status"] = "write_complete"
            
            
        try:
            # Send ack message using msgpack for serialization
            client_socket.sendall(msgpack.packb(ack_message) + b"<EOF>")
            self.log(f"Acknowledgment sent to client for existing file '{filename}'.")
        except (BlockingIOError, socket.error) as e:
            self.log(f"Failed to send acknowledgment for existing file '{filename}' - {e}")




    def read_file(self, file_info, client_socket):
        filename = file_info["filename"]

        # Construct the full file path
        file_path = os.path.join(self.fs_directory, filename)

        # Prepare acknowledgment message for the response
        ack_message = {
            "client_name": file_info["client_name"],
            "action": "ack",
            "filename": filename,
        }

        # Check if the file exists
        if os.path.exists(file_path):
            # Read the file content
            with open(file_path, "rb") as file:
                file_content = file.read()

            # Add the content and status to the ack message
            ack_message["status"] = "read_complete"
            ack_message["content"] = file_content
            self.log(f"File '{filename}' read successfully and content prepared for sending.")
        
        else:
            # If file does not exist, update the ack message with an error
            ack_message["status"] = "file_not_found"
            self.log(f"File '{filename}' not found. Cannot read.")

        try:
            # Send the ack message using msgpack for serialization
            client_socket.sendall(msgpack.packb(ack_message) + b"<EOF>")
            self.log(f"Acknowledgment sent to client with status '{ack_message['status']}' for file '{filename}'.")
        except (BlockingIOError, socket.error) as e:
            self.log(f"Failed to send acknowledgment for file '{filename}' - {e}")



    def acknowledge(self, file_info):
        client_name = file_info["client_name"]
        filename = file_info['filename']
        action = file_info['action']

        self.log(f"Acknowledgment received for file info: {file_info}")
        self.acktracker[(client_name, filename, action)] += 1

        ack_count = self.acktracker[(client_name, filename, action)]

        if ack_count == self.quorum_size:

            # Retrieve the client socket from the client_socket_map using client_name
            client_socket_to_use = self.client_socket_map.get(client_name)

            self.log(f"client_socket_to_use {client_socket_to_use}")

            self.log(f"file_info received from replicas: {file_info}")

            if client_socket_to_use:
                try:
                    # Send the acknowledgment message using msgpack for serialization
                    client_socket_to_use.sendall(msgpack.packb(file_info) + b"<EOF>")
                    self.log(f"Acknowledgment sent to {client_name} for file '{file_info['filename']}'.")

                    # Cleanup ack count
                    del self.acktracker[(client_name, filename, action)]

                except (BlockingIOError, socket.error) as e:
                    self.log(f"Failed to send acknowledgment to {client_name} - {e}")
            else:
                self.log(f"No client socket found for {client_name}.")
        else:
            self.log(f"Quorum not yet met, received ack from {ack_count} replicas")

    

    # Creates a socket to the replica and sends the "write" file_info message over the socket
    def send_request(self, host, port, file_info):
        # Create a non-blocking socket connection to the target replica
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setblocking(False)

        self.log(f"Sending {file_info['action']} to {host}:{port} for file {file_info['filename']}")

        try:
            s.connect((host, port))
        except BlockingIOError:
            # Non-blocking connect may raise this; it’s expected behavior
            pass

        # Add the socket to self.inputs for select monitoring
        self.inputs.append(s)

        # Prepare the file info message and store it to send after connection
        message = msgpack.packb(file_info) + b"<EOF>"

        # Adding message to data_buffer and and outputs list, select.select will check when sock is avail and send data
        self.data_buffer[s] = message
        self.outputs.append(s)

        
        
        # # Keep track of data to be sent in an output buffer
        # try:
        #     # Send the message immediately
        #     s.sendall(message)
        #     self.log(f"Write request sent to {host}:{port} for file {file_info['filename']}")

        # except (BlockingIOError, socket.error) as e:
        #     self.log(f"Failed to send write request to {host}:{port} - {e}")
        
        # self.log(f"Write request initialized for {host}:{port} for file {file_info['filename']}")



    
    def shutdown(self, signum, frame):
        """ Graceful shutdown function """
        self.log("Shutting down HyDFS...")
        self.shutdown_flag.set()  # Signal the listener thread to stop
        if self.listen_thread.is_alive():
            self.log("Listener thread is still running; proceeding to join...")
            self.listen_thread.join()  # Wait for the listener thread to finish
            self.log("Listener thread joined successfully.")
        else:
            self.log("Listener thread is not running, skipping join.")
        self.server_socket.close()  # Close the socket

        self.process.shutdown(signum, frame) # Shutdown process
        
        self.log("HyDFS Node shut down gracefully.")
    
    
    def send_join_request(self):
        self.process.send_join_request()
    

    def hash_string(self, content):
        md5_hash = hashlib.md5(content.encode('utf-8')).hexdigest()    
        hash_int = int(md5_hash, 16)
        
        return hash_int % (2**10)


    def get_next_n_nodes(self, ring_id, n):
        # Filter for live nodes only
        live_nodes = [node for node in self.process.membership_list if node['status'] == 'LIVE']
        
        # Sort nodes by ring_id in ascending order
        sorted_nodes = sorted(live_nodes, key=lambda x: x['ring_id'])
        
        # Find the index of the node with the specified ring_id
        current_index = next((i for i, node in enumerate(sorted_nodes) if node['ring_id'] == ring_id), None)
        
        # If current_index is None, determine the node after where the ring_id would be
        if current_index is None:
            # Find the first node with a larger ring_id
            current_index = next((i for i, node in enumerate(sorted_nodes) if node['ring_id'] > ring_id), 0)
        
        # Retrieve the next n nodes starting after the found or calculated index
        next_nodes = []
        for i in range(n):
            # Use modulo to wrap around to the beginning of the list if necessary
            next_index = (current_index + i) % len(sorted_nodes)
            next_nodes.append({
                'node_id': sorted_nodes[next_index]['node_id'],
                'ring_id': sorted_nodes[next_index]['ring_id']
            })

        # Log the next nodes for debugging
        self.log(f"Next {n} nodes after {ring_id}: {next_nodes}")
        
        return next_nodes


    

    def get_all_replicas(self, filename):

        file_primary_ring_id = self.hash_string(filename)
        next_3_nodes_info = self.get_next_n_nodes(file_primary_ring_id, 3)

        return next_3_nodes_info



if __name__ == '__main__':
    ring_node = RingNode()











