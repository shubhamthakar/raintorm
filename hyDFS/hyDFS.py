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
import time
import copy
import json
import base64

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
        
        self.shutdown_flag = threading.Event() 

        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host_name, self.hydfs_host_port))
        self.server_socket.listen()
        self.server_socket.setblocking(False)
        
        self.fs_directory = "/home/chaskar2/distributed-logger/hyDFS/filesystem"
        self.local_directory = ""
         # Clear the filesystem directory on startup
        self.clear_fs_directory()

        # Socket message handling variables
        self.inputs = [self.server_socket]
        self.outputs = []
        self.data_buffer = defaultdict(lambda: b"")
        self.client_socket_map = {}

        #input tracking for closing sockets after final write
        self.inputtracker = defaultdict(lambda: False)

        # Ack Tracking
        # key: client_name, file_name, action
        self.acktracker = defaultdict(lambda: 0)

        # Initialize the sequence number tracker
        self.sequence_numbers = defaultdict(int)

        # Quorum
        self.quorum_size = 3

        self.listen_thread = threading.Thread(target=self.listen_for_messages)
        self.listen_thread.start()

        self.membership_monitor_thread = threading.Thread(target=self.monitor_membership_list)
        self.membership_monitor_thread.start()

        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

        self.send_join_request()


    def clear_fs_directory(self):
        """Remove all files in the filesystem directory at startup."""
        if os.path.exists(self.fs_directory):
            for file_name in os.listdir(self.fs_directory):
                file_path = os.path.join(self.fs_directory, file_name)
                try:
                    if os.path.isfile(file_path):
                        os.remove(file_path)
                        self.log(f"Deleted file: {file_path}")
                    elif os.path.isdir(file_path):
                        os.rmdir(file_path)  # Remove directories if present
                        self.log(f"Deleted directory: {file_path}")
                except Exception as e:
                    self.log(f"Failed to delete {file_path}. Reason: {e}")
        else:
            os.makedirs(self.fs_directory)  # Create the directory if it does not exist
            self.log(f"Filesystem directory created at {self.fs_directory}")


    def init_logging(self):
        # Create a specific logger for RingNode
        self.logger = logging.getLogger('RingNodeLogger')
        self.logger.setLevel(logging.INFO)
        
        # Ensure only one handler is added to prevent duplicate logs
        if not self.logger.hasHandlers():
            # Set up file handler
            file_handler = logging.FileHandler(self.log_file, mode='w')
            file_handler.setLevel(logging.INFO)
            
            # Set up formatter and add it to handler
            formatter = logging.Formatter('%(asctime)s - %(message)s')
            file_handler.setFormatter(formatter)
            
            # Add handler to logger
            self.logger.addHandler(file_handler)
        
        self.log("Logging initialized for RingNode") 

    def log(self, message):
        self.logger.info(message)
        print(message)
    
    def listen_for_messages(self):
        try:

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
                        # self.log(f"Connection from {client_address}")
                        self.log(f"Connection from {socket.gethostbyaddr(client_address[0])}")
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
                                if action in ["create", "get", "append", "merge", "get_from_replica"]:
                                    client_name = file_info["client_name"]
                                    self.client_socket_map[client_name] = s
                                
                                # remove socket after receiving complete client request data
                                if s in self.inputs:
                                    self.inputs.remove(s)
                                

                                self.inputtracker[client_socket] = True

                                self.handle_message(file_info, s)

                                # Add to outputs list if there's a response to be sent
                                # if s not in self.outputs:
                                #     self.outputs.append(s)
                        else:
                            # Client disconnected unexpectedly
                            self.log("Client disconnected")
                            if s in self.inputs:
                                self.inputs.remove(s)
                            if s in self.client_socket_map:
                                del self.client_socket_map[s]
                            s.close()
                            if s in self.data_buffer:
                                del self.data_buffer[s]

                # Handle writable sockets
                for s in writable:
                    if s in self.data_buffer and self.data_buffer[s]:
                        try:
                            # Send all data from data_buffer and clear it if successful
                            sent = s.send(self.data_buffer[s])
                            self.log(f"Sent {sent} bytes from data_buffer")
                            self.data_buffer[s] = self.data_buffer[s][sent:]

                            # Remove from outputs when all data is sent
                            if not self.data_buffer[s]:
                                self.outputs.remove(s)
                                if s in self.inputtracker:
                                    del self.inputtracker[s]
                                    s.close()

                        except BlockingIOError as e:
                            # Buffer is full, will retry in the next loop iteration
                            self.log("Buffer full, retrying send operation later...")
                            continue

                        except Exception as e:
                            self.log(f"Exception type: {type(e).__name__}")
                            self.log(f"Error sending data: {e}")
                            if s in self.outputs:
                                self.outputs.remove(s)
                            s.close()

                    else:
                        # No more data to send, remove from outputs
                        if s in self.outputs:
                            self.outputs.remove(s)

                # Handle exceptional sockets
                for s in exceptional:
                    self.log(f"Handling exceptional condition for {s.getpeername()}")
                    if s in self.outputs:
                        self.inputs.remove(s)
                    if s in self.outputs:
                        self.outputs.remove(s)
                    s.close()
                    if s in self.data_buffer:
                        del self.data_buffer[s]
        except Exception as e:
            self.log(f"Error in listen_for_messages: {e}")


                        


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
        
        elif action == "append":
            self.append_file(file_info, client_socket)
        
        elif action == "add":
            self.add_file(file_info, client_socket)
        
        elif action == "merge":
            self.merge_file(file_info, client_socket)
        
        elif action == "combine":
            self.combine_file(file_info, client_socket)
        
        elif action == "ack":
            self.acknowledge(file_info, client_socket)

        elif action == "write_replica":
            self.write_file_replica(file_info, client_socket)

        elif action == "ack_replica":
            self.ack_from_replica(file_info, client_socket)

        elif action == "ls":
            self.ls_filename(file_info, client_socket)
        
        else:
            self.log(f"Unknown action: {action}")

    def ls_filename(self, file_info, client_socket):
        filename = file_info["filename"]
        file_path = os.path.join(self.fs_directory, filename)
        is_present = os.path.exists(file_path)
        response = {"filename": filename, "is_present": is_present}

        try:
            # Send the ack message using msgpack for serialization
            # client_socket.sendall(msgpack.packb(response) + b"<EOF>")
            
            
            # Adding message to data_buffer and and outputs list, select.select will check when sock is avail and send data
            self.data_buffer[client_socket] = msgpack.packb(response) + b"<EOF>"
            self.outputs.append(client_socket)

            self.log(f"ls reply sent to client for file '{filename}'.")

            # close client socket after sending ack
            # client_socket.close()

        except (BlockingIOError, socket.error) as e:
            self.log(f"Failed to send ls reply for file '{filename}' - {e}")

    def monitor_membership_list(self):
        """
        Monitor the membership list for changes. If a new node joins or a node status changes to 'DEAD',
        find its 2 predecessors and 1 successor.
        """
        time.sleep(20)
        self.log("Monitoring membership list started")
        previous_membership_list = copy.deepcopy(self.process.membership_list)

        while not self.shutdown_flag.is_set():
            # Check for changes in the membership list every 2 seconds
            time.sleep(2)
            current_membership_list = copy.deepcopy(self.process.membership_list)

            if previous_membership_list != current_membership_list:
                self.log(f"Membership lists are not equal")
                # self.log(f"previous list\n{previous_membership_list}")
                # self.log(f"current list\n{current_membership_list}")
                for current_node in current_membership_list:
                    if not any(previous_node['node_id'] == current_node['node_id']
                            for previous_node in previous_membership_list):
                        # New node detected
                        self.log(f"New node detected: {current_node}")
                        self.handle_node_change(current_node, "joined")

                    elif current_node['status'] == 'DEAD' and any(
                            previous_node['node_id'] == current_node['node_id'] and previous_node['status'] != 'DEAD'
                            for previous_node in previous_membership_list):
                        # Node status changed to DEAD
                        self.log(f"Hydfs node marked as DEAD: {current_node}")
                        try:
                            time.sleep(2*self.process.protocol_period)
                            self.handle_node_change(current_node, "dead")
                        except Exception as e:
                            self.log(f"Exception caught in thread: {e}")

                previous_membership_list = copy.deepcopy(current_membership_list)
            self.log("No change in membership list")

    def handle_node_change(self, affected_node, change_type):
        """
        Handle the logic when a node joins or becomes dead by finding its 2 predecessors and 1 successor.
        Check if the current node (self.ring_id) is one of these.
        """
        # Sort the nodes in the ring by ring_id
        sorted_nodes = sorted(self.process.membership_list, key=lambda x: x['ring_id'])

        # Find the index of the affected node in the sorted list
        affected_node_index = next((i for i, node in enumerate(sorted_nodes) if node['node_id'] == affected_node['node_id']), None)
        succesor = []
        i = affected_node_index + 1  # Start from the next item
        list_length = len(sorted_nodes)

        # Loop until we collect 2 non-DEAD nodes, wrapping around if necessary
        while len(succesor) < 1:
            node = sorted_nodes[i % list_length]  # Wrap around using modulo
            if node.get('status') != 'DEAD':  # Only add nodes not marked as DEAD
                succesor.append(node)
            i += 1

        predecessors = []
        i = affected_node_index - 1 

        # Loop until we collect 2 non-DEAD nodes, wrapping around if necessary
        while len(predecessors) < 2:
            node = sorted_nodes[i % list_length]  # Wrap around using modulo
            if node.get('status') != 'DEAD':
                predecessors.append(node)
            i -= 1
        
        
        first_predecessor = predecessors[0]
        second_predecessor = predecessors[1]
        first_successor = succesor[0]
        self.log(f"{change_type.capitalize()} node {affected_node['node_id']} - 1st predecessor: {first_predecessor['node_id']}, 2nd predecessor: {second_predecessor['node_id']}, 1st successor: {first_successor['node_id']}")

        curr_index = next((i for i, node in enumerate(sorted_nodes) if node['node_id'] == self.process.node_id), None)
        
        replica_list = []
        i = curr_index + 1  # Start from the next item
        list_length = len(sorted_nodes)

        # Loop until we collect 2 non-DEAD nodes, wrapping around if necessary
        while len(replica_list) < 2:
            node = sorted_nodes[i % list_length]  # Wrap around using modulo
            if node.get('status') != 'DEAD':  # Only add nodes not marked as DEAD
                replica_list.append(node)
                self.log(f"Added {node['node_id']} to replica list")
            i += 1

        if affected_node_index is not None:
            # Get the two predecessors and one successor based on the index
            

            # Check if self.ring_id matches any of these roles
            is_first_predecessor = self.process.node_id == first_predecessor['node_id']
            is_second_predecessor = self.process.node_id == second_predecessor['node_id']
            is_first_successor = self.process.node_id == first_successor['node_id']


            for filename in os.listdir(self.fs_directory):
                actual_file_name = filename
                if '.log' in filename:
                    actual_file_name = filename.split('--')[1].replace('.log', '')
                file_path = os.path.join(self.fs_directory, filename)
                file_hash = self.hash_string(actual_file_name)
                primary_replica_list = self.get_next_n_nodes(file_hash, 1)
                self.log(f"Primary replica is {primary_replica_list[0]['node_id']}")
                is_primary_replica = self.process.node_id == primary_replica_list[0]['node_id']
                if is_primary_replica:
                    if change_type == 'dead':
                        # If node is dead then predecessor sends file to next 2 replicas
                        if is_first_predecessor or is_second_predecessor or first_successor:
                            #send file to next 2 replicas
                            self.log(f"I am a predecessor/succesor of {change_type} node {affected_node['node_id']} with ring_id {affected_node['ring_id']}")
                            for replica in replica_list:
                            
                                node_id = replica["node_id"]
                
                                # Extract hostname and port using regex
                                match = re.match(r"^(.*?)_(\d+)_.*$", node_id)
                                if not match:
                                    self.log(f"Error parsing node_id {node_id}")
                                    continue
                                
                                host = match.group(1)
                                port = self.hydfs_host_port

                                # Format write request
                                file_info_write_replica = {
                                "action": "write_replica",
                                "filename": filename,
                                }
                                file_info_write_replica["filesize"] = os.path.getsize(file_path)
                                with open(file_path, 'rb') as file:
                                    file_info_write_replica["content"] = file.read()

                                # Step 3: Send a create request to each replica
                                self.send_request(host, port, file_info_write_replica)
                        # elif first_successor:
                        #     self.log(f"I am the 1st successor of {change_type} node {affected_node['node_id']} with ring_id {affected_node['ring_id']}")
                        #     # Node dead first sucessor, sends file hashing to itself
                        #     pass
                    elif change_type == 'joined':
                        if is_first_predecessor or is_second_predecessor:
                            self.log(f"I am a predecessor of {change_type} node {affected_node['node_id']} with ring_id {affected_node['ring_id']}")
                            # New node predecessor sends it its files
                            pass

                        elif first_successor:
                            self.log(f"I am the 1st successor of {change_type} node {affected_node['node_id']} with ring_id {affected_node['ring_id']}")
                            # New node, sucessor finds files that hash to new node and sends them
                            
            # Log predecessors and successor for reference
            #self.log(f"{change_type.capitalize()} node {affected_node['node_id']} - 1st predecessor: {first_predecessor}, 2nd predecessor: {second_predecessor}, 1st successor: {first_successor}")



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

    

    def append_file(self, file_info, client_socket):
        # Step 1: Get all replicas for the specified filename
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

            # Format append request
            file_info_append = {
                "client_name": file_info["client_name"],
                "action": "add",
                "filename": file_info["filename"],
                "content": file_info["content"]
            }

            # Step 3: Send an append request to each replica
            self.send_request(host, port, file_info_append)

    
    def merge_file(self, file_info, client_socket):
        # Step 1: Get all replicas for the specified filename
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

            # Format append request
            file_info_append = {
                "client_name": file_info["client_name"],
                "action": "combine",
                "filename": file_info["filename"]
            }

            # Step 3: Send an append request to each replica
            self.send_request(host, port, file_info_append)



    def write_file_replica(self, file_info, client_socket):
        filename = file_info["filename"]
        file_content = file_info.get("content", b"")  # Default to empty content if not provided

        # Construct the full file path
        file_path = os.path.join(self.fs_directory, filename)

        # Prepare acknowledgment message indicating the file already exists
        ack_message = {
            "action": "ack_replica",
            "filename": filename,
        }

        # Check if the file already exists
        if os.path.exists(file_path):
            self.log(f"File '{filename}' already exists. Rewritting the file contents.")
            ack_message["status"] = "rewritten file"

        with open(file_path, "wb") as file:
            file.write(file_content)

        self.log(f"File '{filename}' written successfully.")
        ack_message["status"] = "write_complete"
            
            
        try:
            # Send ack message using msgpack for serialization
            # client_socket.sendall(msgpack.packb(ack_message) + b"<EOF>")
            # self.log(f"Acknowledgment sent to client for existing file '{filename}'.")

            # Adding message to data_buffer and and outputs list, select.select will check when sock is avail and send data
            self.data_buffer[client_socket] = msgpack.packb(ack_message) + b"<EOF>"
            self.outputs.append(client_socket)
            self.log(f"Acknowledgment sent to client for existing file '{filename}'.")

            # close client socket after sending ack
            # client_socket.close()

        except (BlockingIOError, socket.error) as e:
            self.log(f"Failed to send acknowledgment for existing file '{filename}' - {e}")



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
            # client_socket.sendall(msgpack.packb(ack_message) + b"<EOF>")
            

            # Adding message to data_buffer and and outputs list, select.select will check when sock is avail and send data
            self.data_buffer[client_socket] = msgpack.packb(ack_message) + b"<EOF>"
            self.outputs.append(client_socket)
            self.log(f"Acknowledgment sent to client '{filename}'.")


            # close client socket after sending ack
            # client_socket.close()

        except (BlockingIOError, socket.error) as e:
            self.log(f"Failed to send acknowledgment '{filename}' - {e}")




    def read_file(self, file_info, client_socket):
        client_name = file_info["client_name"]
        filename = file_info["filename"]

        # Construct the full file path
        file_path = os.path.join(self.fs_directory, filename)
        
        # Prepare acknowledgment message for the response
        ack_message = {
            "client_name": client_name,
            "action": "ack",
            "filename": filename,
        }

        # Check if the file exists
        if os.path.exists(file_path):
            # Read the main file content
            with open(file_path, "rb") as file:
                file_content = file.read()

            
            file_content = file_content.decode("utf-8")

            # Define the path of the append log file
            log_filename = f"{client_name}--{filename}.log"
            log_file_path = os.path.join(self.fs_directory, log_filename)

            # If the append log file exists, read and append its contents
            if os.path.exists(log_file_path):
                with open(log_file_path, "r") as log_file:
                    for line in log_file:
                        try:
                            # Parse each line as JSON
                            log_entry = json.loads(line.strip())
                            
                            # Check if log entry matches the client name and filename
                            if (log_entry.get("client_name") == client_name and
                                log_entry.get("filename") == filename):
                                
                                # Decode the base64 content and append to file content
                                log_content = log_entry["content"]
                                file_content +=  "\n" + log_content
                        except json.JSONDecodeError:
                            self.log(f"Error decoding log entry in {log_filename}")
            
            # Add the combined content and status to the ack message
            ack_message["status"] = "read_complete"
            ack_message["content"] = file_content
            self.log(f"File '{filename}' and append log '{log_filename}' read successfully.")
        
        else:
            # If file does not exist, update the ack message with an error
            ack_message["status"] = "file_not_found"
            self.log(f"File '{filename}' not found. Cannot read.")

        try:
            # Send the ack message using msgpack for serialization
            # client_socket.sendall(msgpack.packb(ack_message) + b"<EOF>")

            # Adding message to data_buffer and and outputs list, select.select will check when sock is avail and send data
            self.data_buffer[client_socket] = msgpack.packb(ack_message) + b"<EOF>"
            self.outputs.append(client_socket)
            self.log(f'File read succsefully {filename}')

            # close client socket after sending ack
            # client_socket.close()

            self.log(f"Acknowledgment sent to client with status '{ack_message['status']}' for file '{filename}'.")
        except (BlockingIOError, socket.error) as e:
            self.log(f"Failed to send acknowledgment for file '{filename}' - {e}")




    def add_file(self, file_info, client_socket):
        # Extract client name and filename from the file_info
        client_name = file_info["client_name"]
        filename = file_info["filename"]
        
        # Define basepath of the file
        base_file_path = os.path.join(self.fs_directory, filename)

        # Define the append log file path
        append_log_filename = f"{client_name}--{filename}.log"
        append_log_filepath = os.path.join(self.fs_directory, append_log_filename)
        

        # Check if the file on which the append is requested exists in the filesystem
        if not os.path.exists(base_file_path):
            error_message = {
                "client_name": client_name,
                "action": "ack",
                "filename": filename,
                "status": "file_not_found",
                "message": f"Error: File '{filename}' does not exist in the filesystem."
            }
            
            # Send error message back to the client
            try:
                # client_socket.sendall(msgpack.packb(error_message) + b"<EOF>")
                # self.log(f"Error message sent to client '{client_name}' - File '{filename}' does not exist.")

                # Adding message to data_buffer and and outputs list, select.select will check when sock is avail and send data
                self.data_buffer[client_socket] = msgpack.packb(error_message) + b"<EOF>"
                self.outputs.append(client_socket)
                self.log(f"Error message sent to client '{client_name}' - File '{filename}' does not exist.")


                # close client socket after sending ack
                # client_socket.close()
            
            except (BlockingIOError, socket.error) as e:
                self.log(f"Failed to send error message for file '{filename}' - {e}")
            
            return  # Exit the function since the file does not exist

        # Increment the sequence number for each append action
        if (client_name, filename) not in self.sequence_numbers:
            self.sequence_numbers[(client_name, filename)] = 0  # Initialize if it doesn't exist
        sequence_number = self.sequence_numbers[(client_name, filename)] + 1
        self.sequence_numbers[(client_name, filename)] = sequence_number

        # Convert the content to a base64 string for JSON serialization
        content_str = file_info["content"].decode('utf-8')

        # Create the new log entry with sequence number
        file_info_append = {
            "client_name": client_name,
            "action": "append",
            "filename": filename,
            "content": content_str,
            "sequence_number": sequence_number
        }

        # Write the new entry to the JSON file in append mode
        with open(append_log_filepath, 'a') as json_file:
            json_file.write(json.dumps(file_info_append) + '\n')

        self.log(f"Append action added to '{append_log_filename}' with sequence number {sequence_number}.")

        # Send acknowledgment back to the client
        ack_message = {
            "client_name": client_name,
            "action": "ack",
            "filename": filename,
            "status": "append_logged",
            "sequence_number": sequence_number
        }

        try:
            # Send the ack message to the client
            # client_socket.sendall(msgpack.packb(ack_message) + b"<EOF>")
            

            # Adding message to data_buffer and and outputs list, select.select will check when sock is avail and send data
            self.data_buffer[client_socket] = msgpack.packb(ack_message) + b"<EOF>"
            self.outputs.append(client_socket)
            self.log(f"Acknowledgment sent to client '{client_name}' for file '{filename}'.")

            # close client socket after sending ack
            # client_socket.close()

        except (BlockingIOError, socket.error) as e:
            self.log(f"Failed to send acknowledgment for file '{filename}' - {e}")



    def combine_file(self, file_info, client_socket):
        client_name = file_info["client_name"]
        filename = file_info["filename"]

        # Construct the full file path
        file_path = os.path.join(self.fs_directory, filename)
        
        # Prepare acknowledgment message for the response
        ack_message = {
            "client_name": client_name,
            "action": "ack",
            "filename": filename,
        }

        # Check if the main file exists
        if os.path.exists(file_path):

            self.log(f"File Exists, starting merge of {filename}")

            # Read the main file content
            with open(file_path, "rb") as file:
                file_content = file.read().decode("utf-8")  # Decode binary to UTF-8

            # Collect log files with the target filename prefix
            log_files = [
                log_filename for log_filename in os.listdir(self.fs_directory)
                if log_filename.endswith(".log") and (f"--{filename}" in log_filename)
            ]

            self.log(f"Collected log file for merge of {filename}: {log_files}")
            
            # Sort log files lexicographically by client name (assumes client name is part of the log filename)
            log_files.sort()

            # Read and append each log file's content in the sorted order
            for log_filename in log_files:
                log_file_path = os.path.join(self.fs_directory, log_filename)
                
                # Read the contents of the log file
                with open(log_file_path, "r") as log_file:
                    for line in log_file:
                        try:
                            # Parse each line as JSON
                            log_entry = json.loads(line.strip())
                            # Only append content if the filename matches
                            if log_entry.get("filename") == filename:
                                log_content = log_entry["content"]
                                file_content += "\n" + log_content
                        except json.JSONDecodeError:
                            self.log(f"Error decoding log entry in {log_filename}")

                # Delete the log file after processing
                os.remove(log_file_path)
                self.log(f"Deleted log file '{log_filename}' after merging.")

            # Overwrite the main file with the combined content
            with open(file_path, "w", encoding="utf-8") as file:
                file.write(file_content)
            self.log(f"File '{filename}' overwritten with combined content.")

            # Add the combined content and status to the acknowledgment message
            ack_message["status"] = "merge_complete"
            self.log(f"File '{filename}' and associated logs combined successfully.")
        
        else:
            # If the main file does not exist, update the acknowledgment message with an error
            ack_message["status"] = "file_not_found"
            self.log(f"File '{filename}' not found. Cannot read.")

        try:
            # Send the acknowledgment message using msgpack for serialization
            # client_socket.sendall(msgpack.packb(ack_message) + b"<EOF>")

            # Adding message to data_buffer and and outputs list, select.select will check when sock is avail and send data
            self.data_buffer[client_socket] = msgpack.packb(ack_message) + b"<EOF>"
            self.outputs.append(client_socket)
            self.log(f"Acknowledgment sent to client with status '{ack_message['status']}' for file '{filename}'.")


            # close client socket after sending ack
            # client_socket.close()

        except (BlockingIOError, socket.error) as e:
            self.log(f"Failed to send acknowledgment for file '{filename}' - {e}")


    def ack_from_replica(self, file_info, s):
        self.log(f"Acknowledgment received for file info: {file_info}")
        # s.close()



    def acknowledge(self, file_info, s):
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
                    # client_socket_to_use.sendall(msgpack.packb(file_info) + b"<EOF>")

                    # Adding message to data_buffer and and outputs list, select.select will check when sock is avail and send data
                    self.data_buffer[client_socket_to_use] = msgpack.packb(file_info) + b"<EOF>"
                    self.outputs.append(client_socket_to_use)
                    self.log(f"Acknowledgment sent to {client_name} for file '{file_info['filename']}'.")


                    # close client socket after sending ack
                    # client_socket_to_use.close()



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

        if self.membership_monitor_thread.is_alive():
            self.log("Membership thread is still running; proceeding to join...")
            self.membership_monitor_thread.join()  # Wait for the listener thread to finish
            self.log("Membership thread joined successfully.")
        else:
            self.log("Membership thread is not running, skipping join.")

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











