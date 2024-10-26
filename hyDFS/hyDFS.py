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



class RingNode:
    def __init__(self):
        """
        Initialize the RingNode with a ring_id based on the host_name and pass it along with other params to Process.
        """
        self.host_name = socket.gethostname()
        self.hydfs_host_port = 5001

        self.ring_id = self.hash_string(self.host_name)
        self.process = Process(socket.gethostname(), 5000, 'fa24-cs425-6901.cs.illinois.edu', 5000, False, 20, 10, 0, self.ring_id)
        
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host_name, self.hydfs_host_port))
        self.server_socket.listen()
        self.server_socket.setblocking(False)
        print(f"Server listening on {self.host_name}:{self.hydfs_host_port}")


        self.listen_thread = threading.Thread(target=self.listen_for_messages)
        self.listen_thread.start()
        
        self.fs_directory = "filesystem"
        self.local_directory = ""


        # Set up signal handlers for graceful shutdown
        self.shutdown_flag = threading.Event() 
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

        # Socket message handling variables
        self.inputs = [self.server_socket]
        self.outputs = []
        self.data_buffer = {}
        
        self.send_join_request()



    
    def listen_for_messages(self):
        self.log(f"Server listening on {self.host_name}:{self.hydfs_host_port}")

        while not self.shutdown_flag.is_set():
            readable, _, exceptional = select.select(self.inputs, self.outputs, self.inputs, 5)

            # Handle readable sockets
            for s in readable:
                if s is self.server_socket:
                    # Accept new client connection
                    client_socket, client_address = self.server_socket.accept()
                    client_socket.setblocking(False)
                    self.inputs.append(client_socket)
                    self.data_buffer[client_socket] = b""
                    print(f"Connection from {client_address}")

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
                            
                            self.handle_message(file_info, s)

                            # Clean up this client
                            self.inputs.remove(s)
                            s.close()
                            del self.data_buffer[s]
                    else:
                    # Client disconnected unexpectedly
                        print("Client disconnected unexpectedly")
                        self.inputs.remove(s)
                        s.close()
                    if s in self.data_buffer:
                        del self.data_buffer[s]


    def handle_message(self, file_info, client_socket):

        action = file_info.get("action")

        if action == "create":
            self.create_file(file_info, client_socket)
        
        elif action == "write":
            self.write_file(file_info, client_socket)
        
        elif action == "ack":
            self.acknowledge(file_info, client_socket)
        
        else:
            print(f"Unknown action: {action}")


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
                print(f"Error parsing node_id {node_id}")
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


    # Writes the files after receiving a  "write" request (handle_message)
    def write_file(self, file_info, client_socket):
        filename = file_info["filename"]
        file_content = file_info.get("content", b"")  # Default to empty content if not provided


        file_path = os.path.join(self.fs_directory, filename)

        # Write content to file
        with open(file_path, "wb") as file:
            file.write(file_content)

        print(f"File '{filename}' written successfully.")

        # Send acknowledgment back to the client socket
        ack_message = {
            "client_name": file_info["client_name"],
            "action": "ack",
            "filename": filename
        }
        
        try:
            # Send ack message using msgpack for serialization
            client_socket.sendall(msgpack.packb(ack_message) + b"<EOF>")
            print(f"Acknowledgment sent to client for '{filename}'.")

        except (BlockingIOError, socket.error) as e:
            print(f"Failed to send acknowledgment for '{filename}' - {e}")


    def acknowledge(self, file_info, client_socket):
        print(f"Acknowledgment received for file info: {file_info}")
    

    # Creates a socket to the replica and sends the "write" file_info message over the socket
    def send_request(self, host, port, file_info):
        # Create a non-blocking socket connection to the target replica
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setblocking(False)

        try:
            s.connect((host, port))
        except BlockingIOError:
            # Non-blocking connect may raise this; it’s expected behavior
            pass

        # Add the socket to self.inputs for select monitoring
        self.inputs.append(s)
        self.data_buffer[s] = b""

        # Prepare the file info message and store it to send after connection
        message = msgpack.packb(file_info) + b"<EOF>"
        
        # Keep track of data to be sent in an output buffer
        try:
            # Send the message immediately
            s.sendall(message)
            print(f"Write request sent to {host}:{port} for file {file_info['filename']}")

        except (BlockingIOError, socket.error) as e:
            print(f"Failed to send write request to {host}:{port} - {e}")
        
        print(f"Write request initialized for {host}:{port} for file {file_info['filename']}")



    
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
        live_nodes = [node for node in self.process.membership_list if node['status'] == 'LIVE']
        
        sorted_nodes = sorted(live_nodes, key=lambda x: x['ring_id'])
        
        current_index = next((i for i, node in enumerate(sorted_nodes) if node['ring_id'] == ring_id), None)
        
        if current_index is None:
            raise ValueError(f"Node with ring_id:{ring_id} not found in the membership list.")
        
        next_nodes = []
        for i in range(1, n+1):
            next_index = (current_index + i) % len(sorted_nodes)
            next_nodes.append({
                'node_id': sorted_nodes[next_index]['node_id'],
                'ring_id': sorted_nodes[next_index]['ring_id']
            })

        print(next_nodes)
        
        return next_nodes
    

    def get_all_replicas(self, filename):

        file_primary_ring_id = self.hash_string(filename)

        primary_node_info = {
                'node_id': self.process.node_id,
                'ring_id': self.process.ring_id
        }

        next_2_nodes_info = self.get_next_n_nodes(file_primary_ring_id, 2)

        all_replicas = [primary_node_info] + next_2_nodes_info

        return all_replicas

    
    def log(self, message):
        print(message)  # Replace with a more sophisticated logging if needed



if __name__ == '__main__':
    ring_node = RingNode()











