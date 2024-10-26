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
        
        self.fs_directory = "/home/chaskar2/distributed-logger/hyDFS/filesystem"
        self.local_directory = "/home/chaskar2/distributed-logger/hyDFS"


        # Set up signal handlers for graceful shutdown
        self.shutdown_flag = threading.Event() 
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

        self.send_join_request()


    
    def listen_for_messages(self):
        self.log(f"Server listening on {self.host_name}:{self.hydfs_host_port}")

        inputs = [self.server_socket]
        outputs = []
        data_buffer = {}


        while not self.shutdown_flag.is_set():
            readable, _, exceptional = select.select(inputs, outputs, inputs,5)

            # Handle readable sockets
            for s in readable:
                if s is self.server_socket:
                    # Accept new client connection
                    client_socket, client_address = self.server_socket.accept()
                    client_socket.setblocking(False)
                    inputs.append(client_socket)
                    data_buffer[client_socket] = b""
                    print(f"Connection from {client_address}")

                else:
                    # Read data from an existing client
                    data = s.recv(4096)
                    if data:
                        data_buffer[s] += data

                        # Check if we have received the complete dictionary with "<EOF>"
                        if b"<EOF>" in data_buffer[s]:
                            # Extract the complete data before <EOF>
                            complete_data, _, _ = data_buffer[s].partition(b"<EOF>")
                            file_info = msgpack.unpackb(complete_data)
                            
                            if file_info["action"] == "create_file":
                                self.create_file(file_info)

                            # Clean up this client
                            inputs.remove(s)
                            s.close()
                            del data_buffer[s]

                    else:
                        # Client disconnected unexpectedly
                        print("Client disconnected unexpectedly")
                        inputs.remove(s)
                        s.close()
                        if s in data_buffer:
                            del data_buffer[s]


    def create_file(self, file_info):

        filename = file_info['filename']
        file_content = file_info['content']

        # Save the file to the server's local storage
        file_path = os.path.join("received_files", filename)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        with open(file_path, 'wb') as f:
            f.write(file_content)

        print(f"File '{filename}' received and saved successfully.")

    
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

    def handle_create_file(filename):

        file_primary_node_id = self.hash_string(filename)
        next_2_nodes = get_next_n_nodes(file_primary_node_id, 2)

        file_primary_node = {
                'node_id': self.process.node_id,
                'ring_id': self.process.ring_id
        }

        self.send_file_actual(file_primary_node, filename)    

        for node in next_2_nodes:
            self.send_file_actual(node, filename)


    def get_next_n_nodes(node_id, n):
        live_nodes = [node for node in self.process.membership_list if node['status'] == 'LIVE']
        
        sorted_nodes = sorted(live_nodes, key=lambda x: x['ring_id'])
        
        current_index = next((i for i, node in enumerate(sorted_nodes) if node['node_id'] == node_id), None)
        
        if current_index is None:
            raise ValueError(f"Node {node_id} not found in the membership list.")
        
        next_nodes = []
        for i in range(1, n+1):
            next_index = (current_index + i) % len(sorted_nodes)
            next_nodes.append({
                'node_id': sorted_nodes[next_index]['node_id'],
                'ring_id': sorted_nodes[next_index]['ring_id']
            })

        print(next_nodes)
        
        return next_nodes




    def handle_message(self, message, addr):
        # Existing message handling...
        if message['type'] == 'file_upload':
            self.handle_file_upload(message, addr)
        elif message['type'] == 'create_file':
            self.handle_create_file(message, addr)
        else:
            # Handle other message types...
            pass


    

    def send_file_actual(self, node_id, filename):
        """Handle file upload request from a client."""

        addr = node_id.split("-")[0]
        self.log(f"Starting file download: {filename} to {addr}")

        local_file_path = os.path.join(self.local_directory, filename)

        if os.path.exists(local_file_path):
            filesize = os.path.getsize(local_file_path)

            with open(local_file_path, 'rb') as f:
                while (data := f.read(4096)):  # Adjust buffer size as needed
                    message = {
                    'type': 'receive_this',
                    'filename': filename,
                    'data': data
                    }
                    self.server_socket.sendto(message, addr)

            self.log(f"File upload completed: {filename}")
        else:
            self.log(f"File {filename} does not exist, sending error message.")
            self.server_socket.sendto(json.dumps({'error': 'File not found'}).encode(), addr)

    def log(self, message):
        print(message)  # Replace with a more sophisticated logging if needed



if __name__ == '__main__':
    ring_node = RingNode()











