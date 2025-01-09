# How to assign nodes 
import socket
import os
import hashlib
from process import Process
import socket
import msgpack
import threading
import select
import signal
from collections import defaultdict



class RingNode:
    def __init__(self):
        """
        Initialize the RingNode with a ring_id based on the host_name and pass it along with other params to Process.
        """
        self.host_name = socket.gethostname()
        self.ring_id = self.hash_string(self.host_name)
        self.process = Process(socket.gethostname(), 5000, 'fa24-cs425-6901.cs.illinois.edu', 5000, False, 20, 10, 0, self.ring_id)
        
        # Create a TCP/IP socket for incoming connections
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(('fa24-cs425-6901.cs.illinois.edu', 65432))
        self.server_socket.listen()
        self.sockets_list = [self.server_socket]
        self.clients = {}
        self.outgoing_connections = {}
        self.message_buffer = defaultdict(lambda :"")
        self.fs_directory = "/home/chaskar2/distributed-logger/hyDFS/filesystem"
        self.local_directory = "/home/chaskar2/distributed-logger/hyDFS"

        self.shutdown_flag = threading.Event()
        self.listen_thread = threading.Thread(target=self.listen_for_messages)
        self.listen_thread.start()
        self.send_join_request()
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)


    def shutdown(self, signum, frame):
        """ Graceful shutdown function """
        self.log("Shutting down HyDFS node ...")
        self.shutdown_flag.set()  # Signal the listener thread to stop
        if self.listen_thread.is_alive():
            self.listen_thread.join()  # Wait for the listener thread to finish
        self.server_socket.close()  # Close the socket

        signal.signal(signal.SIGINT, self.process.shutdown)
        signal.signal(signal.SIGTERM, self.process.shutdown)

        self.log("hyDFS Node shut down gracefully.")

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
    
    def _close_connection(self, socket):
        print(f"Closing connection from {self.clients.get(socket)}")
        socket.close()  # Explicitly close the socket
        if socket in self.sockets_list:
            self.sockets_list.remove(socket)
        if socket in self.clients:
            del self.clients[socket]
        if socket in self.message_buffer:
            del self.message_buffer[socket]


    def listen_for_messages(self):
        while not self.shutdown_flag.is_set():
            # Use select to wait for activity on any of the sockets
            read_sockets, write_sockets, exception_sockets = select.select(self.sockets_list, [], self.sockets_list)

            # Handle read_sockets (ready to read)
            for notified_socket in read_sockets:
                if notified_socket == self.server_socket:
                    # Incoming connection
                    client_socket, client_address = self.server_socket.accept()
                    self.sockets_list.append(client_socket)
                    self.clients[client_socket] = client_address
                    self.message_buffer[client_socket] = ""  # Initialize buffer for this client
                    self.outgoing_connections[notified_socket] = ""
                    print(f"Accepted new connection from {client_address}")

                    # self.handle_message(notified_socket, self.outgoing_connections[notified_socket], self.message_buffer[notified_socket])

                elif notified_socket in self.outgoing_connections:
                    # Outgoing connection has some data to read
                    data = notified_socket.recv(1024)
                    if not data:
                        # Connection closed by remote host
                        print(f"Outgoing connection to {self.outgoing_connections[notified_socket]} closed.")
                        self.sockets_list.remove(notified_socket)
                        del self.outgoing_connections[notified_socket]
                        del self.message_buffer[notified_socket]
                    else:
                        # Accumulate data in the message buffer for the outgoing connection
                        self.message_buffer[notified_socket] += data.decode()
                        # Optionally handle the message if you reach some delimiter or condition
                        self.handle_message(notified_socket, self.message_buffer[notified_socket])
                else:
                    # Handle incoming data from an already established connection
                    data = notified_socket.recv(1024)
                    if not data and "<EOF>" in self.message_buffer.get(notified_socket, ""):
                        complete_message = self.message_buffer.get(notified_socket, "")
                        if complete_message:
                            self.handle_message(notified_socket, complete_message)
                        print(f"Closed connection from {self.clients[notified_socket]}")
                        self.sockets_list.remove(notified_socket)
                        del self.clients[notified_socket]
                        del self.message_buffer[notified_socket]
                        notified_socket.close()
                    else:
                        self.message_buffer[notified_socket] += str(data.decode())

            # Handle exceptional sockets (error handling)
            for notified_socket in exception_sockets:
                if notified_socket in self.clients:
                    self.sockets_list.remove(notified_socket)
                    del self.clients[notified_socket]
                    del self.message_buffer[notified_socket]
                    notified_socket.close()
                elif notified_socket in self.outgoing_connections:
                    self.sockets_list.remove(notified_socket)
                    del self.outgoing_connections[notified_socket]
                    del self.message_buffer[notified_socket]
                    notified_socket.close()
                
                self._close_connection(notified_socket)

    
    def handle_file_save(self, message):
        filename = message.split("<START_OF_NAME>")[1].split("<END_OF_NAME>")[0]
        print(f"Received file: {filename}")

        file_content = message.split("<END_OF_NAME>")[1]

        print("FILE CONTENT:", file_content)

        # Open a new file to write the received data

        file_path = os.path.join("received_files", filename)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        with open(file_path, 'w') as file:
            file.write(file_content.strip("<EOF>"))



    def handle_message(self, client_socket, message):
        """
        Handles the messages sent from the client. If a file is being sent, it will save the file.
        """
        # Check if the message indicates the start of a file transfer

        message = str(message)
        print("RECEIVED MESSAGE: ", message)

        if message.startswith("FILE_TRANSFER:"):
            self.handle_file_save(message)

        else:
            # Handle other types of messages here
            print(f"Received non-file message: {message}")

        # Send the appropriate response back to the client

    
    def create_outgoing_connection(self, remote_host, remote_port):
        """
        Create a non-blocking outgoing connection to a remote server.
        """
        outgoing_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        outgoing_socket.setblocking(False)  # Non-blocking socket
        try:
            outgoing_socket.connect((remote_host, remote_port))
        except BlockingIOError:
            pass  # Connection is in progress; select will detect when it's done

        sockets_list.append(outgoing_socket)
        outgoing_connections[outgoing_socket] = (remote_host, remote_port)
        message_buffer[outgoing_socket] = ""

        print(f"Initiating outgoing connection to {remote_host}:{remote_port}")

    

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



