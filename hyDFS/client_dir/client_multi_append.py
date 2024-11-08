import socket
import random
import os
import threading
import time
import msgpack
import sys

class Client:
    def __init__(self, client_name, action, filename, append_filename, file_path):
        self.client_name = client_name
        self.action = action
        self.file_name = filename
        self.file_path = file_path

    def create_request(self):
        message = {
            "client_name": self.client_name,
            "action": self.action,
            "filename": self.file_name
        }

        if self.action == "append" and self.file_path:
            if os.path.exists(self.file_path):
                message["filesize"] = os.path.getsize(self.file_path)
                with open(self.file_path, 'rb') as file:
                    message["content"] = file.read()
            else:
                print(f"Error: File at {self.file_path} does not exist.")
                return None
        
        return msgpack.packb(message) + b"<EOF>"
    
    def receive_response(self, sock):
        """Receives and deserializes the response from the server."""
        response = b""
        try:
            while True:
                chunk = sock.recv(1024)
                if b"<EOF>" in chunk:
                    response += chunk.replace(b"<EOF>", b"")
                    break
                response += chunk
            return msgpack.unpackb(response)
        except Exception as e:
            print(f"Error receiving response: {e}")
            return None

    def send_request(self, server_address):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect(server_address)
                message = self.create_request()
                if message:
                    sock.sendall(message)
                    response = self.receive_response(sock)
                    print(f"Response from server {server_address}: {response}")
        except Exception as e:
            print(f"Error communicating with server {server_address}: {e}")

def client_thread(server_list, client_name, action, filename, append_filename, file_path):
    # Choose a random server from the list
    server_address = random.choice(server_list)
    print(server_address)
    client = Client(client_name, action, filename, append_filename, file_path)
    client.send_request(server_address)

def main(server_list, num_clients, action, filenames, append_filenames, file_paths):
    threads = []
    for i in range(num_clients):
        client_name = f"client_{i+1}"
        thread = threading.Thread(target=client_thread, args=(server_list, client_name, action, filenames[i], append_filenames[i], file_paths[i]))
        threads.append(thread)
        thread.start()
        time.sleep(0.1)  # Add delay to avoid overwhelming the servers

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

# Example usage:
if __name__ == "__main__":

    if len(sys.argv) < 2:
        print("Usage: python client_multi_append.py <filename>")
        sys.exit(1)

    filename = sys.argv[1]

    # List of server addresses (IP, port)
    servers = [(f'fa24-cs425-69{i:02d}.cs.illinois.edu', 5001) for i in range(1, 11)]

    # Number of clients to create
    num_clients = 4
    action = "append"
    filenames = [f"{filename}"]*4
    append_filenames = [f"test_multi_append_client{i}.txt" for i in range(1,5)]
    # file_paths = [f"../local/test_multi_append_client{i}.txt" for i in range(1,5)]
    file_paths = [f"../local/random_file.txt" for i in range(1,5)]


    main(servers, num_clients, action, filenames, append_filenames, file_paths)
