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

def main(server_list, clients, action, filenames, append_filenames, file_paths):
    threads = []
    for client_name in clients:
        thread = threading.Thread(target=run_for_client, args=(server_list, client_name, action, filenames, append_filenames, file_paths))
        threads.append(thread)
        thread.start()
        # time.sleep(0.1)
    
    # Wait for all threads to complete
    for thread in threads:
        thread.join()



def run_for_client(server_list, client_name, action, filenames, append_filenames, file_paths):
    
    for i in range(len(filenames)):
        print(f"append num: {i+1} for client: {client_name}")
        client_name = client_name
        client_thread(server_list, client_name, action, filenames[i], append_filenames[i], file_paths[i])


# Example usage:
if __name__ == "__main__":

    if len(sys.argv) < 2:
        print("Usage: python client_multi_append.py <filename>")
        sys.exit(1)

    filename = sys.argv[1]

    # List of server addresses (IP, port)
    servers = [(f'fa24-cs425-69{i:02d}.cs.illinois.edu', 5001) for i in range(1, 11)]
    append_count = 5
    # Number of clients to create
    clients = ["client_1", "client_2", "client_3", "client_4", "client_5", "client_6", "client_7", "client_8", "client_9", "client_10"]
    action = "append"
    filenames = [f"{filename}"]*append_count
    append_filenames = [f"/home/chaskar2/distributed-logger/hyDFS/local/test_cache/test_cache_{i}.txt" for i in range(1,append_count+1)]
    # file_paths = [f"../local/test_multi_append_client{i}.txt" for i in range(1,5)]
    file_paths = [f"./report_1/random_text_10mb.txt" for i in range(1,append_count+1)]


    main(servers, clients, action, filenames, append_filenames, file_paths)
