import socket
import os
import msgpack
import time
import random

def insert_at_random_positions(big_array, small_array):
    # Generate random positions for the small array elements in the big array
    positions = random.sample(range(len(big_array) + 1), len(small_array))
    positions.sort()  # Sort to maintain relative order when inserting

    result = []
    small_index = 0

    # Iterate through the big array and insert elements at the chosen positions
    for i in range(len(big_array) + len(small_array)):
        if small_index < len(positions) and i == positions[small_index]:
            result.append(small_array[small_index])
            small_index += 1
        else:
            result.append(big_array[i - small_index])  # Adjust index for added small elements

    return result

class LRUCache:

    def __init__(self, capacity: int, client_name: str):
        self.dic = {}
        self.capacity = capacity
        self.client_name = client_name

    def get(self, key: int) -> int:
        if key in self.dic:
            value = self.dic[key]
            del self.dic[key]
            self.dic[key] = value  # Move to the end to mark as recently used
            return value
        return -1

    def put(self, key: int, value: int) -> None:
        if key in self.dic:
            del self.dic[key]
        elif len(self.dic) == self.capacity:
            del self.dic[next(iter(self.dic))]  # Remove the least recently used
        self.dic[key] = value

    def invalidate(self, key: int) -> None:
        if key in self.dic:
            del self.dic[key]

class FileClient:
    def __init__(self, server_port, client_name, cache_capacity=5):
        self.server_port = server_port
        self.client_name = client_name
        self.client_socket = None
        self.cache = LRUCache(capacity=cache_capacity, client_name=client_name)

    def connect(self, server_ip):
        """Establishes a connection to the server."""
        try:
            self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.client_socket.connect((server_ip, self.server_port))
            self.client_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            print("Connected to the server.")
        except Exception as e:
            print(f"Connection error: {e}")
            self.close()

    def prepare_message(self, action, file_name=None, file_path=None, append_file_path=None):
        """Prepares the message based on the action."""
        message = {
            "client_name": self.client_name,
            "action": action,
            "filename": file_name
        }

        if action == "create" and file_path:
            if os.path.exists(file_path):
                message["filesize"] = os.path.getsize(file_path)
                with open(file_path, 'rb') as file:
                    message["content"] = file.read()
            else:
                print(f"Error: File at {file_path} does not exist.")
                return None
        elif action == "append" and append_file_path:
            if os.path.exists(append_file_path):
                message["filesize"] = os.path.getsize(append_file_path)
                with open(append_file_path, 'rb') as file:
                    message["content"] = file.read()
            else:
                print(f"Error: File at {append_file_path} does not exist.")
                return None

        return msgpack.packb(message) + b"<EOF>"

    def send_message(self, message):
        """Sends the prepared message to the server."""
        try:
            self.client_socket.sendall(message)
            print("Message sent successfully.")
        except Exception as e:
            print(f"Error sending message: {e}")

    def receive_response(self):
        """Receives and deserializes the response from the server."""
        response = b""
        try:
            while True:
                chunk = self.client_socket.recv(1024)
                if b"<EOF>" in chunk:
                    response += chunk.replace(b"<EOF>", b"")
                    break
                response += chunk
            return msgpack.unpackb(response)
        except Exception as e:
            print(f"Error receiving response: {e}")
            return None

    def close(self):
        """Closes the socket connection."""
        if self.client_socket:
            self.client_socket.close()
            print("Connection closed.")

    def perform_action(self, action, server_ip, file_name=None, file_path=None, append_file_path=None):
        """Main function to perform the action and measure latency."""
        start_time = time.time()
        if action == "get":
            cached_content = self.cache.get(file_name)
            if cached_content != -1:
                print(f"Cache hit: {file_name}")
                # print("Cached content:", cached_content)
                print(f"Latency for 'get' from cache: {time.time() - start_time:.4f} seconds")
                return time.time() - start_time
            else:
                print(f"Cache miss: {file_name}")
        
        self.connect(server_ip)
        if self.client_socket:
            message = self.prepare_message(action, file_name, file_path, append_file_path)
            if message:
                self.send_message(message)
                response = self.receive_response()
                if response:
                    # print("Server response:", response)
                    if action == "get" and response.get("status") == 'read_complete':
                        content = response.get("content")
                        self.cache.put(file_name, content)
                        self.close()
                        return time.time() - start_time

                    elif action == "append":
                        self.cache.invalidate(file_name)
                    elif action == "merge":
                        self.cache.invalidate(file_name)  # Simulate invalidating entry
            self.close()
        print(f"Latency for '{action}': {time.time() - start_time:.4f} seconds")
        return 0

if __name__ == "__main__":
    import sys
    
    # Example commands to measure latency
    # commands = [
    #     {"action": "get", "file_name": "file1.txt"},
    #     {"action": "create", "file_name": "file2.txt", "file_path": "./file2.txt"},
    #     {"action": "append", "file_name": "file1.txt", "append_file_path": "./append_file.txt"},
    #     {"action": "merge", "file_name": "file1.txt"}
    # ]
    #create files 10k
    #commands = [{"action": "create", "file_name": f"test_cache_{i}.txt", "file_path": f"../local/test_cache/test_cache_{i}.txt"} for i in range(1, 101)]
    #do random gets 20k
    #commands = [{"action": "get", "file_name": f"test_cache_{random.randint(1, 100)}.txt"} for i in range(1, 91)]
    
    
    
    #do zipfian gets
    import numpy as np

    # Parameters for the Zipfian distribution
    n_items = 100  # Total number of unique items
    zipf_param = 1.2  # Parameter to control the skewness of the distribution (1 < s < 2)

    # Initialize an empty list to store valid samples
    zipf_samples = []

    # Continue sampling until we reach 100 valid samples
    while len(zipf_samples) < 90:
        # Generate a batch of samples
        new_samples = np.random.zipf(zipf_param, 90)
        
        # Filter out samples that are larger than the number of items
        new_samples = new_samples[new_samples <= n_items]
        
        # Add valid samples to the list, but only up to the required number (100)
        zipf_samples.extend(new_samples[:90 - len(zipf_samples)])

    # Generate random 'get' commands based on the sampled indices
    commands = [{"action": "get", "file_name": f"test_cache_{i}.txt"} for i in zipf_samples]


    # 10 percent append commands
    append_commands = [{"action": "append", "file_name": f"test_cache_{random.randint(1, 100)}.txt", "append_file_path": "../local/test_multi_append_client1.txt"} for i in range(0, 10)]
 
    #vary cache capacity from 0 to 10,000
    commands = insert_at_random_positions(commands, append_commands)

    client = FileClient(server_port=5001, client_name="client1", cache_capacity=50)

    total_get_time = 0
    for command in commands:
        servers = [f'fa24-cs425-69{i:02d}.cs.illinois.edu' for i in range(1, 11)]
        server_ip = random.choice(servers)
        total_get_time += client.perform_action(
            action=command["action"],
            server_ip=server_ip,
            file_name=command.get("file_name"),
            file_path=command.get("file_path"),
            append_file_path=command.get("append_file_path")
        )
    print(len(commands))
    print(f"Overall get time for 100 operations {total_get_time}")
