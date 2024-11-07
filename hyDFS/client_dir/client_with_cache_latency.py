import socket
import os
import msgpack
import time

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
    def __init__(self, server_ip, server_port, client_name, cache_capacity=5):
        self.server_ip = server_ip
        self.server_port = server_port
        self.client_name = client_name
        self.client_socket = None
        self.cache = LRUCache(capacity=cache_capacity, client_name=client_name)

    def connect(self):
        """Establishes a connection to the server."""
        try:
            self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.client_socket.connect((self.server_ip, self.server_port))
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

    def perform_action(self, action, file_name=None, file_path=None, append_file_path=None):
        """Main function to perform the action and measure latency."""
        start_time = time.time()
        if action == "get":
            cached_content = self.cache.get(file_name)
            if cached_content != -1:
                print(f"Cache hit: {file_name}")
                print("Cached content:", cached_content)
                print(f"Latency for 'get' from cache: {time.time() - start_time:.4f} seconds")
                return
            else:
                print(f"Cache miss: {file_name}")
        
        self.connect()
        if self.client_socket:
            message = self.prepare_message(action, file_name, file_path, append_file_path)
            if message:
                self.send_message(message)
                response = self.receive_response()
                if response:
                    print("Server response:", response)
                    if action == "get" and response.get("status") == 'read_complete':
                        content = response.get("content")
                        self.cache.put(file_name, content)
                    elif action == "append":
                        self.cache.invalidate(file_name)
                    elif action == "merge":
                        self.cache.invalidate(file_name)  # Simulate invalidating entry
            self.close()
        print(f"Latency for '{action}': {time.time() - start_time:.4f} seconds")

if __name__ == "__main__":
    import sys

    # Example commands to measure latency
    commands = [
        {"action": "get", "file_name": "file1.txt"},
        {"action": "create", "file_name": "file2.txt", "file_path": "./file2.txt"},
        {"action": "append", "file_name": "file1.txt", "append_file_path": "./append_file.txt"},
        {"action": "merge", "file_name": "file1.txt"}
    ]

    client = FileClient(server_ip="127.0.0.1", server_port=8080, client_name="client1")
    for command in commands:
        client.perform_action(
            action=command["action"],
            file_name=command.get("file_name"),
            file_path=command.get("file_path"),
            append_file_path=command.get("append_file_path")
        )
