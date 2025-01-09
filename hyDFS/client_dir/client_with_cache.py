import socket
import os
import msgpack
import json

def invalidate_entry_in_all_caches(file_name):
    """Invalidates the specific file entry from all client caches."""
    for cache_file in os.listdir('./cache'):
        if cache_file.endswith("_cache.json"):
            file_path = f'./cache/{cache_file}'
            with open(file_path, 'r') as file:
                try:
                    cache_data = json.load(file)
                except json.JSONDecodeError:
                    cache_data = {}
            
            if file_name in cache_data:
                del cache_data[file_name]
                with open(file_path, 'w') as file:
                    json.dump(cache_data, file)

class LRUCache:

    def __init__(self, capacity: int, client_name: str):
        self.dic = {}
        self.capacity = capacity
        self.client_name = client_name
        self.cache_file = f"./cache/{client_name}_cache.json"
        self.load_cache()

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
        self.save_cache()

    def invalidate(self, key: int) -> None:
        if key in self.dic:
            del self.dic[key]
            self.save_cache()

    def save_cache(self):
        with open(self.cache_file, 'w') as cache_file:
            json.dump(self.dic, cache_file)

    def load_cache(self):
        if os.path.exists(self.cache_file):
            with open(self.cache_file, 'r') as cache_file:
                self.dic = json.load(cache_file)

class FileClient:
    def __init__(self, server_ip, server_port, client_name, action, file_name=None, file_path=None, append_file_name=None, append_file_path=None):
        self.server_ip = server_ip
        self.server_port = server_port
        self.client_name = client_name
        self.action = action
        self.file_name = file_name
        self.file_path = file_path
        self.append_file_name = append_file_name
        self.append_file_path = append_file_path
        self.client_socket = None
        self.cache = LRUCache(capacity=5, client_name=client_name)
        
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

    def prepare_message(self):
        """Prepares the message based on the action."""
        message = {
            "client_name": self.client_name,
            "action": self.action,
            "filename": self.file_name
        }

        if self.action == "create" and self.file_path:
            if os.path.exists(self.file_path):
                message["filesize"] = os.path.getsize(self.file_path)
                with open(self.file_path, 'rb') as file:
                    message["content"] = file.read()
            else:
                print(f"Error: File at {self.file_path} does not exist.")
                return None
        elif self.action == "append" and self.append_file_path:
            if os.path.exists(self.append_file_path):
                message["filesize"] = os.path.getsize(self.append_file_path)
                with open(self.append_file_path, 'rb') as file:
                    message["content"] = file.read()
            else:
                print(f"Error: File at {self.append_file_path} does not exist.")
                return None

        return msgpack.packb(message) + b"<EOF>"

    def send_message(self, message):
        """Sends the prepared message to the server."""
        try:
            self.client_socket.sendall(message)
            print(f"Message for action '{self.action}' sent successfully.")
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

    def perform_action(self):
        """Main function to perform the action."""
        if self.action == "get":
            cached_content = self.cache.get(self.file_name)
            if cached_content != -1:
                print(f"Cache hit: {self.file_name}")
                print("Cached content:", cached_content)
                return
            else:
                print(f"Cache miss: {self.file_name}")
        
        self.connect()
        if self.client_socket:
            message = self.prepare_message()
            if message:
                self.send_message(message)
                response = self.receive_response()
                if response:
                    print("Server response:", response)
                    if self.action == "get" and response.get("status") == 'read_complete':
                        content = response.get("content")
                        self.cache.put(self.file_name, content)
                        with open(response["filename"], 'w') as file:
                            file.write(content)
                    elif self.action == "append":
                        self.cache.invalidate(self.file_name)
                    elif self.action == "merge":
                        invalidate_entry_in_all_caches(self.file_name)
            self.close()

if __name__ == "__main__":
    import sys

    if len(sys.argv) < 5:
        print("Usage: python client.py <server_ip> <server_port> <client_name> <action> [<file_name>] [<file_path>] [<append_file_path>]")
        sys.exit(1)

    # Command-line arguments
    server_ip = sys.argv[1]
    server_port = int(sys.argv[2])
    client_name = sys.argv[3]
    action = sys.argv[4]
    file_name = sys.argv[5] if len(sys.argv) > 5 else None
    file_path = sys.argv[6] if len(sys.argv) > 6 else None
    append_file_path = sys.argv[7] if len(sys.argv) > 7 else None

    # Initialize and perform action
    client = FileClient(server_ip, server_port, client_name, action, file_name, file_path, append_file_path=append_file_path)
    client.perform_action()

