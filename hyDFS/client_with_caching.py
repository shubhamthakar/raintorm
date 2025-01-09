import socket
import os
import msgpack

class FileClient:
    cache = {}

    def __init__(self, server_ip, server_port, client_name, action, file_name=None, file_path=None):
        self.server_ip = server_ip
        self.server_port = server_port
        self.client_name = client_name
        self.action = action
        self.file_name = file_name
        self.file_path = file_path
        self.client_socket = None

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
        if self.action == "get" and (self.client_name, self.file_name) in FileClient.cache:
            print("File found in cache.")
            return None  # Return None if file is cached and valid

        if self.action == "create" and (self.client_name, self.file_name) in FileClient.cache:
            print("File already exists in filesystem, try appending instead.")
            return None

        message = {
            "client_name": self.client_name,
            "action": self.action,
            "filename": self.file_name
        }

        if (self.action == "create" or self.action == "append") and self.file_path:
            if os.path.exists(self.file_path):
                message["filesize"] = os.path.getsize(self.file_path)
                with open(self.file_path, 'rb') as file:
                    message["content"] = file.read()
            else:
                print(f"Error: File at {self.file_path} does not exist.")
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

    def cache_response(self, response):
        """Updates the cache with the response if action is 'get'."""
        if self.action == "get":
            FileClient.cache[(self.client_name, self.file_name)] = response

    def invalidate_cache(self):
        """Invalidates cache for the file if action is 'append' or 'merge'."""
        if self.action in ["append", "merge"]:
            FileClient.cache.pop((self.client_name, self.file_name), None)

    def perform_action(self):
        """Main function to perform the action."""
        self.connect()
        if self.client_socket:
            message = self.prepare_message()
            if message:
                self.send_message(message)
                response = self.receive_response()
                if response:
                    print("Server response:", response)
                    self.cache_response(response)  # Update cache on successful get response
            self.invalidate_cache()  # Invalidate cache if action is append or merge
            self.close()


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 7:
        print("Usage: python client.py <server_ip> <server_port> <client_name> <action> <file_name> <file_path>")
        sys.exit(1)

    server_ip = sys.argv[1]
    server_port = int(sys.argv[2])
    client_name = sys.argv[3]
    action = sys.argv[4]
    file_name = sys.argv[5]
    file_path = sys.argv[6] if len(sys.argv) > 6 else None

    client = FileClient(server_ip, server_port, client_name, action, file_name, file_path)
    client.perform_action()
