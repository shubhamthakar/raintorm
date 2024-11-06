import socket
import os
import msgpack

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

        # For "create" action, add file size and content if file_path is provided
        if (self.action == "create" ) and self.file_path:
            if os.path.exists(self.file_path):
                message["filesize"] = os.path.getsize(self.file_path)
                with open(self.file_path, 'rb') as file:
                    message["content"] = file.read()
            else:
                print(f"Error: File at {self.file_path} does not exist.")
                return None
        elif (self.action == "append") and self.append_file_path:
            if os.path.exists(self.file_path):
                message["filesize"] = os.path.getsize(self.append_file_path)
                with open(self.append_file_path, 'rb') as file:
                    message["content"] = file.read()
            else:
                print(f"Error: File at {self.file_path} does not exist.")
                return None

        elif self.action == "get":
        # No additional data needed for "get" action
            pass
        
        elif self.action == "merge":
            pass

        elif self.action == "store":



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
        self.connect()
        if self.client_socket:
            message = self.prepare_message()
            if message:
                self.send_message(message)
                response = self.receive_response()
                if response:
                    print("Server response:", response)
                    if response["status"] == 'read_complete':
                        with open(response["filename"], 'w') as file:
                            file.write(response["content"])
            self.close()


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 7:
        print("Usage: python client.py <server_ip> <server_port> <client_name> <action> <file_name> <file_path> <append_file_name>")
        sys.exit(1)

    # Command-line arguments
    server_ip = sys.argv[1]
    server_port = int(sys.argv[2])
    client_name = sys.argv[3]
    action = sys.argv[4]
    file_name = sys.argv[5] if len(sys.argv) > 5 else None
    file_path = sys.argv[6] if len(sys.argv) > 6 else None
    append_file_name = sys.argv[7] if len(sys.argv) > 7 else None
    append_file_path = sys.argv[8] if len(sys.argv) > 8 else None


    # Initialize and perform action
    client = FileClient(server_ip, server_port, client_name, action, file_name, file_path, append_file_name, append_file_path)
    client.perform_action()
