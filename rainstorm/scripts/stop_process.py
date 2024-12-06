import socket
import os
import msgpack
import time
import subprocess

class FileClient:
    def __init__(self):
        self.client_socket = None

    def connect(self, server):
        """Establishes a connection to the server."""
        try:
            hostname, port = server
            self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.client_socket.connect((hostname, port))
            # self.client_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            print("Connected to the server.")
        except Exception as e:
            print(f"Connection error: {e}")
            self.close()

    def prepare_message(self):
        """Prepares the message based on the action."""
        message = {
            "client_name": "stop_process_client",
            "action": "stop_process",
        }

        return msgpack.packb(message) + b"<EOF>"

    def send_message(self, message):
        """Sends the prepared message to the server."""
        try:
            self.client_socket.sendall(message)
            print(f"Message for action stop_process sent successfully.")
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

    def perform_action(self, server):
        """Main function to perform the action with overall time measurement."""

        self.connect(server)
        if self.client_socket:
            message = self.prepare_message()
            if message:
                self.send_message(message)
                # response = self.receive_response()
                self.close()

    def run_task_cleanup_script(self, script_path, *args):
        """
        Runs the specified shell script with the given arguments.
        Args:
            script_path (str): The path to the shell script.
            *args: The arguments to pass to the script.
        """
        try:
            # Build the command with the script and its arguments
            command = [script_path] + list(args)
            
            # Execute the shell script
            result = subprocess.run(command, text=True, capture_output=True, check=True)
            
            # Print the script's output
            print("Script Output:")
            print(result.stdout)

            # Print any error output
            if result.stderr:
                print("Script Errors:")
                print(result.stderr)

        except subprocess.CalledProcessError as e:
            print(f"Error occurred while executing the script: {e}")
            print("Output:", e.output)
            print("Error Output:", e.stderr)


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python client.py <node 1> <node 2>")
        sys.exit(1)

    # Command-line arguments
    node_1 = sys.argv[1]
    node_2 = sys.argv[2]
    servers = [(f"fa24-cs425-69{node_1}.cs.illinois.edu", 5001), (f"fa24-cs425-69{node_2}.cs.illinois.edu", 5001)]


    # Initialize and perform action
    client = FileClient()
    for server in servers:
        # client.perform_action(server)
        client.run_task_cleanup_script('./task_cleanup_specify_nodes.sh', node_1, node_2)