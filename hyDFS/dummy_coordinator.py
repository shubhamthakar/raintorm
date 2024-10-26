import socket
import select
import os
import msgpack

# Directory where received files are stored
RECEIVE_DIRECTORY = 'received_files'
os.makedirs(RECEIVE_DIRECTORY, exist_ok=True)

# Server configuration
SERVER_HOST = 'fa24-cs425-6901.cs.illinois.edu'  # Listen on all network interfaces
SERVER_PORT = 65433  # Same port as specified in the client code
BUFFER_SIZE = 1024

def start_server():
    # Initialize and configure server socket
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((SERVER_HOST, SERVER_PORT))
    server_socket.listen()
    server_socket.setblocking(False)
    print(f"Server listening on {SERVER_HOST}:{SERVER_PORT}")

    # Lists to manage active sockets and data from clients
    inputs = [server_socket]
    outputs = []
    data_buffer = {}

    try:
        while True:
            readable, _, exceptional = select.select(inputs, outputs, inputs)

            # Handle readable sockets
            for s in readable:
                if s is server_socket:
                    # Accept new client connection
                    client_socket, client_address = server_socket.accept()
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

                            try:
                                # Deserialize with msgpack
                                file_info = msgpack.unpackb(complete_data)
                                filename = file_info['filename']
                                file_content = file_info['content']

                                # Save the file to the server's local storage
                                file_path = os.path.join("received_files", filename)
                                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                                with open(file_path, 'wb') as f:
                                    f.write(file_content)

                                print(f"File '{filename}' received and saved successfully.")

                            except msgpack.UnpackException:
                                print("Error: Could not unpack data from client.")

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

    except KeyboardInterrupt:
        print("\nServer shutting down...")

    finally:
        server_socket.close()

if __name__ == "__main__":
    start_server()
