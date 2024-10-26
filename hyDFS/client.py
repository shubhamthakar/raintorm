import socket
import os
import json
import msgpack

def create_file(server_ip, server_port, file_path):
    try:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((server_ip, server_port))
        client_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        # Send a header with file transfer and the filename followed by a delimiter
        filename = os.path.basename(file_path)
        # Create a dictionary with filename, filesize, and file content
        file_info = {
            "action": "create_file",
            "filename": filename,
            "filesize": os.path.getsize(file_path),
            "content": None
        }

        # Read the file content
        with open(file_path, 'rb') as file:
            file_info["content"] = file.read()

        # Send the dictionary as a serialized string
        serialized_info = msgpack.packb(file_info) + b"<EOF>"
        client_socket.sendall(serialized_info)

        print(f"File {filename} sent successfully.")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        client_socket.close()

if __name__ == "__main__":
    create_file("fa24-cs425-6901.cs.illinois.edu", 5001, "example_3.txt")
