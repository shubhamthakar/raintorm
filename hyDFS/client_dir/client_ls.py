import socket
import json
import msgpack

# List of server hostnames and ports
servers = [(f'fa24-cs425-69{i:02d}.cs.illinois.edu', 5001) for i in range(1, 11)]

def check_file_on_server(hostname, port, filename):
    try:
        # Establish a TCP connection
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((hostname, port))
            
            # Prepare the request data
            message = {"action": "ls", "filename": filename}
            request_data = msgpack.packb(message) + b"<EOF>"
            
            # Send the request
            s.sendall(request_data)
            
            # Receive the response
            # print(hostname)
            response = b""
            while True:
                chunk = s.recv(1024)
                if b"<EOF>" in chunk:
                    response += chunk.replace(b"<EOF>", b"")
                    break
                response += chunk
            response = msgpack.unpackb(response)
            
            # Check if the file is present
            if response.get("is_present", False):
                print(f"File is present on server: {hostname}")
    except Exception as e:
        print(f"Error connecting to {hostname}:{port} - {e}")


if __name__ == "__main__":
    import sys

    filename = sys.argv[1]
    # Loop through each server and check for the file
    for hostname, port in servers:
        check_file_on_server(hostname, port, filename)
