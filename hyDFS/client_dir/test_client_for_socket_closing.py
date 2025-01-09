import socket
import msgpack

def simple_client(server_host, server_port):
    try:
        # Create a socket object
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
        # Connect to the server
        client_socket.connect((server_host, server_port))
        print(f"Connected to {server_host}:{server_port}")

        # Send some data
        message = {
            "client_name": "",
            "action": "",
            "filename": ""
        }
        client_socket.sendall(msgpack.packb(message) + b"<EOF>")
        print(f"Sent: {message}")

        # Close the connection
        client_socket.close()
        print("Connection closed gracefully.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == '__main__':
    # Replace with your server's hostname and port
    SERVER_HOST = 'fa24-cs425-6901.cs.illinois.edu'  # Change to the server's hostname or IP
    SERVER_PORT = 5001         # Change to the server's listening port
    simple_client(SERVER_HOST, SERVER_PORT)
