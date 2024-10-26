import socket
import os
import msgpack

def send_file(filename, node_ip, node_port):
    """Send a file upload request to a node."""
    if not os.path.isfile(filename):
        print(f"File '{filename}' does not exist.")
        return

    # Prepare the message
    message = {
        'type': 'file_upload',
        'filename': os.path.basename(filename),
        'filesize': os.path.getsize(filename)
    }

    # Create a UDP socket
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        try:
            # Send the message
            sock.sendto(msgpack.packb(message), (node_ip, node_port))
            print(f"Sent file upload message for '{filename}' to {node_ip}:{node_port}")
        except Exception as e:
            print(f"Failed to send message: {e}")

if __name__ == "__main__":
    # Replace these with your actual values
    filename_to_send = 'example.txt'  # The file you want to send
    target_node_ip = 'fa24-cs425-6901.cs.illinois.edu'  # The IP address of the target node
    target_node_port = 5000  # The port the node is listening on

    send_file(filename_to_send, target_node_ip, target_node_port)
