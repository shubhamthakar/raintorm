# How to assign nodes 
import socket
import hashlib
from ..SWIM.process import Process

class RingNode:
    def __init__(self):
        """
        Initialize the RingNode with a ring_id based on the host_name and pass it along with other params to Process.
        """
        self.host_name = socket.gethostname()
        md5_hash = hashlib.md5(self.host_name.encode('utf-8')).hexdigest()
        self.ring_id = int(md5_hash, 16) % (2**10)
        self.process = Process(socket.gethostname(), 5000, 'fa24-cs425-6901.cs.illinois.edu', 5000, False, 20, 10, 0, self.ring_id)
        self.send_join_request()


    def send_join_request(self):
        self.process.send_join_request()

if __name__ == '__main__':
    ring_node = RingNode()