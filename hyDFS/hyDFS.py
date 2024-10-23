# How to assign nodes 
import socket
import hashlib
from process import Process

class RingNode:
    def __init__(self):
        """
        Initialize the RingNode with a ring_id based on the host_name and pass it along with other params to Process.
        """
        self.host_name = socket.gethostname()
        self.ring_id = self.hash_string(self.host_name)
        self.process = Process(socket.gethostname(), 5000, 'fa24-cs425-6901.cs.illinois.edu', 5000, False, 20, 10, 0, self.ring_id)
        self.send_join_request()


    def send_join_request(self):
        self.process.send_join_request()
    

    def hash_string(self, content):
        md5_hash = hashlib.md5(content.encode('utf-8')).hexdigest()    
        hash_int = int(md5_hash, 16)
        
        return hash_int % (2**10)

    def create_file(filename):

        primary_node_id = self.hash_string(filename)
        next_2_nodes = get_next_2_nodes(primary_node_id, 2)


        
        
    def get_next_n_nodes(node_id, n):
        live_nodes = [node for node in self.process.membership_list if node['status'] == 'LIVE']
        
        sorted_nodes = sorted(live_nodes, key=lambda x: x['ring_id'])
        
        current_index = next((i for i, node in enumerate(sorted_nodes) if node['node_id'] == node_id), None)
        
        if current_index is None:
            raise ValueError(f"Node {node_id} not found in the membership list.")
        
        next_nodes = []
        for i in range(1, n+1):
            next_index = (current_index + i) % len(sorted_nodes)
            next_nodes.append({
                'node_id': sorted_nodes[next_index]['node_id'],
                'ring_id': sorted_nodes[next_index]['ring_id']
            })
        
        return next_nodes


    

if __name__ == '__main__':
    ring_node = RingNode()
