import threading
from nodeServer import NodeServer




"""
initializes and starts each node server for the distributed key-value store. 
Each node server runs on a specified host and port 
and utilizes a specified file for data storage. 
"""
def start_node(host, port, data_file):
    node = NodeServer(host, port, data_file)
    node.start()

# Define your nodes
nodes = [
    ('localhost', 5001, 'node1_data.json'),
    ('localhost', 6002, 'node2_data.json'),
    # Add other nodes here
]

# Start each node in a separate thread
for host, port, data_file in nodes:
    node_thread = threading.Thread(target=start_node, args=(host, port, data_file))
    node_thread.start()
