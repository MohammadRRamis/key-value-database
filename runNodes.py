import threading
from nodeServer import NodeServer
from uhashring import HashRing
from hashRing import ring, nodesInfo


"""
initializes and starts each node server for the distributed key-value store. 
Each node server runs on a specified host and port 
and utilizes a specified file for data storage. 
"""
def start_node(host, port, data_file):
    node = NodeServer(host, port, data_file)
    node.start()

# # Define your nodes
# nodes = [
#     ('localhost', 5002, 'node1_data.json'),
#     ('localhost', 6005, 'node2_data.json'),
#     ('localhost', 6004, 'node3_data.json'),
#     # Add other nodes here
# ]

nodes = nodesInfo()

# # Start each node in a separate thread
# for host, port, data_file in nodes:
#     node_thread = threading.Thread(target=start_node, args=(host, port, data_file))
#     node_thread.start()

for node_name, node_info in nodes.items():
    host = node_info['host']
    data_file = node_info['data_file']
    port = node_info['port']

    node_thread = threading.Thread(target=start_node, args=(host, port, data_file))
    node_thread.start()
