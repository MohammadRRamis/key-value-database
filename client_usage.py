from distributed_store_client import DistributedStoreClient

# Define node information (host, port)
node_info = {
    'node1': ('localhost', 5001),
    'node2': ('localhost', 6002),
}

# Initialize the client with node information
client = DistributedStoreClient(node_info)

# Using the client to put and get data
client.put('key5', 'value1')
print(client.get('key5'))  # Should return 'value1'
