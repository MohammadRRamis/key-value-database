from distributed_store_client import DistributedStoreClient

# Define node information (host, port)
node_info = {
    'node1': ('localhost', 5001),
    'node2': ('localhost', 6002),
}

# Initialize the client with node information
client = DistributedStoreClient(node_info)

while True:
    print("Available actions: create, read, update, delete, exit")
    action = input("Enter action: ")

    if action == 'exit':
        break
    
    if action in ['create', 'read', 'update', 'delete']:

        key = input("Enter key: ")

        if action == 'create' or action == 'update':
            value = input("Enter value: ")
            if action == 'create':
                client.create(key, value)
            elif action == 'update':
                client.update(key, value)
        elif action == 'read':
            result = client.read(key)
            print(result)
        elif action == 'delete':
            client.delete(key)
    else:
        print("Invalid action. Please try again.")
