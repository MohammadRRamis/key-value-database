from distributedStoreClient import DistributedStoreClient


# we need to ensure the client is aware of the node information 
# and that the HashRing is utilized correctly for distributing keys across the nodes

# Initialize the client with node information
client = DistributedStoreClient()

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
