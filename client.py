from distributedStoreClient import DistributedStoreClient
from hashRing import ring
import time



# we need to ensure the client is aware of the node information 
# and that the HashRing is utilized correctly for distributing keys across the nodes

# Initialize the client with node information
# client = DistributedStoreClient()
hr = ring()

while True:
    print("Available actions: create, read, update, delete, exit")
    action = input("Enter action: ")

    if action == 'exit':
        break
    
    if action in ['create', 'read', 'update', 'delete']:
        print(hr['name'].get('name'))

        key = input("Enter key: ")

        if action == 'create' or action == 'update':
            value = input("Enter value: ")
            if action == 'create':
                hr[key].set(key,value)
  
                
        #     elif action == 'update':
        #         client.update(key, value)
        # elif action == 'read':
        #     result = client.read(key)
        #     print(result)
        # elif action == 'delete':
        #     client.delete(key)
    else:
        print("Invalid action. Please try again.")
