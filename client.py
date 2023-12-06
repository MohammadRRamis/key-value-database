import socket
import pickle

while True:
    print("Available actions: create, read, update, delete, exit")
    action = input("Enter action: ")

    if action == 'exit':
        break

    if action in ['create', 'read', 'update', 'delete']:
        key = input("Enter key: ")

        if action == 'create' or action == 'update':
            value = input("Enter value: ")

        # Create a socket object
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Define the port on which you want to connect
        port = 12345

        # Connect to the server on local computer
        s.connect(('127.0.0.1', port))

        # Send the request to the server
        request = {'key': key, 'action': action, 'value': value}
        s.send(pickle.dumps(request))

        if action == 'read':
            # Receive the result from the server
            result = pickle.loads(s.recv(1024))
            print(result)

        # Close the connection
        s.close()
    else:
        print("Invalid action. Please try again.")
