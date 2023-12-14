import socket
import time

# Node information
nodes = {
    'node4': {'id': 4, 'hostname': 'localhost', 'port': 8003},
    'node3': {'id': 3, 'hostname': 'localhost', 'port': 8002},
    'node2': {'id': 2, 'hostname': 'localhost', 'port': 8001},
    'node1': {'id': 1, 'hostname': 'localhost', 'port': 8000},
}

# Function to try connecting to a node
def try_connect(sock, node_info):
    try:
        sock.connect((node_info['hostname'], node_info['port']))
        return True
    except socket.error as e:
        print(f"Connection error: {e}")
        return False

# Function to request coordinator information
def request_coordinator_info(sock):
    sock.sendall("CLIENT_REQUEST_COORDINATOR_INFO".encode())
    try:
        response = sock.recv(1024).decode()
        return response
    except Exception as e:
        print(f"Error receiving coordinator information: {e}")
        return None
# Command processing functions
def create(sock, args):
    message = f"CREATE {args}"
    sock.sendall(message.encode())

def read(sock, args):
    message = f"READ {args}"
    sock.sendall(message.encode())

def update(sock, args):
    message = f"UPDATE {args}"
    sock.sendall(message.encode())

def delete(sock, args):
    message = f"DELETE {args}"
    sock.sendall(message.encode())


def process_command(sock, command, args):
    if command == "CREATE":
        create(sock, args)
    elif command == "READ":
        read(sock, args)
    elif command == "UPDATE":
        update(sock, args)
    elif command == "DELETE":
        delete(sock, args)
    else:
        print("Invalid command.")
        return
    
    # Wait for response from the server
    response = sock.recv(1024).decode()
    print("Response:", response)

    if (response == None or response == ''):
        print("Misson Failed, Try Again")

def main():
    node_order = ['node4', 'node3', 'node2', 'node1']
    node_index = 0

    while True:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(7)
                node = node_order[node_index]
                if try_connect(sock, nodes[node]):
                    print(f"Connected to {node}. Requesting coordinator information...")
                    coordinator_response = request_coordinator_info(sock)

                    if coordinator_response:
                        coordinator_id = coordinator_response.split()[-1]
                        if coordinator_id != f"node{nodes[node]['id']}":
                            print(f"Redirected to coordinator at {nodes[coordinator_id]['hostname']}:{nodes[coordinator_id]['port']}")
                            node_index = node_order.index(coordinator_id)
                            continue
                        else:
                            print("This node is the coordinator. Proceeding with CRUD operations.")

                            while True:
                                user_input = input("Enter command (CREATE, READ, UPDATE, DELETE) and arguments, or 'exit' to quit: ")
                                if user_input.lower() == 'exit':
                                    return

                                parts = user_input.split(maxsplit=1)
                                if len(parts) == 2:
                                    command, args = parts
                                    process_command(sock, command.upper(), args)
                                else:
                                    print("Invalid input format.")
                    else:
                        # print("Coordinator information not received. Retrying...")
                        node_index = (node_index + 1) % len(node_order)
                else:
                    print(f"Failed to connect to {node}.")
                    node_index = (node_index + 1) % len(node_order)
                time.sleep(5)
        except socket.error as e:
            print(f"Connection lost or error occurred: {e}")
            print("Attempting to reconnect...")
            node_index = (node_index + 1) % len(node_order)
        finally:
            if sock:
                sock.close()
if __name__ == "__main__":
    main()