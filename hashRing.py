from uhashring import HashRing
import socket
import json
import threading
file_lock = threading.Lock()




def hashRing(nodes):
    hr = HashRing(nodes)
    return hr


def get_previous_node(nodes, current_node_name):
    nodenames = sorted(nodes)  # Get sorted list of node names

    # Find the index of the current node
    current_index = nodenames.index(current_node_name)

    # Calculate the indices of the previous two nodes
    # The '% len(nodenames)' ensures that the index wraps around the list
    prev_index1 = (current_index - 1) % len(nodenames)

    # Return the names of the previou node
    return nodenames[prev_index1]

def get_next_node(nodes, current_node_name):
    nodenames = sorted(nodes)  # Get sorted list of node names
    # Ensure current node is in the list of nodes
    if current_node_name not in nodes:
        raise ValueError(f"Node {current_node_name} not found in the list of nodes")

    # Find the index of the current node
    current_index = nodenames.index(current_node_name)

    # Calculate the index of the next node, wrapping around if necessary
    next_index = (current_index + 1) % len(nodenames)

    # Return the name of the next node
    return nodenames[next_index]



import json

def read_json_file(nodename):
    """
    Reads a JSON file and returns the data as a Python dictionary.

    :param nodename
    :return: A dictionary containing the key-value pairs from the JSON file.
    """
    try:
        with open(f'{nodename}.json', 'r') as file:
            data = json.load(file)
            return data
    except Exception as e:
        print(e)

    
def save_data(nodename, key, value):
    with file_lock:
        filename = f'{nodename}.json'

        # Load existing data from the file, if it exists
        try:
            with open(filename, 'r') as file:
                data = json.load(file)
        except (FileNotFoundError, json.JSONDecodeError):
            data = {}

        # Update the data with the new key-value pair
        if key in data:
            return "FAILED-Duplicated-Key"
        data[key] = value

        # Write the updated data back to the file
        with open(filename, 'w') as file:
            json.dump(data, file, indent=4)
        return f"SUCCESS-({key}:{value})-added"
    
def delete_data(nodename, key):
    with file_lock:
        filename = f'{nodename}.json'

        try:
            with open(filename, 'r') as file:
                data = json.load(file)

            if key in data:
                del data[key]

                with open(filename, 'w') as file:
                    json.dump(data, file, indent=4)
                return f"SUCCESS-{key}-deleted"

            else:
                return f"NOT-FOUND"

        except FileNotFoundError:
            return f"NOT-FOUND"
        except json.JSONDecodeError:
            return f"ERROR-Invalid-Data"


def update_data(nodename, key, new_value):
    with file_lock:  # Assuming file_lock is a threading.Lock() instance
        filename = f'{nodename}.json'

        # Load existing data from the file, if it exists
        try:
            with open(filename, 'r') as file:
                data = json.load(file)
        except (FileNotFoundError, json.JSONDecodeError):
            return "ERROR-File-NotFound-or-Corrupt"

        # Check if the key exists in the data, and update its value
        if key in data:
            data[key] = new_value
            with open(filename, 'w') as file:
                json.dump(data, file, indent=4)
            return f"SUCCESS-{key}-updated"
        else:
            return "ERROR-Key-Not-Found"



def load_node_data(nodename, key):
    filename = f'{nodename}.json'
    try:
        with open(filename, 'r') as file:
            data = json.load(file)
            # Extract the value for the specific key
            if key in data:
                return data[key]
            else:
                return f"NOT-FOUND"
    except FileNotFoundError:
        return f"NOT-FOUND"
    

def create(nodename, key, value):
    response = save_data(nodename, key, value)
    return response


def read(nodename, key):
    response = load_node_data(nodename, key)
    return response

def delete(nodename, key):
    response = delete_data(nodename, key)
    return response

def update(nodename, key, value):
    response = update_data(nodename, key, value)
    return response
    
def get_target_node_id(key, hash_ring):
    return hash_ring.get_node(key)


def add_node(hash_ring, nodes, nodename):
    # Step 1: Add the node back to the hash ring
    hash_ring.add_node(nodename)
    

    # Step 2: Identify the source node for data replication
    predecessor = get_previous_node(nodes, nodename)
    if not is_node_alive(predecessor, nodes):
        predecessor = get_previous_node(hash_ring, predecessor)

    # Step 3: Identify the next node for data replication
    successor = get_next_node(nodes, nodename)
    if not is_node_alive(successor, nodes):
        successor = get_next_node(hash_ring, predecessor)

    # Step 4: Handle data transfer from the source node to the newly added node
    if is_node_alive(predecessor, nodes):
        request_replicated_data(predecessor, nodename, nodes)

    if is_node_alive(successor, nodes):
        request_replicated_data(successor, nodename, nodes)


#The source node is the node that currently holds the data that needs to be replicated or transferred.
#connect to the node that has the replicated data
def request_replicated_data(source_node_name, target_node_name, nodes):
    source_host = nodes[source_node_name]['hostname']
    source_port = nodes[source_node_name]['port']
    
    try:
        # Connect to the source node to retrieve relevant data
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as source_socket:
            source_socket.connect((source_host, source_port))

            # Request only the keys that belong to the target node's hash range
            message = f"GET_REPLICATED_DATA {target_node_name}"
            source_socket.sendall(message.encode())
    except Exception as e:
        print(f"Error during data transfer: {e}")


def is_node_alive(nodename, nodes):
    return nodes[nodename]['isAlive']










