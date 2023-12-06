from uhashring import HashRing
import threading
import redis
import time
import socket
import pickle


def start_election(nodename):
    """
    Start an election from the given node.
    """
    # Get all nodes with higher IDs
    higher_nodes = [node for node in nodes if node > nodename]

    if higher_nodes:
        # Send election message to all nodes with higher IDs
        for node in higher_nodes:
            send_election_message(node)
    else:
        # If there are no nodes with higher IDs, this node becomes the coordinator
        become_coordinator(nodename)


def send_election_message(nodename):
    """
    Send an election message to the given node.
    """
    # If the node responds, it will start its own election
    if nodes[nodename]["status"] == "active":
        start_election(nodename)
    else:
        # If the node does not respond, remove it from the election
        nodes[nodename]["status"] = "inactive"


def become_coordinator(nodename):
    """
    Become the coordinator and notify all other nodes.
    """
    # Set the current node as the coordinator
    nodes[nodename]["status"] = "coordinator"

    # Notify all other nodes
    for node in nodes:
        if node != nodename:
            send_coordinator_message(node)


def send_coordinator_message(nodename):
    """
    Send a coordinator message to the given node.
    """
    # If the node is active, update its coordinator
    if nodes[nodename]["status"] == "active":
        nodes[nodename]["coordinator"] = nodename


nodes = {
    "node1": {
        "hostname": "localhost",
        "instance": redis.StrictRedis(
            host="localhost", port=6379
        ),  # replica on port 6370
        "vnodes": 40,
        "port": 6379,
        "status": "active",
        "replica": redis.StrictRedis(host="localhost", port=6370),
        "coordinator": "node2"
    },
    "node2": {
        "hostname": "localhost",
        "instance": redis.StrictRedis(host="localhost", port=6378),
        "vnodes": 40,
        "port": 6378,
        "status": "active",
        'replica': redis.StrictRedis(host='localhost', port=6371),
        "coordinator": "node2"
    },
}


hr = HashRing(nodes)


def ring():
    return hr


def nodesInfo():
    return nodes


"""
In case of a failure, the keys-values in the failed nodes needs to be 
distributed to other nodes in the ring using the "replication" of the failed node
"""


def removeNode(nodename):
    # Create a temporary HashRing without the node being removed
    temp_nodes = nodes.copy()
    del temp_nodes[nodename]
    temp_hr = HashRing(temp_nodes)

    try:
        replica = nodes[nodename].get("replica")
        keys = replica.keys()
        for key in keys:
            value = replica.get(key)
            new_node_name = temp_hr.get_node(key)
            if new_node_name != nodename:
                nodes[new_node_name].get("instance").set(key, value)
    except AttributeError:
        print("their are not keys in this node")

    nodes[nodename]["status"] = "inactive"
    hr.remove_node(nodename)

    print(f"Node {nodename} removed successfully.")


def addNode(nodename):
    if nodename in nodes and nodes[nodename]["status"] == "inactive":
        nodes[nodename]["status"] = "active"
        hr.add_node(nodename)

    # temp_nodes = nodes.copy()
    # add temp_nodes[nodename]
    # temp_hr = HashRing(temp_nodes)

    # Identify the node which might have keys to be redistributed to the new node
    # For simplicity, we're just checking the next node in the ring
    # In a more complex setup, you might need a more sophisticated method to identify this node
    # Determine the next node in the ring
    next_node = getNodeAfter(hr, nodename)
    if next_node:
        addNodeRedistributeData(next_node, nodename, hr)

    print(f"{nodename} added and data redistributed successfully.")


def addNodeRedistributeData(source_node, target_node, hr):
    """
    Move data from the source node to the target node based on the updated hash ring.
    """
    source_instance = nodes[source_node]["instance"]
    keys = source_instance.keys()

    for key in keys:
        # Check if the key should be moved to the target node
        correct_node = hr.get_node(key)
        if correct_node == target_node:
            value = source_instance.get(key)
            nodes[target_node].get("instance").set(key, value)
            # nodes[target_node]['instance'].set(key, value)
            source_instance.delete(key)


def getNodeAfter(hr, nodename):
    """
    Get the next node in the hash ring after the specified node.
    """
    sorted_nodes = sorted(hr.get_nodes())
    current_index = sorted_nodes.index(nodename)

    # the mod (%) is used to "wrap around" if the index goes beyond the length of the list.
    next_index = (current_index + 1) % len(sorted_nodes)
    return sorted_nodes[next_index]


# Regular Health Checks: Periodically ping each node to check its status.
# Handling Failures: If a node fails to respond, consider it as unavailable
# and proceed with failure handling, remove it from the HashRing and redistributing its data.
def check_node_health(node_instance, nodename):
    try:
        if node_instance.ping():
            if nodes[nodename]["status"] == "inactive":
                print(f"{nodename} is back online. Re-adding to HashRing.")
                addNode(nodename)
            return True
    except redis.exceptions.ConnectionError:
        if nodes[nodename]["status"] == "active":
            print(f"{nodename} is down. Marking as inactive.")
            removeNode(nodename)
            start_election(nodename)
        return False


def periodic_health_check(interval=5):
    while True:
        for nodename in list(nodes.keys()):
            node_info = nodes.get(nodename)
            if node_info:
                check_node_health(node_info["instance"], nodename)

        time.sleep(interval)


# def readReplica(nodename):
#     instance = nodes[nodename].get("instance")
#     # In case of failure, switch to a replica
#     try:
#         keys = instance.keys()
#     except ConnectionError:
#         # Handle the failure - switch to replica
#         replica = nodes[nodename].get("replica")
#         if replica:
#             print(f"Primary node {nodename} is down. Switching to replica.")
#             instance = replica
#             keys = instance.keys()
#             return keys, instance
#         else:
#             print(f"No replica available for node {nodename}.")
#             return


# Start the periodic health check in a separate thread
health_check_thread = threading.Thread(target=periodic_health_check, args=(30,))
health_check_thread.start()


# Create a socket object
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Define the port on which you want to connect
port = 12345

# Bind to the port
s.bind(('127.0.0.1', port))

# Put the socket into listening mode
s.listen(5)

while True:
    # Establish a connection with the client
    c, addr = s.accept()

    # Receive the client's request
    request = pickle.loads(c.recv(1024))

    # Process the request
    hr = ring()
    key = request['key']
    action = request['action']
    value = request.get('value')  # 'value' might not be in the request
    if action == 'create':
        hr[key].set(key, value)
    elif action == 'read':
        result = hr[key].get(key)
        c.send(pickle.dumps(result))

    # Close the connection with the client
    c.close()
