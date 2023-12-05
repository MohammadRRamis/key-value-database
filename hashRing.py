from uhashring import HashRing
import threading
import redis
import time


nodes = {
    'node1': {
        'hostname': 'localhost',
        'instance': redis.StrictRedis(host='localhost', port=6379),
        'vnodes': 40,
        'port': 6379,
        'status': 'active',
    },
    'node2': {
        'hostname': 'localhost',
        'instance': redis.StrictRedis(host='localhost', port=6378),
        'vnodes': 40,
        'port': 6378,
        'status': 'active',
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

    instance = nodes[nodename].get('instance')
    keys = instance.keys()
    for key in keys:
        value = instance.get(key)
        new_node_name = temp_hr.get_node(key)
        if new_node_name != nodename:
            nodes[new_node_name].get('instance').set(key,value)

    nodes[nodename]['status'] = 'inactive'
    hr.remove_node(nodename)
    # del nodes[nodename]

    print(f"Node {nodename} removed successfully.")

def addNode(nodename):
    pass


# Regular Health Checks: Periodically ping each node to check its status.
# Handling Failures: If a node fails to respond, consider it as unavailable 
# and proceed with failure handling, remove it from the HashRing and redistributing its data.
def check_node_health(node_instance, nodename):
    try:
        if node_instance.ping():
            if nodes[nodename]['status'] == 'inactive':
                print(f"{nodename} is back online. Re-adding to HashRing.")
                addNode(nodename)
            return True
    except redis.exceptions.ConnectionError:
        if nodes[nodename]['status'] == 'active':
            print(f"{nodename} is down. Marking as inactive.")
            removeNode(nodename)
        return False


def periodic_health_check(interval=30):
    """
    Periodically checks the health of all nodes in the HashRing.
    """
    while True:
        for nodename, node_info in nodes.items():
            check_node_health(node_info['instance'], nodename)
        time.sleep(interval)  # Pause for the specified interval

# Start the periodic health check in a separate thread
health_check_thread = threading.Thread(target=periodic_health_check, args=(30,))
health_check_thread.start()
