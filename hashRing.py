from uhashring import HashRing
import redis


nodes = {
    'node1': {
        'hostname': 'localhost',
        'instance': redis.StrictRedis(host='localhost', port=6379),
        'vnodes': 40,
        'port': 6379,
    },
    'node2': {
        'hostname': 'localhost',
        'instance': redis.StrictRedis(host='localhost', port=6378),
        'vnodes': 40,
        'port': 6378,
    },
}

hr = HashRing(nodes)

def ring():
    return hr

def nodesInfo():
    return nodes

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

    hr.remove_node(nodename)
    del nodes[nodename]

    print(f"Node {nodename} removed successfully.")

def addNode(nodename):
    pass




