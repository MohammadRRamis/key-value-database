import redis

nodes = {
    'node1': {
        'hostname': 'localhost',
        'instance': redis.StrictRedis(host='localhost', port=6379), #replica on port 6370
        'vnodes': 40,
        'port': 6379,
        'status': 'active',
        'replica': redis.StrictRedis(host='localhost', port=6370), 
    },
    'node2': {
        'hostname': 'localhost',
        'instance': redis.StrictRedis(host='localhost', port=6378), 
        'vnodes': 40,
        'port': 6378,
        'status': 'active',
        'replica': redis.StrictRedis(host='localhost', port=6371),
    },
    'node3': {
        'hostname': 'localhost',
        'instance': redis.StrictRedis(host='localhost', port=6378), 
        'vnodes': 40,
        'port': 6378,
        'status': 'active',
        'replica': redis.StrictRedis(host='localhost', port=6372),
    },
}

def getNodes():
    return nodes

def update_node_status(node_name, status):
    nodes[node_name]['status'] = status



