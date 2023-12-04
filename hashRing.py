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

