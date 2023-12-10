from Node import Node
from uhashring import HashRing
import redis

testNodes = {
    'node1': {
        'id': 1,
        'hostname': 'localhost',
        'instance': redis.StrictRedis(host='localhost', port=6379), #replica on port 6370
        'vnodes': 40,
        'port': 6379,
        'isAlive': True,
        'replica': redis.StrictRedis(host='localhost', port=6370),
        
    },
    'node2': {
        'id': 2,
        'hostname': 'localhost',
        'instance': redis.StrictRedis(host='localhost', port=6378), 
        'vnodes': 40,
        'port': 6378,
        'isAlive': True,
        'replica': redis.StrictRedis(host='localhost', port=6371),
    },
}
hr = HashRing(testNodes)


nodes = [
        Node(1, 'localhost', 7056, testNodes,  redis.StrictRedis(host='localhost', port=6379), hr),
        Node(2, 'localhost', 7057, testNodes, redis.StrictRedis(host='localhost', port=6378), hr)
]

    


