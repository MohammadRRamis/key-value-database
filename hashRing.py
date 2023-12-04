from uhashring import HashRing


nodes = {
    'node1': {
            'host': 'localhost',
            'data_file': 'node1_data.json',
            'port': 6001,

        },
    'node2': {
            'host': 'localhost',
            'data_file': 'node2_data.json',
            'port': 6002,

        },
    'node3': {
            'host': 'localhost',
            'data_file': 'node2_data.json',
            'port': 6003,
        }
    }
hr = HashRing(nodes)

def ring():
    return hr

def nodesInfo():
    return nodes