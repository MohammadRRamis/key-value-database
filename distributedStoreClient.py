from uhashring import HashRing
from hashRing import ring, nodesInfo
import socket


class DistributedStoreClient:
    def __init__(self):
        self.ring = ring()
        self.node_info = nodesInfo()
        
        
    def _send_request(self, node_name, action, key, value=None):
        host, port = self.node_info[node_name]['host'], self.node_info[node_name]['port'], 
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host, port))
            request = f'{action}:{key}:{value or ""}'
            s.sendall(request.encode('utf-8'))
            response = s.recv(1024)
        return response.decode('utf-8')

    def create(self, key, value):
        node_name = self.ring.get_node(key)
        return self._send_request(node_name, 'CREATE', key, value)

    def read(self, key):
        node_name = self.ring.get_node(key)
        return self._send_request(node_name, 'READ', key)

    def update(self, key, value):
        node_name = self.ring.get_node(key)
        return self._send_request(node_name, 'UPDATE', key, value)

    def delete(self, key):
        node_name = self.ring.get_node(key)
        return self._send_request(node_name, 'DELETE', key)
