import threading
import time
import socket
import threading
import json
from classes.HeartBeat import Heartbeat
from classes.Network import Network
from classes.Election import Election
from classes.MessageHandler import MessageHandler




from hashRing import hashRing

class Node(threading.Thread):


    def __init__(self, id, hostname, port, nodes):
        super(Node, self).__init__()
        self.node_id = id #the id of the node
        self.hostname = hostname #ip address of the server
        self.port = port #port of the server
        self.nodes = nodes #a list of the other nodes in the distributed system
        self.nodename = "node" + str(id)
        self.isAlive = True 
        self.mutex = threading.Lock() 
        self.server_running = threading.Event()
        self.hr = None

        #define classess instances
        self.Network = Network(self, hostname, port)
        self.Election = Election(self) 
        self.Heartbeat = Heartbeat(self)  # Create Heartbeat instance
        self.MessageHandler = MessageHandler(self)  # Create Heartbeat instance

        # Set up cross-references
        self.Heartbeat.set_instances(self.Election, self.Network)
        self.Election.set_instances(self.Heartbeat, self.Network)
        self.Network.set_instances(self.MessageHandler, self.Election)
        self.MessageHandler.set_instances(self.Election, self.Heartbeat, self.Network)



    def run(self):
        #when a new node joins the system, it will ask about the coordinator information
        self.Election.request_coordinator_info()
        time.sleep(1)
        self.Heartbeat.notify_recovery()
        while True:
            self.Heartbeat.send_heartbeat() #to enform other nodes that 'i am alive'
            self.Heartbeat.monitor_heartbeats() #check if some other node failed 
            time.sleep(1)


    #the coordinator 
    def broadcast_updated_node_list(self, node_id, isAlive):

        message = f"NODES-UPDATE {node_id} {isAlive}"
        # Send this message to all other nodes except this one
        for target_node_name, target_node_info in self.nodes.items():
               if target_node_info['id'] != self.node_id:
                self.Network.send_message(target_node_info['hostname'], target_node_info['port'], message)

    def start_hash_ring(self):
        # coordinator will start the hashring, with the updated list of nodes
        # filter the nodes dict to create hash a ring with alive nodes only
        alive_nodes = {node_id: node_info for node_id, node_info in self.nodes.items() if node_info['isAlive']}
        self.hr = hashRing(sorted(alive_nodes))

    def redirect_to_coordinator():
        pass 





nodes = {
    'node1': {
        'id': 1,
        'hostname': 'localhost',
        'port': 5004,
        'isAlive': True,
    },
    'node2': {
        'id': 2,
        'hostname': 'localhost',
        'port': 5020,
        'isAlive': True,
    },
    'node3': {
        'id': 3,
        'hostname': 'localhost',
        'port': 5030,
        'isAlive': True,
    },
    'node4': {
        'id': 4,
        'hostname': 'localhost',
        'port': 5040,
        'isAlive': True,
    },
}    

node = Node(1, 'localhost', 5004, nodes)
node = Node(2, 'localhost', 5020, nodes)
node = Node(3, 'localhost', 5030, nodes)
node = Node(4, 'localhost', 5040, nodes)

node.start()
node.Network.run_server()

