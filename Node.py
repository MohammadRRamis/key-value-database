import threading
import time
import socket
import threading
import json
import pickle



nodes = {
    'node1': {
        'id': 1,
        'hostname': 'localhost',
        'port': 5010,
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
}

from HashRing import hashRing

class Node(threading.Thread):

    heartbeats = {}  # Shared dictionary to store the latest heartbeat time for each node
    heartbeat_lock = threading.Lock()
    heartbeat_interval = 5  # Interval for sending heartbeats (in seconds)
    failure_timeout = 10    # Time after which a node is considered failed (in seconds)

    def __init__(self, id, hostname, port, nodes):
        super(Node, self).__init__()
        self.pid = id #the id of the node
        self.hostname = hostname #ip address of the server
        self.port = port #port of the server
        self.nodes = nodes #a list of the other nodes in the distributed system
        self.nodename = "node" + str(id)
        self.coordinator = None #their is only one coordiantor
        self.election_in_progress = False #if the node started an election
        self.isAlive = True 
        self.mutex = threading.Lock() 
        self.server_running = threading.Event()
        self.heartbeats = {}  # Shared dictionary to store the latest heartbeat time for each node
        self.heartbeat_lock = threading.Lock()
        self.heartbeat_interval = 5  # Interval for sending heartbeats (in seconds)
        self.failure_timeout = 10    # Time after which a node is considered failed (in seconds)
 
        
        
    def run(self):

        #when a new node joins the system, it will ask about the coordinator information
        self.request_coordinator_info()
        time.sleep(1)
        self.notify_recovery()
        while True:
            self.send_heartbeat() #to enform other nodes that 'i am alive'
            self.monitor_heartbeats() #check if some other node failed 
            time.sleep(1)

    def request_coordinator_info(self):
        message = f'COORDINATOR_REQUEST {self.pid}'
        for node_name, node_info in self.nodes.items():
            if node_info['id'] != self.pid:
                self.send_message(node_info['hostname'], node_info['port'], message)

    def send_heartbeat(self):
        for node_name, node_info in self.nodes.items():
            if node_info['id'] != self.pid:
                self.send_message(node_info['hostname'], node_info['port'], 'HEARTBEAT')


    def notify_recovery(self):
        # Send a message to the coordinator indicating that this node has recovered
        if self.coordinator is not None and self.coordinator != self.pid:
            coordinator_info = self.nodes[f'node{self.coordinator}']
            self.send_message(coordinator_info['hostname'], coordinator_info['port'], f'RECOVERY {self.pid}')


    def monitor_heartbeats(self):
        current_time = time.time()
        with self.heartbeat_lock:
            for node_id, last_time in self.heartbeats.items():
                if current_time - last_time > self.failure_timeout:
                    # If the failing node is the coordinator, start an election
                    if node_id == self.coordinator:
                        print(f"Node {self.pid} detected that the coordinator (Node {node_id}) has failed.")
                        # self.nodes[f'node{node_id}']['isAlive'] = False
                        self.start_election()
                    # If this node is the coordinator and detects another node's failure, update the list and broadcast
                    elif self.pid == self.coordinator and self.nodes[f'node{node_id}']['isAlive']:
                        self.nodes[f'node{node_id}']['isAlive'] = False
                        print(f"Coordinator (Node {self.pid}) detected that Node {node_id} has failed.")
                        self.broadcast_updated_node_list(node_id, False)

                        time.sleep(0.5)

                        # the coordinator will remove any node that failed
                        # including the prev. coordinator
                        # hashRing.removeNode("node"+node_id)


            # If this node thinks there's no coordinator and an election is not in progress, start an election
            if self.coordinator is None and not self.election_in_progress:
                self.start_election()


    def send_message_to_node(self, node_name, message):
        node_info = self.nodes[node_name]
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((node_info['hostname'], node_info['port']))
                sock.sendall(f"{message} {self.pid}".encode())
        except ConnectionRefusedError:
            # print(f"Unable to connect to node {node_name} at {node_info['hostname']}:{node_info['port']}")
            pass


    def become_coordinator(self):
        print(f"Node {self.pid} becomes the coordinator")
        
        #when the old coordinator fails, the new coordinator will have access to the hash-ring 
        self.start_hash_ring()
        print(self.hr)

        self.coordinator = self.pid
        for node_name, node_info in self.nodes.items():
            if node_info['id'] != self.pid:
                self.send_message_to_node(node_name, 'COORDINATOR')
                

    def start_election(self):
        if not self.election_in_progress:
            self.election_in_progress = True
            higher_nodes = [node_info for node_info in self.nodes.values() if node_info['id'] > self.pid]

            # Send election messages to nodes with higher IDs
            for node in higher_nodes:
                self.send_message(node['hostname'], node['port'], 'ELECTION')

            # Wait for responses
            time.sleep(2)

            # Check if any higher ID nodes have responded
            with self.mutex:
                alive_higher_nodes = [node for node in higher_nodes if self.heartbeats.get(node['id'], 0) > time.time() - self.failure_timeout]

            if not alive_higher_nodes:
                self.become_coordinator()

            self.election_in_progress = False
    

    def run_server(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.hostname, self.port))
            server_socket.listen()
            while True:
                conn, addr = server_socket.accept()
                threading.Thread(target=self.handle_client, args=(conn,)).start()

    def send_message(self, hostname, port, message):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((hostname, port))
                sock.sendall(f"{message} {self.pid}".encode())
                # print(f"Sent {message} to {hostname}:{port}")
        except ConnectionRefusedError:
            # print(f"Unable to connect to node at {hostname}:{port}")
            pass
        except Exception as e:
            print(f"Error while sending message to {hostname}:{port}: {e}")

    def handle_client(self, conn):
        with conn:
            while True:
                data = conn.recv(2024)
                if not data:
                    break

                


                parts = data.decode().split()
                if len(parts) >= 2:
                    message = parts[0]
                    node_id = int(parts[1])

                # Don't print the heartbeat message, because it will do it forever
                if message != "HEARTBEAT" and message != "UPDATE":
                    print(f"Received {message} from Node {node_id}")

                self.handle_messages(message, node_id, parts)


    def handle_messages(self, message, node_id, parts):
            if message == 'HEARTBEAT':
                self.handle_heartbeat(node_id)
            elif message == 'ELECTION':
                self.handle_election_message(node_id)
            elif message == 'COORDINATOR':
                self.receive_coordinator_message(node_id)
            elif message == 'NEW_COORDINATOR':
                self.handle_new_coordinator(node_id)
            elif message == 'COORDINATOR_REQUEST':
                self.handle_coordinator_request(node_id)
            elif message == 'COORDINATOR_INFO':
                self.handle_coordinator_info(node_id, parts)
            elif message == 'RECOVERY':
                self.handle_recovery(node_id)
            elif message == 'UPDATE':
                self.handle_status_update(parts)


    def handle_status_update(self, parts):
        node_id = parts[1]
        isAlive = parts[2]
        # Update the node's status in the local dictionary
        with self.heartbeat_lock: 
            node_exists = any(node_info['id'] == int(node_id) for node_info in self.nodes.values())
            if node_exists:
                self.nodes['node'+node_id]['isAlive'] = isAlive
                print(f"Node {node_id} status updated to {isAlive}.")
        

    def receive_coordinator_message(self, new_coordinator_id):
        with self.mutex:
            if self.isAlive:
                print(f"Node {self.pid} acknowledges new coordinator {new_coordinator_id}")
                self.coordinator = new_coordinator_id

    def handle_recovery(self, node_id):
        # Update the status of the recovered node
        with self.heartbeat_lock:
            node_exists = any(node_info['id'] == node_id for node_info in self.nodes.values())
            if node_exists:
                self.nodes[f'node{node_id}']['isAlive'] = True
                print(f"Node {node_id} has recovered and is now marked as alive.")
                self.broadcast_updated_node_list(node_id, True)

    def handle_coordinator_request(self, from_node_id):
        # Respond with coordinator information if this node is aware of the current coordinator
        if self.coordinator is not None:
            self.send_message(self.nodes[f'node{from_node_id}']['hostname'], self.nodes[f'node{from_node_id}']['port'], f'COORDINATOR_INFO {self.coordinator}')

    def handle_coordinator_info(self, from_node_id, message_parts):
        # Assuming the coordinator ID is the second part of the message
        coordinator_id = int(message_parts[1])
        with self.mutex:
            if self.coordinator is None or self.coordinator != coordinator_id:
                self.coordinator = coordinator_id
                print(f"Updated coordinator to {coordinator_id} based on info from Node {from_node_id}")

                
    def handle_new_coordinator(self, coordinator_id):
        with self.mutex:
            self.coordinator = coordinator_id
            print(f"Node {self.pid} acknowledges new coordinator {coordinator_id}")

    def handle_heartbeat(self, node_id):
        # Update the last received heartbeat timestamp for the node
        self.heartbeats[node_id] = time.time()


    def handle_election_message(self, from_node_id):
    # Respond to the election message if this node's ID is higher
        if self.pid > from_node_id:
            self.send_message(self.nodes[f'node{from_node_id}']['hostname'], self.nodes[f'node{from_node_id}']['port'], 'ELECTION_RESPONSE')


    #the coordinator 
    def broadcast_updated_node_list(self, node_id, isAlive):

        message = f"UPDATE {node_id} {isAlive}"
        # Send this message to all other nodes except this one
        for target_node_name, target_node_info in self.nodes.items():
               if target_node_info['id'] != self.pid:
                self.send_message(target_node_info['hostname'], target_node_info['port'], message)


    def start_hash_ring(self):
        # coordinator will start the hashring, with the updated list of nodes
        # filter the nodes dict to create hash a ring with alive nodes only
        alive_nodes = {node_id: node_info for node_id, node_info in self.nodes.items() if node_info['isAlive']}
        self.hr = hashRing(alive_nodes)



    def forward_request_to_coordiantor():
        pass 

    def client_request():
        pass 

node = Node(1, 'localhost', 5010, nodes)
node = Node(2, 'localhost', 5020, nodes)
node = Node(3, 'localhost', 5030, nodes)

node.start()
node.run_server()

