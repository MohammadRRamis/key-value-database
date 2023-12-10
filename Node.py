import threading
import time
import socket
import threading
import redis
from uhashring import HashRing


testNodes = {
    'node1': {
        'id': 1,
        'hostname': 'localhost',
        'port': 5010,
        'instance': redis.StrictRedis(host='localhost', port=6379), #replica on port 6370
        'isAlive': True,
        
    },
    'node2': {
        'id': 2,
        'hostname': 'localhost',
        'port': 5020,
        'instance': redis.StrictRedis(host='localhost', port=6378), 
        'isAlive': True,
    },
    'node3': {
        'id': 3,
        'hostname': 'localhost',
        'port': 5030,
        'instance': redis.StrictRedis(host='localhost', port=6377), 
        'isAlive': True,
    },
}


from hashRing import getNodes

class Node(threading.Thread):

    heartbeats = {}  # Shared dictionary to store the latest heartbeat time for each node
    heartbeat_lock = threading.Lock()
    heartbeat_interval = 5  # Interval for sending heartbeats (in seconds)
    failure_timeout = 10    # Time after which a node is considered failed (in seconds)

    def __init__(self, id, hostname, port, nodes, redisInstance):
        super(Node, self).__init__()
        self.pid = id #the id of the node
        self.hostname = hostname #ip address of the server
        self.port = port #port of the server
        self.redisInstance = redisInstance #the database associated with the node
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
        # self.run_server()
        while True:
            self.send_heartbeat() #to enform other nodes that 'i am alive'
            self.monitor_heartbeats() #check if some other node failed
            if self.detect_coordinator_failure(): #if the coordiator failed, start new election 
                print(f"Process {self.pid} detected coordinator failure, starting election.")
                self.start_election()
            time.sleep(1)

    def send_heartbeat(self):
        for node_name, node_info in self.nodes.items():
            if node_info['id'] != self.pid:
                self.send_message(node_info['hostname'], node_info['port'], 'HEARTBEAT')


    def monitor_heartbeats(self):
        current_time = time.time()
        with self.heartbeat_lock:
            for node_id, last_time in self.heartbeats.items():
                if current_time - last_time > self.failure_timeout:
                    if node_id == self.coordinator:
                        self.nodes[f'node{node_id}']['isAlive'] = False
                        self.start_election()
                    # else:
                    self.nodes[f'node{node_id}']['isAlive'] = False


    def detect_coordinator_failure(self):
        current_time = time.time()
        with self.heartbeat_lock:
            if self.coordinator is None or self.coordinator == self.pid:
                return False

            last_heartbeat = self.heartbeats.get(self.coordinator, 0)
            if current_time - last_heartbeat > self.failure_timeout:
                print(f"Node {self.pid} detected that coordinator {self.coordinator} has failed.")
                with self.mutex:
                    self.coordinator = None
                return True
        return False



    def send_message_to_node(self, node_name, message):
        node_info = self.nodes[node_name]
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((node_info['hostname'], node_info['port']))
                sock.sendall(f"{message} {self.pid}".encode())
        except ConnectionRefusedError:
            # print(f"Unable to connect to node {node_name} at {node_info['hostname']}:{node_info['port']}")
            pass


    def receive_election_message(self, initiator_id):
        print(f"Node {self.pid} received an election message from Node {initiator_id}.")
        if self.isAlive:
            self.send_message_to_node(initiator_id, 'ANSWER')
            if not self.election_in_progress and (self.coordinator is None or self.coordinator < self.pid):
                self.start_election()


    def send_answer_message(self, initiator):
        print(f"Process {self.pid} answers to {initiator.pid}")


    def become_coordinator(self):
        print(f"Node {self.pid} becomes the coordinator")
        self.coordinator = self.pid
        for node_name, node_info in self.nodes.items():
            if node_info['id'] != self.pid:
                self.send_message_to_node(node_name, 'COORDINATOR')
                

    def receive_coordinator_message(self, new_coordinator_id):
        with self.mutex:
            if self.isAlive:
                print(f"Node {self.pid} acknowledges new coordinator {new_coordinator_id}")
                self.coordinator = new_coordinator_id


    """
    XxxXXXXXXXXXXXXXXXXXXXXXXXXX
    
    """
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
            if all(not node['isAlive'] for node in higher_nodes):
                self.become_coordinator()

            self.election_in_progress = False


    def check_higher_pid_nodes(self):
        self.response_received = {}
        for node_name, node_info in self.nodes.items():
            if node_info['id'] > self.pid:
                self.send_message_to_node(node_name, 'STATUS_REQUEST')
                self.response_received[node_info['id']] = False

        # Wait for responses (adjust the timeout as needed)
        time.sleep(2)

        return any(self.response_received.values())

    
    def detect_node_failure(self):
        # Check the status of other nodes in the nodes dictionary to detect failures
        for node_name, node_info in self.nodes.items():
            if node_name != self.nodename and node_info["isAlive"] != True:
                print(f"Process {self.pid} detected failure or inactivity of {node_name}.")
                return True
        return False
    

    def restart_election(self):
        # If an election is not already in progress, start a new one
        if not self.election_in_progress:
            self.election_in_progress = True
            self.start_election()
            self.election_in_progress = False

    def run_server(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.hostname, self.port))
            server_socket.listen()
            while True:
                conn, addr = server_socket.accept()
                threading.Thread(target=self.handle_client, args=(conn,)).start()



    def receive_message(self, message, from_node_id):
        if message == 'ELECTION':
            self.receive_election_message(from_node_id)
        elif message == 'COORDINATOR':
            self.receive_coordinator_message(from_node_id)
        elif message == 'STATUS_RESPONSE':
            self.response_received[from_node_id] = True

    def handle_client(self, conn):
        with conn:
            while True:
                data = conn.recv(1024)
                if not data:
                    break
                message, node_id = data.decode().split()
                node_id = int(node_id)
                print(f"Received {message} from Node {node_id}")

                if message == 'HEARTBEAT':
                    self.handle_heartbeat(node_id)
                elif message == 'ELECTION':
                    self.handle_election_message(node_id)
                elif message == 'COORDINATOR':
                    self.receive_coordinator_message(node_id)
                elif message == 'NEW_COORDINATOR':
                    self.handle_new_coordinator(node_id)
                # ... other message handling as needed ...


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



# node = Node(1, 'localhost', 5010, testNodes,  redis.StrictRedis(host='localhost', port=6379))
# node = Node(2, 'localhost', 5020, testNodes,  redis.StrictRedis(host='localhost', port=6379))
node = Node(3, 'localhost', 5030, testNodes,  redis.StrictRedis(host='localhost', port=6379))
node.start()
node.start_election()
node.run_server()

