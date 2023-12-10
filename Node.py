import threading
import time
import socket
import threading
from uhashring import HashRing



from hashRing import getNodes

class Node(threading.Thread):

    heartbeats = {}  # Shared dictionary to store the latest heartbeat time for each node
    heartbeat_lock = threading.Lock()
    heartbeat_interval = 5  # Interval for sending heartbeats (in seconds)
    failure_timeout = 10    # Time after which a node is considered failed (in seconds)

    def __init__(self, id, hostname, port, nodes, redisInstance, hr):
        super(Node, self).__init__()
        self.pid = id
        self.hostname = hostname
        self.port = port
        self.redisInstance = redisInstance
        self.nodes = nodes
        self.nodename = "node" + str(id)
        self.coordinator = None
        self.election_in_progress = False
        self.mutex = threading.Lock()
        self.isAlive = True 
        self.server_running = threading.Event()
        self.run_server()
        self.hr = hr

    def run(self):
        while True:
            self.send_heartbeat()
            self.monitor_heartbeats()
            if self.detect_coordinator_failure():
                print(f"Process {self.pid} detected coordinator failure, starting election.")
                self.start_election()
            time.sleep(1)

    def send_heartbeat(self):
        with Node.heartbeat_lock:
            Node.heartbeats[self.pid] = time.time()

    def monitor_heartbeats(self):
        with Node.heartbeat_lock:
            current_time = time.time()
            for pid, last_heartbeat in list(Node.heartbeats.items()):
                if pid != self.pid and current_time - last_heartbeat > Node.failure_timeout:
                    print(f"Process {self.pid} detected failure of node {pid}.")
                    Node.heartbeats.pop(pid, None)  # Remove the failed node
                    if self.coordinator and self.coordinator.pid == pid:
                        self.start_election()  # Start an election if the coordinator has failed


    def detect_coordinator_failure(self):
        with self.mutex:
            if self.coordinator is None:
                return False
            
            print(f"Process {self.pid} checking coordinator {self.coordinator.pid} status: {self.coordinator.isAlive}")
            if not self.coordinator.isAlive:
                print(f"Process {self.pid} detected that coordinator {self.coordinator.pid} has failed.")
                self.coordinator = None
                return True

    def send_election_message(self):
        with self.mutex:
            print(f"Process {self.pid} is starting an election.")
            self.election_in_progress = True
            for process in self.all_processes:
                if process.pid > self.pid and process.isAlive:
                    process.receive_election_message(self)
            time.sleep(2)
            if not any(p.pid > self.pid and p.coordinator == p for p in self.all_processes):
                self.become_coordinator()
            self.election_in_progress = False



    def receive_election_message(self, initiator):
        print(f"Process {self.pid} received an election message from Process {initiator.pid}.")
        if self.isAlive:
            self.send_answer_message(initiator)
            # Start own election if not already in progress and no coordinator from a higher PID
            if not self.election_in_progress and (self.coordinator is None or self.coordinator.pid < self.pid):
                self.start_election()


    def send_answer_message(self, initiator):
        print(f"Process {self.pid} answers to {initiator.pid}")


    def become_coordinator(self):
        print(f"Process {self.pid} becomes the coordinator")
        self.coordinator = self
        for process in self.all_processes:
            if process.pid != self.pid:
                process.receive_coordinator_message(self)
                
    def receive_coordinator_message(self, new_coordinator):
        with self.mutex:
            if self.isAlive:
                print(f"Process {self.pid} acknowledges new coordinator {new_coordinator.pid}")
                self.coordinator = new_coordinator

    def start_election(self):
        if self.isAlive:
            self.election_in_progress = True
            self.send_election_message()

            # Wait for a fixed time for responses
            time.sleep(2)

            with self.mutex:
                # Check if any higher PID process has responded
                if not any(p.isAlive and p.pid > self.pid for p in self.all_processes):
                    self.become_coordinator()

            self.election_in_progress = False

    
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


    def handle_client(self, conn):
        with conn:
            while True:
                data = conn.recv(1024)
                if not data:
                    break
                # Process request and send response
                response = self.process_request(data.decode())
                conn.sendall(response.encode())

    def process_request(self, request):
        result = self.hr["node1"].get(1)
        return result
        


# # Your nodes dictionary
# nodes = getNodes()

# # Create a list of processes with the IDs derived from the Redis port numbers
# processes = [Node(node_info['id'], nodes) for node_info in nodes.values()]


# # Each process knows about all other processes
# for process in processes:
#     process.all_processes = processes

# # Start all processes
# for process in processes:
#     process.start()

# # Give some time for the threads to start
# time.sleep(1)


# # Start the election from the node with the lowest ID
# processes.sort(key=lambda x: x.pid)  # Sort the processes by pid
# processes[0].start_election()  # Start an election from the lowest ID process

# # Give time for the election to complete
# time.sleep(5)

# # Check and print who is the coordinator after the election
# coordinators = [p for p in processes if p.coordinator is p]
# if coordinators:
#     print(f"The elected coordinator is Process {coordinators[0].pid}")
# else:
#     print("No coordinator was elected.")