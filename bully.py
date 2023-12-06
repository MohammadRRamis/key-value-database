import threading
import time


from hashRing import getNodes

class Process(threading.Thread):
    def __init__(self, pid, nodes):
        super(Process, self).__init__()
        self.pid = pid
        self.nodes = nodes
        self.nodename = "node" + str(pid)
        self.coordinator = None
        self.election_in_progress = False
        self.mutex = threading.Lock()
        # self.last_known_status = self.nodes[self.nodename]['status'] 
        self.isAlive = self.nodes[self.nodename]['isAlive']   # Keep track of the last known status


    def detect_coordinator_failure(self):
        # Method to detect coordinator failure (simulation purposes)
        if self.coordinator and not self.coordinator.isAlive:
            print(f"Process {self.pid} detected that coordinator {self.coordinator.pid} has failed.")
            self.coordinator = None
            return True
        return False

    def run(self):
        while True:
            # Check for changes in the node
            if self.detect_change():
                self.restart_election()

            # Check for node failures and restart election if necessary
            if self.detect_node_failure():
                self.restart_election()

            time.sleep(1)  # Sleep to prevent continuous looping without pause

    def send_election_message(self):
        print(f"Process {self.pid} is starting an election.")
        self.election_in_progress = True
        for process in self.all_processes:
            if process.pid > self.pid and process.isAlive:
                process.receive_election_message(self)

        # Wait a bit for responses or a coordinator message
        time.sleep(2)

        # Elect self as coordinator if no higher PID process responded
        if not any(p.pid > self.pid and p.coordinator is not None for p in self.all_processes):
            self.become_coordinator()


    def receive_election_message(self, initiator):
        print(f"Process {self.pid} received an election message from Process {initiator.pid}.")
        if self.isAlive and self.nodes[self.nodename]['isAlive'] == True:
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
        if self.nodes[self.nodename]['isAlive'] == True:
            self.election_in_progress = True
            self.send_election_message()

            # Wait for a fixed time for responses
            time.sleep(2)

            with self.mutex:
                # Check if any higher PID process has responded
                if not any(p.isAlive and p.pid > self.pid for p in self.all_processes):
                    self.become_coordinator()

            self.election_in_progress = False


    def detect_change(self):
        # Check if the current status is different from the last known status
        if self.nodes[self.nodename]['isAlive'] == False:
            return False
        current_status = self.nodes[self.nodename]['isAlive']
        if current_status != self.isAlive:
            print(f"Change detected in node {self.pid}: status changed from {self.isAlive} to {current_status}")
            self.isAlive = current_status  # Update Alive status
            return True
        return False
    
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


# Your nodes dictionary
nodes = getNodes()

# Create a list of processes with the IDs derived from the Redis port numbers
processes = [Process(node_info['id'], nodes) for node_info in nodes.values()]


# Each process knows about all other processes
for process in processes:
    process.all_processes = processes

# Start all processes
for process in processes:
    process.start()

# Give some time for the threads to start
time.sleep(1)

# Start the election from the node with the lowest ID
processes.sort(key=lambda x: x.pid)  # Sort the processes by pid
processes[0].start_election()  # Start an election from the lowest ID process

# Give time for the election to complete
time.sleep(5)

# Check and print who is the coordinator after the election
coordinators = [p for p in processes if p.coordinator is p]
if coordinators:
    print(f"The elected coordinator is Process {coordinators[0].pid}")
else:
    print("No coordinator was elected.")
