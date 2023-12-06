import threading
import time
import redis

class Process(threading.Thread):
    def __init__(self, pid, all_processes):
        super(Process, self).__init__()
        self.pid = pid
        self.all_processes = all_processes
        self.coordinator = None
        self.mutex = threading.Lock()
        self.is_alive = True  # For simulating process failure

    def detect_coordinator_failure(self):
        # Method to detect coordinator failure (simulation purposes)
        if self.coordinator and not self.coordinator.is_alive:
            print(f"Process {self.pid} detected that coordinator {self.coordinator.pid} has failed.")
            self.coordinator = None
            return True
        return False

    def run(self):
        while True:
            time.sleep(1)
            with self.mutex:
                if self.detect_coordinator_failure():
                    self.start_election()

    def send_election_message(self):
        print(f"Process {self.pid} is starting an election.")
        for process in self.all_processes:
            if process.pid > self.pid and process.is_alive:
                process.receive_election_message(self)

    def receive_election_message(self, initiator):
        with self.mutex:
            if self.is_alive:
                print(f"Process {self.pid} received an election message from Process {initiator.pid}.")
                self.send_answer_message(initiator)
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
            if self.is_alive:
                print(f"Process {self.pid} acknowledges new coordinator {new_coordinator.pid}")
                self.coordinator = new_coordinator

    def start_election(self):
        self.send_election_message()
        # Wait a bit for answer messages
        time.sleep(2)
        with self.mutex:
            # If no higher process has taken over, become the coordinator
            if not any(p.is_alive and p.pid > self.pid for p in self.all_processes):
                self.become_coordinator()

    def simulate_failure(self):
        with self.mutex:
            print(f"Process {self.pid} has failed.")
            self.is_alive = False

    def simulate_recovery(self):
        with self.mutex:
            print(f"Process {self.pid} has recovered.")
            self.is_alive = True
            self.start_election()

# Assuming the Process class is already defined and modified to handle your nodes structure

# Your nodes dictionary
nodes = {
    'node1': {
        'id': 1,
        'hostname': 'localhost',
        'instance': redis.StrictRedis(host='localhost', port=6379),
        'vnodes': 40,
        'port': 6379,
        'status': 'active',
        'replica': redis.StrictRedis(host='localhost', port=6370), 
    },
    'node2': {
        'id': 2,
        'hostname': 'localhost',
        'instance': redis.StrictRedis(host='localhost', port=6378), 
        'vnodes': 40,
        'port': 6378,
        'status': 'active',
        'replica': redis.StrictRedis(host='localhost', port=6371),
    },
    'node3': {
        'id': 3,
        'hostname': 'localhost',
        'instance': redis.StrictRedis(host='localhost', port=6377), 
        'vnodes': 40,
        'port': 6377,
        'status': 'active',
        'replica': redis.StrictRedis(host='localhost', port=6372),
    },
    'node5': {
        'id': 5,
        'hostname': 'localhost',
        'instance': redis.StrictRedis(host='localhost', port=6374), 
        'vnodes': 40,
        'port': 6374,
        'status': 'active',
        'replica': redis.StrictRedis(host='localhost', port=6374),
    },
    'node4': {
        'id': 4,
        'hostname': 'localhost',
        'instance': redis.StrictRedis(host='localhost', port=6374), 
        'vnodes': 40,
        'port': 6374,
        'status': 'active',
        'replica': redis.StrictRedis(host='localhost', port=6374),
    },
}

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
