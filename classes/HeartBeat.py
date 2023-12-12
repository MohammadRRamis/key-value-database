import time
import threading


class Heartbeat:
    def __init__(self, node):
        self.Node = node  # Reference to the Node instance
        self.heartbeat_lock = threading.Lock()
        self.heartbeats = {}  # Stores the latest heartbeat time for each node
        self.heartbeat_interval = 5  # Interval for sending heartbeats (in seconds)
        self.failure_timeout = 10    # Time after which a node is considered failed (in seconds)

    def set_instances(self, Election, Network):
        self.Election = Election
        self.Network = Network

    def send_heartbeat(self):
        for node_name, node_info in self.Node.nodes.items():
            if node_info['id'] != self.Node.node_id:
                self.Network.send_message(node_info['hostname'], node_info['port'], 'HEARTBEAT')

    def monitor_heartbeats(self):
        current_time = time.time()
        with self.heartbeat_lock:
            for node_id, last_time in self.heartbeats.items():
                if current_time - last_time > self.failure_timeout:
                    # If the failing node is the coordinator, start an election
                    if node_id == self.Election.coordinator:
                        print(f"Node {self.Node.node_id} detected that the coordinator (Node {node_id}) has failed.")
                        # self.nodes[f'node{node_id}']['isAlive'] = False
                        self.Election.start_election()
                    # If this node is the coordinator and detects another node's failure, update the list and broadcast
                    elif self.Node.node_id == self.Election.coordinator and self.Node.nodes[f'node{node_id}']['isAlive']:
                        self.Node.nodes[f'node{node_id}']['isAlive'] = False
                        print(f"Coordinator (Node {self.Node.node_id}) detected that Node {node_id} has failed.")
                        self.Node.broadcast_updated_node_list(node_id, False)

                        time.sleep(0.5)

                        # the coordinator will remove any node that failed
                        # including the prev. coordinator
                        # hashRing.removeNode("node"+node_id)
                        
            # If this node thinks there's no coordinator and an election is not in progress, start an election
            if self.Election.coordinator is None and not self.Election.election_in_progress:
                self.Election.start_election()


    def notify_recovery(self):
        # Send a message to the coordinator indicating that this node has recovered
        if self.Election.coordinator is not None and self.Election.coordinator != self.Node.node_id:
            coordinator_info = self.Node.nodes[f'node{self.Election.coordinator}']
            self.Network.send_message(coordinator_info['hostname'], coordinator_info['port'], f'RECOVERY {self.Node.node_id}')

