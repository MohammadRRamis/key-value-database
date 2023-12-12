import time


class Election:

    def __init__(self, Node):
        self.Node = Node
        self.coordinator = None #their is only one coordiantor
        self.election_in_progress = False #if the node started an election

    
    def set_instances(self, Heartbeat, Network):
        self.Heartbeat = Heartbeat
        self.Network = Network


    def start_election(self):
        if not self.election_in_progress:
            self.election_in_progress = True
            higher_nodes = [node_info for node_info in self.Node.nodes.values() if node_info['id'] > self.Node.node_id]

            # Send election messages to nodes with higher IDs
            for node in higher_nodes:
                self.Network.send_message(node['hostname'], node['port'], 'ELECTION')


            # Wait for responses
            time.sleep(2)

            # Check if any higher ID nodes have responded
            with self.Node.mutex:
                alive_higher_nodes = [node for node in higher_nodes if self.Heartbeat.heartbeats.get(node['id'], 0) > time.time() - self.Heartbeat.failure_timeout]

            if not alive_higher_nodes:
                self.become_coordinator()

            self.election_in_progress = False


    def become_coordinator(self):
        print(f"Node {self.Node.node_id} becomes the coordinator")
        
        #when the old coordinator fails, the new coordinator will know have access to the hash-ring 
        self.Node.start_hash_ring()

        self.coordinator = self.Node.node_id
        for node_name, node_info in self.Node.nodes.items():
            if node_info['id'] != self.Node.node_id:
                self.Network.send_message_to_node(node_name, 'COORDINATOR')  

    def request_coordinator_info(self):
        message = f'COORDINATOR_REQUEST {self.Node.node_id}'
        for nodename, node_info in self.Node.nodes.items():
            if node_info['id'] != self.Node.node_id:
                self.Network.send_message(node_info['hostname'], node_info['port'], message)   
