import time


from hashRing import hashRing, create, read, delete


class MessageHandler:
    def __init__(self, Node):
        self.Node = Node  # Reference to the node instance

    def set_instances(self, Election, Heartbeat, Network):
        self.Election = Election
        self.Heartbeat = Heartbeat
        self.Network = Network

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
            elif message == 'NODES-UPDATE':
                self.handle_status_update(parts)
            # elif 'COMMAND' in message:
            #     print("yes")
            #     self.handle_command(parts)
                
    def handle_status_update(self, parts):
        node_id = parts[1]
        isAlive = parts[2]
        # Update the node's status in the local dictionary
        with self.Heartbeat.heartbeat_lock: 
            node_exists = any(node_info['id'] == int(node_id) for node_info in self.Node.nodes.values())
            if node_exists:
                self.Node.nodes['node'+node_id]['isAlive'] = isAlive
                print(f"Node {node_id} status updated to {isAlive}.")


    def handle_recovery(self, node_id):
        # Update the status of the recovered node
        with self.Heartbeat.heartbeat_lock:
            node_exists = any(node_info['id'] == node_id for node_info in self.Node.nodes.values())
            if node_exists:
                self.Node.nodes[f'node{node_id}']['isAlive'] = True
                print(f"Node {node_id} has recovered and is now marked as alive.")
                self.Node.broadcast_updated_node_list(node_id, True)

    def handle_new_coordinator(self, coordinator_id):
        with self.Node.mutex:
            self.Election.coordinator = coordinator_id
            print(f"Node {self.Node.node_id} acknowledges new coordinator {coordinator_id}")

    def handle_coordinator_request(self, from_node_id):
        # Respond with coordinator information if this node is aware of the current coordinator
        if self.Election.coordinator is not None:
            self.Network.send_message(self.Node.nodes[f'node{from_node_id}']['hostname'], self.Node.nodes[f'node{from_node_id}']['port'], f'COORDINATOR_INFO {self.Election.coordinator}')
    


    def handle_coordinator_info(self, from_node_id, message_parts):
        # Assuming the coordinator ID is the second part of the message
        coordinator_id = int(message_parts[1])
        with self.Node.mutex:
            if self.Election.coordinator is None or self.Election.coordinator != coordinator_id:
                self.Election.coordinator = coordinator_id
                print(f"Updated coordinator to {coordinator_id} based on info from Node {from_node_id}")

    def handle_heartbeat(self, node_id):
        # Update the last received heartbeat timestamp for the node
        self.Heartbeat.heartbeats[node_id] = time.time()

    def handle_election_message(self, from_node_id):
    # Respond to the election message if this node's ID is higher
        if self.Node.node_id > from_node_id:
            self.Network.send_message(self.Node.nodes[f'node{from_node_id}']['hostname'], self.Node.nodes[f'node{from_node_id}']['port'], 'ELECTION_RESPONSE')


    def receive_coordinator_message(self, new_coordinator_id):
        with self.Node.mutex:
            if self.Node.isAlive:
                print(f"Node {self.Node.node_id} acknowledges new coordinator {new_coordinator_id}")
                self.Election.coordinator = new_coordinator_id

    #the command is sent only from the coordinator
    def handle_command(self, parts):
        "COMMAND CREATE KEY VALUE CLIENT_CONN"
        "REPLICATION CREATE KEY VALUE CLIENT_CONN"

        command_or_replication = parts[0]
        command = parts[1]
        key = parts[2]
        client_id = parts[-1]

        """
        if the the node successfully created the key, 
        send a message to the coordinator
        """
   
        if command in ['CREATE', 'READ', 'UPDATE', 'DELETE']:
            if (command == 'CREATE'):
                value = parts[3]
                response = create(self.Node.nodename, key, value)
                coordinator_name = 'node'+str(self.Election.coordinator)
                #response is either:
                # "FAIL Duplicated Key" or "SUCCESS"
                if command_or_replication == "COMMAND":
                    self.Network.send_response_to_coordinator(coordinator_name, response, client_id)
                
            elif (command == 'READ'):
                response = read(self.Node.nodename, key)
                coordinator_name = 'node'+str(self.Election.coordinator)
                #response is either:
                # "FAIL Duplicated Key" or "SUCCESS"
                self.Network.send_response_to_coordinator(coordinator_name, response, client_id)


