import time


from hashRing import hashRing, create, read, delete, update, add_node, read_json_file, get_next_node


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

            isAlive = self.Node.nodes[f'node{node_id}']['isAlive']
            if node_exists and not isAlive:
                self.Node.nodes[f'node{node_id}']['isAlive'] = True
                print(f"Node {node_id} has recovered and is now marked as alive.")
                self.Node.broadcast_updated_node_list(node_id, True)

                #add the new node to the hash-ring
                nodename = f"node{node_id}"
                add_node(self.Node.hr, self.Node.nodes, nodename)

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

                if command_or_replication == "COMMAND":
                    self.Network.send_response_to_coordinator(coordinator_name, response, client_id)
                
            elif (command == 'READ'):
                response = read(self.Node.nodename, key)
                coordinator_name = 'node'+str(self.Election.coordinator)

                self.Network.send_response_to_coordinator(coordinator_name, response, client_id)

            if (command == 'UPDATE'):
                value = parts[3]
                response = update(self.Node.nodename, key, value)
                coordinator_name = 'node'+str(self.Election.coordinator)

                if command_or_replication == "COMMAND":
                    self.Network.send_response_to_coordinator(coordinator_name, response, client_id)

            if (command == 'DELETE'):
                response = delete(self.Node.nodename, key)
                coordinator_name = 'node'+str(self.Election.coordinator)

                if command_or_replication == "COMMAND":
                    self.Network.send_response_to_coordinator(coordinator_name, response, client_id)


    def handle_replicated_data(self, from_node, to_node, from_node_data):
        """
        Handles the replicated data: reads from a source node's file,
        checks if each key belongs to the target node, and sends it if so.

        :param from_node: The file name representing data from the source node.
        :param to_node: The target node's identifier.
        :param hr: The hash ring instance.
        """

        nodes = ["node1", "node2", "node3", "node4"]
        hr = hashRing(nodes)
        

        next_node = get_next_node(nodes, to_node)
        next_next_node = get_next_node(nodes, next_node)


        if from_node_data is None:
            print(f"Failed to read data from {from_node}")
            return
        for key, value in from_node_data.items():
            # Check if the key belongs to the target node or the value is from the next node (we should get it to replicate it)
            get_node = hr.get_node(key)
            if get_node == to_node or (get_node == next_node  or get_node == next_next_node):
            # Logic to send this key-value to the target node
                message = f"COMMAND CREATE {key} {value}"
                self.Network.send_message_to_node(to_node, message)

    def handle_create(self, parts, target_node, previous_node1, previous_node2, replication_message, client_id):
        key = parts[1]
        value = parts[2]
        response = create(target_node, key, value)
        if response == "FAILED-DUPLICATED-KEY":
            return response

        #send replication messages to previous 2 nodes (EX: node3 will send to node2 and node1 in order to replicate its data)
        self.Network.send_message_to_node(previous_node1, replication_message, client_id)
        self.Network.send_message_to_node(previous_node2, replication_message, client_id)
        return response
    
    def handle_update(self, parts, target_node, previous_node1, previous_node2, replication_message, client_id):
        key = parts[1]
        value = parts[2]
        response = update(target_node, key, value)
        if response == "ERROR-Key-Not-Found":
            return response

        #send replication messages to previous 2 nodes (EX: node3 will send to node2 and node1 in order to replicate its data)
        self.Network.send_message_to_node(previous_node1, replication_message, client_id)
        self.Network.send_message_to_node(previous_node2, replication_message, client_id)
        return response
    
    def handle_delete(self, parts, target_node, previous_node1, previous_node2, replication_message, client_id):
        key = parts[1]
        response = delete(target_node, key)
        if response == "NO-KEY-TO-UPDATE":
            return response

        #send replication messages to previous 2 nodes (EX: node3 will send to node2 and node1 in order to replicate its data)
        self.Network.send_message_to_node(previous_node1, replication_message, client_id)
        self.Network.send_message_to_node(previous_node2, replication_message, client_id)
        return response
    
    def handle_read(self, key, target_node):
        response = read(target_node, key)
        return response

