import socket
import threading

from hashRing import create, read, delete, get_previous_node

class Network:
    def __init__(self, Node, hostname, port):
        self.Node = Node  # Reference to the Node instance
        self.hostname = hostname #ip address of the server
        self.port = port #port of the server
        self.active_clients = {}  # Dictionary to store active client connections



    def set_instances(self, MessageHandler, Election):
        self.MessageHandler = MessageHandler
        self.Election = Election

    def run_server(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.hostname, self.port))
            server_socket.listen()
            while True:
                conn, addr = server_socket.accept()
                client_id = self.generate_unique_client_id(addr)
                self.active_clients[client_id] = conn  # Store the client connection
                threading.Thread(target=self.handle_client, args=(conn, client_id)).start()

    # ------------------------------------------------------------
    def send_message(self, hostname, port, message):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((hostname, port))
                sock.sendall(f"{message} {self.Node.node_id}".encode())
                # print(f"Sent {message} to {hostname}:{port}")
        except ConnectionRefusedError:
            # print(f"Unable to connect to node at {hostname}:{port}")
            pass
        except Exception as e:
            print(f"Error while sending message to {hostname}:{port}: {e}")



    """
    The send_message_to_node method now takes an optional client_id parameter.
        "If client_id is provided"-, this mean that the node is communicating with a client
        "If client_id is not provided"-, this mean that the node is communicating with another server node
    
    """
    def send_message_to_node(self, node_name, message, client_id=None):
        node_info = self.Node.nodes[node_name]
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((node_info['hostname'], node_info['port']))

                if not client_id:
                    sock.sendall(f"{message} {self.Node.node_id}".encode())

                if client_id:
                    # If client_conn is provided, send the client_conn to communicate with him when a response is back
                    sock.sendall(f"{message} {self.Node.node_id} {client_id}".encode())

        except ConnectionRefusedError:
            pass
        except Exception as e:
            pass
        

    def handle_client(self, conn, client_id):
        try:
            with conn:
                while True:
                    data = conn.recv(1024)
                    if not data:
                        break
                    data = data.decode()
                    parts = data.split()

                    if parts[0] in ['CREATE', 'READ', 'UPDATE', 'DELETE'] and parts[0] != 'NODES-UPDATE':
                        self.request_from_client(parts, data, conn, client_id)

                    #if the request comes from other node in the system
                    else:
                        self.request_from_node(parts)
                
        except Exception as e:
                print(f"Error with client {client_id}: {e}")

        finally:    
                conn.close()
                self.handle_client_disconnection(client_id)



    def request_from_node(self, parts):


        if parts[0] == "NODE-CLIENT-RESPONSE":
            self.handle_response(parts)

        elif len(parts) <= 3 or parts[0] == "NODES-UPDATE":
            message = parts[0]
            node_id = int(parts[1])
            self.MessageHandler.handle_messages(message, node_id, parts)

        #if the message is a COMMAND (CREATE KEY VALUE) 
        # or COMMAND READ 2
        elif len(parts) >= 5 or parts[0] == "COMMAND":
            self.MessageHandler.handle_command(parts)
            return
        

    def request_from_client(self, parts, data, client_conn, client_id):

        #Example of request: CREATE KEY VALUE
        command = parts[0]

        #if the request comes to a coordinator, handle it, otherwise pass the request to the coordinator
        if self.Election.coordinator:
            # Handle the command (CREATE, READ, DELETE) directly
            self.process_client_request(data, command, client_conn, client_id)
        else:
            # Redirect client to the coordinator
            self.redirect_to_coordinator(data, client_conn)


    def process_client_request(self, data, command, client_conn, client_id):
        parts = data.split()
        key = parts[1]

        #find the next two nodes, to replicate the data
        target_node = self.Node.hr.get_node(key)
        print(target_node)

        #get the list of the alive nodes in the ring
        hr_nodes = self.Node.hr.get_nodes()
        next_node1 = get_previous_node(hr_nodes, target_node)
        next_node2 = get_previous_node(hr_nodes, next_node1)
        
        #if the coordiantor does no have the key in his database
        #send a request to the target_node to get the value
        #then send the value to the client
        if (self.Node.nodename != target_node):
            #Format:  COMMAND CREATE KEY VALUE
            message = f"COMMAND {data}"
            replication_message = f"REPLICATION {data}"
            

            self.send_message_to_node(target_node, message, client_id)
            if parts[0] in ['CREATE', 'UPDATE', 'DELETE']:
                self.send_message_to_node(next_node1, replication_message, client_id)
                self.send_message_to_node(next_node2, replication_message, client_id)
            #send response to the client

        # If the target node is this node, process the request and respond to the client
        else:
            if command == 'CREATE':
                value = parts[2]
                response = create(target_node, key, value)
                create(next_node1, key, value)
                create(next_node2, key, value)
                client_conn.sendall(response.encode())

            elif command == 'READ':
                target_node = self.Node.hr.get_node(key)
                response = read(target_node, key)
                client_conn.sendall(response.encode())

            elif command == 'DELETE':
                pass

            elif command == 'UPDATE':
                pass

    def send_response_to_coordinator(self, coordinator_name, response, client_id):
        try:
            coordinator_info = self.Node.nodes[coordinator_name]  # Assuming this contains coordinator's address info

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((coordinator_info['hostname'], coordinator_info['port']))

                # Prepare a message that includes both the client ID and the response
                message = f"NODE-CLIENT-RESPONSE {response} {client_id}"
                sock.sendall(message.encode('utf-8'))

        except Exception as e:
            print(f"Error sending response to coordinator: {e}")
            # Handle the exception, log it, or take corrective measures

    def handle_response(self, parts):
        if parts[0] == "NODE-CLIENT-RESPONSE":
            # Extract the client identifier and the response
            response = parts[1]
            client_id = parts[-1]

            # Forward the response to the appropriate client
            self.forward_response_to_client(client_id, response)


    def forward_response_to_client(self, client_id, response):
        client_conn = self.active_clients.get(client_id)
        if client_conn:
            try:
                client_conn.sendall(response.encode('utf-8'))
            except Exception as e:
                print(f"Error sending response to client: {e}")
                # Handle disconnection, etc.
        else:
            print(f"No active connection found for client ID {client_id}")



    def generate_unique_client_id(self, addr):
        # Simple implementation: Use the address as the identifier
        return f"{addr[0]}:{addr[1]}"


    def handle_client_disconnection(self, client_id):
        if client_id in self.active_clients:
            del self.active_clients[client_id]