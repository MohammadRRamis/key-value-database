import socket
import threading

from HashRing import hashRing, create, read, delete

class Network:
    def __init__(self, Node, hostname, port):
        self.Node = Node  # Reference to the Node instance
        self.hostname = hostname #ip address of the server
        self.port = port #port of the server


    def set_instances(self, MessageHandler, Election):
        self.MessageHandler = MessageHandler
        self.Election = Election

    def run_server(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.hostname, self.port))
            server_socket.listen()
            while True:
                conn, addr = server_socket.accept()
                threading.Thread(target=self.handle_client, args=(conn,)).start()

    def process_client_request(self, data, command):
        parts = data.split()
        key = parts[1]
        target_node = self.Node.hr.get_node(key)
        
        #if the coordiantor does no have the key in his database
        #send a request to the target_node to get the value
        #then send the value to the client
        if (self.Node.nodename != target_node):
            #Format:  COMMAND CREATE KEY VALUE
            message = f"COMMAND {data}"
            
            response = self.send_request_wait_for_response(target_node, message)
            print(response)
        else:
            if command == 'CREATE':
                value = parts[2]
                create(target_node, key, value)
            elif command == 'read':
                read(target_node, key)
            elif command == 'delete':
            # Logic for handling 'delete' command
                pass

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

    #both methods are same, i need to remove one of them------------------------------
    def send_message_to_node(self, node_name, message):
        node_info = self.Node.nodes[node_name]
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((node_info['hostname'], node_info['port']))
                sock.sendall(f"{message} {self.Node.node_id}".encode())
        except ConnectionRefusedError:
            # print(f"Unable to connect to node {node_name} at {node_info['hostname']}:{node_info['port']}")
            pass



    def send_request_wait_for_response(self, target_node, message):
        node_info = self.Node.nodes[target_node]
        
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((node_info['hostname'], node_info['port']))
                sock.sendall(message.encode('utf-8'))
                
                # Wait for the response from the target node
                response = sock.recv(1024)
                return response.decode('utf-8')
        except ConnectionError as e:
            return f"Error connecting to node {target_node}: {e}"
        

    def handle_client(self, conn):
        with conn:
            while True:
                data = conn.recv(2024)
                if not data:
                    break
                data = data.decode()
                parts = data.split()

                if parts[0] in ['CREATE', 'READ', 'UPDATE', 'DELETE']:
                    
                    #Example of request: CREATE KEY VALUE
                    command = parts[0]

                    if self.Election.coordinator:
                        # Handle the command (CREATE, READ, DELETE) directly
                        self.process_client_request(data, command)
                    else:
                        # Redirect client to the coordinator
                        self.redirect_to_coordinator(conn, data)
                
                #if the request comes from other node in the system
                else: 
                    if len(parts) <= 3:
                        message = parts[0]
                        node_id = int(parts[1])

                    #if the message is a command 
                    #COMMAND CREATE KEY VALUE
                    elif len(parts) == 4:
                        # operation = parts[1]
                        # key = parts[2]
                        # value = parts[3]
                        self.MessageHandler.handle_command(parts)
                        continue

                    # Don't print the heartbeat message, because it will do it forever
                    if message != "HEARTBEAT" and message != "UPDATE":
                        print(f"Received {message} from Node {node_id}")

                    self.MessageHandler.handle_messages(message, node_id, parts)