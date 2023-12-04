import socket
import json
import threading

class NodeServer:
    def __init__(self, host, port, storage_file):
        self.host = host
        self.port = port
        self.storage_file = storage_file
        self.data = self.load_data()
        self.lock = threading.Lock()

    def load_data(self):
        try:
            with open(self.storage_file, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            return {}

    def save_data(self):
        with self.lock:
            with open(self.storage_file, 'w') as f:
                json.dump(self.data, f)

    def handle_client(self, client_socket):
        with client_socket:
            request = client_socket.recv(1024).decode('utf-8')
            key, value = request.split(':')
            if value:
                self.data[key] = value
                self.save_data()
                response = f'SAVED {key}:{value}'
            else:
                response = self.data.get(key, 'NOT FOUND')
            client_socket.sendall(response.encode('utf-8'))

    def start(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.host, self.port))
            s.listen()
            while True:
                client_socket, addr = s.accept()
                client_thread = threading.Thread(target=self.handle_client, args=(client_socket,))
                client_thread.start()
