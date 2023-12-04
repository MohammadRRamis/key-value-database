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
            parts = request.split(':')
            action = parts[0].upper()
            key = parts[1]

            if action == 'CREATE':
                value = parts[2]
                self.data[key] = value
                self.save_data()
                response = f'SAVED {key}:{value}'
            elif action == 'READ':
                response = self.data.get(key, 'NOT FOUND')
            elif action == 'UPDATE':
                value = parts[2]
                if key in self.data:
                    self.data[key] = value
                    self.save_data()
                    response = f'UPDATED {key}:{value}'
                else:
                    response = 'KEY NOT FOUND'
            elif action == 'DELETE':
                if key in self.data:
                    del self.data[key]
                    self.save_data()
                    response = f'DELETED {key}'
                else:
                    response = 'KEY NOT FOUND'
            else:
                response = 'INVALID COMMAND'

            client_socket.sendall(response.encode('utf-8'))

    def start(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.host, self.port))
            s.listen()
            while True:
                client_socket, addr = s.accept()
                client_thread = threading.Thread(target=self.handle_client, args=(client_socket,))
                client_thread.start()

# Example usage:
if __name__ == "__main__":
    server = NodeServer('localhost', 12345, 'data.json')
    server.start()
