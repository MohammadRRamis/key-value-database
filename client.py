import socket


def send_request(server_host, server_port, request):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((server_host, server_port))
        sock.sendall(request.encode('utf-8'))
        response = sock.recv(1024).decode('utf-8')
        return response

def create(server_host, server_port, key, value):
    return send_request(server_host, server_port, f"CREATE {key} {value}")

def read(server_host, server_port, key):
    return send_request(server_host, server_port, f"READ {key}")

def update(server_host, server_port, key, value):
    return send_request(server_host, server_port, f"UPDATE {key} {value}")

def delete(server_host, server_port, key):
    return send_request(server_host, server_port, f"DELETE {key}")



def run_client(server_host, server_port):
    while True:
        user_input = input("Enter command (CREATE, READ, UPDATE, DELETE) and arguments, or 'exit' to quit: ")
        if user_input.lower() == 'exit':
            break

        parts = user_input.split()
        if len(parts) < 2:
            print("Invalid command format.")
            continue

        command, key = parts[0], parts[1]
        value = parts[2] if len(parts) > 2 else None

        if command.upper() == 'CREATE' and value:
            print(create(server_host, server_port, key, value))
        elif command.upper() == 'READ':
            print(read(server_host, server_port, key))
        elif command.upper() == 'UPDATE' and value:
            print(update(server_host, server_port, key, value))
        elif command.upper() == 'DELETE':
            print(delete(server_host, server_port, key))
        else:
            print("Unknown command or missing value.")

           
if __name__ == "__main__":
    SERVER_HOST = 'localhost'  # Replace with actual server host
    SERVER_PORT = 5003       # Replace with actual server port
    run_client(SERVER_HOST, SERVER_PORT)
