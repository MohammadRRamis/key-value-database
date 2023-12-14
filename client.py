import socket


def create(sock, args):
    # Implementation of create function
    # Example: send a request to the coordinator
    message = f"CREATE {args}"
    sock.sendall(message.encode())

def read(sock, args):
    # Implementation of read function
    message = f"READ {args}"
    sock.sendall(message.encode())

def update(sock, args):
    # Implementation of update function
    message = f"UPDATE {args}"
    sock.sendall(message.encode())

def delete(sock, args):
    # Implementation of delete function
    message = f"DELETE {args}"
    sock.sendall(message.encode())


def process_command(sock, command, args):
    if command == "CREATE":
        create(sock, args)
    elif command == "READ":
        read(sock, args)
    elif command == "UPDATE":
        update(sock, args)
    elif command == "DELETE":
        delete(sock, args)
    else:
        print("Invalid command.")
        return

        # Wait for response from the server
    response = sock.recv(1024).decode()
    print("Response:", response)


def main():
    coordinator_host = 'localhost'  # Replace with actual host
    coordinator_port = 5010        # Replace with actual port

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((coordinator_host, coordinator_port))

    print("Client started. Type 'exit' to quit.")
    while True:
        user_input = input("Enter command (CREATE, READ, UPDATE, DELETE) and arguments, or 'exit' to quit: ")
        if user_input.lower() == 'exit':
            break

        parts = user_input.split(maxsplit=1)
        if len(parts) == 2:
            command, args = parts
            process_command(sock, command.upper(), args)
        else:
            print("Invalid input format.")

    sock.close()

if __name__ == "__main__":
    main()