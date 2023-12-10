import socket

def send_request_to_coordinator(data, coordinator_host, coordinator_port):
    """
    Sends a request to the coordinator and prints the response.

    :param data: The data to send (e.g., a search key).
    :param coordinator_host: The IP address or hostname of the coordinator.
    :param coordinator_port: The port number on which the coordinator is listening.
    """
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
            # Connect to the coordinator server
            client_socket.connect((coordinator_host, coordinator_port))
            print(f"Connected to Coordinator at {coordinator_host}:{coordinator_port}")

            # Send the request
            client_socket.sendall(data.encode())
            print(f"Sent request: {data}")

            # Wait for and print the response
            response = client_socket.recv(1024)
            print(f"Response from coordinator: {response.decode()}")
    except ConnectionRefusedError:
        print("Failed to connect to the coordinator. Is it running and accessible?")
    except Exception as e:
        print(f"An error occurred: {e}")

# Example usage
coordinator_host = "localhost"  # Change to the actual host of your coordinator
coordinator_port = 7056        # Change to the actual port of your coordinator
search_key = "1"    # Example search key

send_request_to_coordinator(search_key, coordinator_host, coordinator_port)
