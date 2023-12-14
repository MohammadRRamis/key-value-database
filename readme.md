Introduction

This documentation provides guidelines on managing nodes within our distributed system. It covers the initial setup of nodes and the process of adding new nodes to the system. Proper management of nodes is crucial for seamless CRUD (Create, Read, Update, Delete) operations.

1. Starting Nodes
    Before performing any CRUD operations, it's essential to start all the nodes listed in the nodes dictionary within the Node class. Each node in the system needs to be initialized and running to handle requests and data replication.

    Make sure that each node is alive ('isAlive': True)


2. Adding New Nodes to the system
    1. Modify the "nodes" Dictionary in the Node Class
    2. Modify the "nodes" Array in MessageHandler
    3. Start nodes


3. The way Client Connection Works
    1. Connect to a node.
    2.  Send the "CLIENT_REQUEST_COORDINATOR_INFO" message.
        Wait for a response.
    3. Interpret the response:
        1. If the response indicates a different node as the coordinator, disconnect and connect to that node.
        2. If the response indicates the current node is the coordinator, continue with the current connection.
