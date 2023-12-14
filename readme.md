Introduction

This documentation provides guidelines on managing nodes within our distributed system. It covers the initial setup of nodes and the process of adding new nodes to the system. Proper management of nodes is crucial for seamless CRUD (Create, Read, Update, Delete) operations.

1. Starting Nodes
    Before performing any CRUD operations, it's essential to start all the nodes listed in the nodes dictionary within the Node class. Each node in the system needs to be initialized and running to handle requests and data replication.

    Make sure that each node is alive ('isAlive': True)


2. Adding New Nodes to the system
    1. Modify the "nodes" Dictionary in the Node Class
    2. Modify the "nodes" Array in MessageHandler
    3. Start nodes
