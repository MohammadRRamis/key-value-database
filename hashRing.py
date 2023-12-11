from uhashring import HashRing



def hashRing(nodes):
    hr = HashRing(nodes)
    return hr

# def ring():
#     return hr

# def nodesInfo():
#     return nodes


"""
In case of a failure, the keys-values in the failed nodes needs to be 
distributed to other nodes in the ring using the "replication" of the failed node
"""
def removeNode(nodename, nodes):
    # Create a temporary HashRing without the node being removed
    temp_nodes = nodes.copy()
    del temp_nodes[nodename]
    temp_hr = HashRing(temp_nodes)

    try:
        replica = nodes[nodename].get('replica')
        keys = replica.keys()
        for key in keys:
            value = replica.get(key)
            new_node_name = temp_hr.get_node(key)
            if new_node_name != nodename:
                nodes[new_node_name].get('instance').set(key,value)
    except AttributeError:
        print("their are not keys in this node")

    nodes[nodename]['isAlive'] = False
    hr.remove_node(nodename)

    print(f"Node {nodename} removed successfully.")

# def addNode(nodename, nodes):
#     if nodename in nodes and nodes[nodename]["isAlive"] == False:
#         nodes[nodename]["isAlive"] = True
#         hr.add_node(nodename)

#     # temp_nodes = nodes.copy()
#     # add temp_nodes[nodename]
#     # temp_hr = HashRing(temp_nodes)

#     # Identify the node which might have keys to be redistributed to the new node
#     # For simplicity, we're just checking the next node in the ring
#     # In a more complex setup, you might need a more sophisticated method to identify this node
#     # Determine the next node in the ring
#     next_node = getNodeAfter(hr, nodename)
#     if next_node:
#         addNodeRedistributeData(next_node, nodename, hr)

#     print(f"{nodename} added and data redistributed successfully.")


# def addNodeRedistributeData(source_node, target_node, hr, nodes):
#     """
#     Move data from the source node to the target node based on the updated hash ring.
#     """
#     source_instance = nodes[source_node]['instance']
#     keys = source_instance.keys()

#     for key in keys:
#         # Check if the key should be moved to the target node
#         correct_node = hr.get_node(key)
#         if correct_node == target_node:
#             value = source_instance.get(key)
#             nodes[target_node].get('instance').set(key,value)
#             # nodes[target_node]['instance'].set(key, value)
#             source_instance.delete(key)



# def getNodeAfter(hr, nodename): 
#     """
#     Get the next node in the hash ring after the specified node.
#     """
#     sorted_nodes = sorted(hr.get_nodes())
#     current_index = sorted_nodes.index(nodename)

#     # the mod (%) is used to "wrap around" if the index goes beyond the length of the list. 
#     next_index = (current_index + 1) % len(sorted_nodes)
#     return sorted_nodes[next_index]








