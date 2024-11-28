import networkx as nx
import community as community_louvain  # This is the python-louvain package
from collections import defaultdict

def add_community_branch(citation_network_clean):
    """
    Adds a 'branch' attribute to the nodes of the directed citation network
    based on community detection using the Louvain algorithm.
    The branches are numbered such that branch 1 is the largest community,
    branch 2 is the second largest, and so on.

    Parameters:
    ----------
    citation_network_clean : networkx.DiGraph
        Directed graph representing the cleaned citation network.

    Returns:
    -------
    citation_network_with_branch : networkx.DiGraph
        The directed graph with the 'branch' node attribute added.
    """
    # Create an undirected copy of the directed graph
    undirected_graph = citation_network_clean.to_undirected()

    # Compute the best partition using the Louvain algorithm with random_state=0
    # To ensure deterministic results, set random_state to a fixed integer
    partition = community_louvain.best_partition(undirected_graph, random_state=0)

    # Invert the partition to get community_id -> list of nodes
    community_to_nodes = defaultdict(list)
    for node, community_id in partition.items():
        community_to_nodes[community_id].append(node)

    # Get the sizes of the communities
    community_sizes = {community_id: len(nodes) for community_id, nodes in community_to_nodes.items()}

    # Sort the communities by size, largest first
    sorted_communities = sorted(community_sizes.items(), key=lambda x: x[1], reverse=True)

    # Create a mapping from old community IDs to new branch IDs starting from 1
    old_to_new_community_id = {}
    for new_id, (old_id, size) in enumerate(sorted_communities, start=1):
        old_to_new_community_id[old_id] = new_id

    # Create a new partition with updated community IDs
    new_partition = {node: old_to_new_community_id[community_id] for node, community_id in partition.items()}

    # Add the 'branch' attribute to the original directed graph
    nx.set_node_attributes(citation_network_clean, new_partition, 'branch')

    return citation_network_clean  # Return the graph with the new node attributes
