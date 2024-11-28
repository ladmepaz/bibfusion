import networkx as nx

def clean_citation_network(citation_network):
    """
    Cleans the citation network by extracting the giant component and removing nodes
    with in-degree one and out-degree zero.
    
    Parameters:
    ----------
    citation_network : networkx.DiGraph
        Directed graph representing the citation network.
    
    Returns:
    -------
    cleaned_network : networkx.DiGraph
        Cleaned directed graph after extracting the giant component and removing specified nodes.
    """
    # Step 1: Extract the largest weakly connected component
    largest_weakly_cc = max(nx.weakly_connected_components(citation_network), key=len)
    
    # Create a subgraph of the giant component
    giant_component = citation_network.subgraph(largest_weakly_cc).copy()
    
    # Step 2: Remove nodes with in-degree one and out-degree zero
    nodes_to_remove = [node for node in giant_component.nodes()
                       if giant_component.in_degree(node) == 1 and giant_component.out_degree(node) == 0]
    
    giant_component.remove_nodes_from(nodes_to_remove)
    
    return giant_component
