import networkx as nx

def get_tos(citation_network_with_branch, max_roots=20, max_trunk=20):
    """
    Identifies roots and trunks in the citation network graph and assigns the 'tos' attribute to nodes.

    The 'tos' attribute will have values:
    - 'root' for root nodes
    - 'trunk' for trunk nodes
    - 'branch_1', 'branch_2', etc., based on the 'branch' attribute

    Parameters:
    ----------
    citation_network_with_branch : networkx.DiGraph
        Directed graph representing the citation network with 'branch' node attribute.

    max_roots : int, optional
        Maximum number of roots to identify. Default is 20.

    max_trunk : int, optional
        Maximum number of trunk nodes to identify. Default is 20.

    Returns:
    -------
    citation_network_with_tos : networkx.DiGraph
        The directed graph with the 'tos' node attribute added.
    """

    # Create a copy of the graph to avoid modifying the original
    G = citation_network_with_branch.copy()

    # Step 1: Identify roots
    # Roots are nodes with out-degree zero (they do not cite any other papers)
    valid_roots = [
        (node, G.in_degree(node))
        for node in G.nodes()
        if G.out_degree(node) == 0
    ]
    # Sort roots by in-degree (number of citations received)
    sorted_roots = sorted(valid_roots, key=lambda x: x[1], reverse=True)[:max_roots]

    # Assign 'root' attribute
    nx.set_node_attributes(G, 0, 'root')
    for node, degree in sorted_roots:
        G.nodes[node]['root'] = degree

    # Step 2: Compute sap values without considering leaves
    # Initialize sap and root connections
    nx.set_node_attributes(G, 0, '_sap')
    nx.set_node_attributes(G, 0, '_root_connections')
    for node in G.nodes():
        if G.nodes[node]['root'] > 0:
            G.nodes[node]['_sap'] = G.nodes[node]['root']
            G.nodes[node]['_root_connections'] = 1

    # Propagate sap values up the graph
    try:
        topological_order = list(nx.topological_sort(G))
    except nx.NetworkXUnfeasible:
        # If the graph is not acyclic, create a DAG by ignoring cycles
        G = nx.DiGraph(nx.DiGraph(G).reverse())  # Reverse the graph
        G.remove_edges_from(nx.selfloop_edges(G))
        topological_order = list(nx.topological_sort(G))

    for node in reversed(topological_order):
        successors = list(G.successors(node))
        if successors:
            G.nodes[node]['_sap'] = sum(G.nodes[nb]['_sap'] for nb in successors)
            G.nodes[node]['_root_connections'] = sum(G.nodes[nb]['_root_connections'] for nb in successors)

    # Step 3: Identify trunks
    potential_trunk = [
        (node, G.nodes[node]['_sap'])
        for node in G.nodes()
        if G.nodes[node]['root'] == 0 and G.nodes[node]['_sap'] > 0
    ]
    # Sort trunks by sap value
    sorted_trunks = sorted(potential_trunk, key=lambda x: x[1], reverse=True)[:max_trunk]

    # Assign 'trunk' attribute
    nx.set_node_attributes(G, 0, 'trunk')
    for node, sap_value in sorted_trunks:
        G.nodes[node]['trunk'] = sap_value

    # Step 4: Assign 'tos' attribute based on 'root', 'trunk', and 'branch' attributes
    for node in G.nodes():
        if G.nodes[node].get('root', 0) > 0:
            G.nodes[node]['tos'] = 'root'
        elif G.nodes[node].get('trunk', 0) > 0:
            G.nodes[node]['tos'] = 'trunk'
        elif G.nodes[node].get('branch', 0) > 0:
            G.nodes[node]['tos'] = 'branch_' + str(G.nodes[node]['branch'])
        else:
            G.nodes[node]['tos'] = None  # Nodes not classified as root, trunk, or branch

    return G
