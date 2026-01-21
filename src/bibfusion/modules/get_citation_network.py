import pandas as pd
import networkx as nx

def get_citation_network(scopus_citation):
    """
    Creates a directed citation network graph from scopus_citation DataFrame.
    
    Parameters:
    ----------
    scopus_citation : pd.DataFrame
        DataFrame containing at least two columns: 'SR' and 'SR_ref'.
    
    Returns:
    -------
    G : networkx.DiGraph
        Directed graph representing the citation network.
    """
    # Remove rows with missing 'SR' or 'SR_ref' values
    scopus_citation_clean = scopus_citation.dropna(subset=['SR', 'SR_ref'])

    # Remove self-loops where 'SR' == 'SR_ref' (if desired)
    scopus_citation_clean = scopus_citation_clean[scopus_citation_clean['SR'] != scopus_citation_clean['SR_ref']]

    # Remove duplicate edges
    scopus_citation_clean = scopus_citation_clean.drop_duplicates(subset=['SR', 'SR_ref'])

    # Create a directed graph
    G = nx.DiGraph()

    # Add edges to the graph
    edge_list = list(zip(scopus_citation_clean['SR'], scopus_citation_clean['SR_ref']))
    G.add_edges_from(edge_list)

    return G
