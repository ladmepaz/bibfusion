import pandas as pd
import networkx as nx

def graph_to_df(G, df_article=None):
    """
    Converts a NetworkX graph into DataFrames and optionally adds title and author to df_nodes.

    Parameters:
        G (networkx.Graph): NetworkX graph
        df_article (pd.DataFrame, optional): DataFrame containing article data
            that must include the columns ['SR', 'title', 'author']

    Returns:
        tuple: (df_nodes, df_edges, df_tos)
    """

    # --------- Calculate degree of each node ---------
    degree_dict = dict(G.degree())

    # --------- DataFrame of nodes ---------
    nodes_data = []
    for node, attrs in G.nodes(data=True):
        node_info = {
            "id": node,
            "Label": str(node),
            "Subfield": attrs.get("branch"),
            "tos": attrs.get("tos"),
            "degree": degree_dict.get(node, 0)
        }
        nodes_data.append(node_info)
    df_nodes = pd.DataFrame(nodes_data)

    # --------- DataFrame of edges ---------
    edges_data = []
    for u, v, attrs in G.edges(data=True):
        edges_data.append({
            "Source": u,
            "Target": v,
            "weight": attrs.get('weight', 1)
        })
    
    df_edges = pd.DataFrame(edges_data)
    
    # --------- DataFrame of Id and tos ---------
    df_tos = df_nodes[["id", "tos"]].copy()
    df_tos.rename(columns={"id": "SR"}, inplace=True)

    # --------- ADD TITLE AND AUTHOR TO df_nodes ---------
    if df_article is not None:
        # Check that df_article has the necessary columns
        if all(col in df_article.columns for col in ['SR', 'title', 'author']):
            # Merge only with the needed columns
            df_nodes = pd.merge(
                df_nodes,
                df_article[['SR', 'title', 'author']],
                left_on='id',
                right_on='SR',
                how='left'
            )
            # Remove the duplicate SR column
            df_nodes = df_nodes.drop('SR', axis=1)
        else:
            print("Warning: df_article does not have the columns 'SR', 'title', and 'author'")

    return df_nodes, df_edges, df_tos