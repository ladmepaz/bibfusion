import pandas as pd
import networkx as nx

def graph_to_df(G, df_article=None):
    """
    Convierte un grafo NetworkX en DataFrames y opcionalmente agrega title y author al df_nodos.
    
    Parámetros:
        G (networkx.Graph): Grafo de NetworkX
        df_article (pd.DataFrame, opcional): DataFrame con datos de artículos 
            que debe contener columnas ['SR', 'title', 'author']
    
    Retorna:
        tuple: (df_nodos, df_aristas, df_tos)
    """
    # --------- Calcular degree de cada nodo ---------
    degree_dict = dict(G.degree())

    # --------- DataFrame de nodos ---------
    nodos_data = []
    for node, attrs in G.nodes(data=True):
        nodo_info = {
            "id": node,
            "Label": str(node),
            "Subfield": attrs.get("branch"),
            "tos": attrs.get("tos"),
            "degree": degree_dict.get(node, 0)
        }
        nodos_data.append(nodo_info)
    df_nodos = pd.DataFrame(nodos_data)

    # --------- DataFrame de aristas ---------
    aristas_data = []
    for u, v, attrs in G.edges(data=True):
        aristas_data.append({
            "Source": u,
            "Target": v,
            "weight": attrs.get('weight', 1)
        })
    
    df_aristas = pd.DataFrame(aristas_data)
    
    # --------- DataFrame de Id y tos ---------
    df_tos = df_nodos[["id", "tos"]].copy()
    df_tos.rename(columns={"id": "SR"}, inplace=True)

    # --------- AGREGAR TITLE Y AUTHOR AL df_nodos ---------
    if df_article is not None:
        # Verificar que df_article tiene las columnas necesarias
        if all(col in df_article.columns for col in ['SR', 'title', 'author']):
            # Hacer merge solo con las columnas needed
            df_nodos = pd.merge(
                df_nodos,
                df_article[['SR', 'title', 'author']],
                left_on='id',
                right_on='SR',
                how='left'
            )
            # Eliminar la columna SR duplicada
            df_nodos = df_nodos.drop('SR', axis=1)
        else:
            print("Advertencia: df_article no tiene las columnas 'SR', 'title' y 'author'")

    return df_nodos, df_aristas, df_tos