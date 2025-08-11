import pandas as pd
import networkx as nx

def graph_to_df(G):
    """
    Convierte un grafo NetworkX en:
      - df_nodos: tabla con todos los atributos de cada nodo (formato Gephi)
      - df_aristas: tabla con todos los atributos de cada arista (formato Gephi)
      - df_tos: Id y tos de cada nodo
    Listo para exportar a CSV y cargar en Gephi.
    """
    # --------- DataFrame de nodos (formato Gephi) ---------
    nodos_data = []
    for node, attrs in G.nodes(data=True):
        nodo_info = {"Id": node, "Label": str(node)}  # Gephi necesita Id y Label
        nodo_info.update(attrs)  # Agregar todos los atributos del nodo
        nodos_data.append(nodo_info)
    df_nodos = pd.DataFrame(nodos_data)

    # --------- DataFrame de aristas (formato Gephi) ---------
    aristas_data = []
    for source, target, attrs in G.edges(data=True):
        arista_info = {"Source": source, "Target": target}
        arista_info.update(attrs)  # Agregar todos los atributos de la arista
        aristas_data.append(arista_info)
    df_aristas = pd.DataFrame(aristas_data)

    # --------- DataFrame de Id y tos ---------
    df_tos = df_nodos[["Id", "tos"]].copy()
    df_tos.rename(columns={"Id": "SR"}, inplace=True)



    return df_nodos, df_aristas, df_tos









# def graph_to_df(G):
#     """
#     Convierte un grafo en dos DataFrames: nodos y aristas.
#     Retorna: (df_nodos, df_aristas)
#     """
#     # DataFrame de nodos
#     nodos_data = []
#     for node, attrs in G.nodes(data=True):
#         fila = {"node": node}
#         fila.update(attrs)  # agrega todos los atributos del nodo
#         nodos_data.append(fila)
#     df_nodos = pd.DataFrame(nodos_data)

#     # DataFrame de aristas
#     aristas_data = []
#     for source, target, attrs in G.edges(data=True):
#         fila = {"source": source, "target": target}
#         fila.update(attrs)  # agrega todos los atributos de la arista
#         aristas_data.append(fila)
#     df_aristas = pd.DataFrame(aristas_data)

#     return df_nodos, df_aristas


# Ejemplo de uso
# df_nodos, df_aristas = graph_to_dfs(G)

# print("NODOS:")
# print(df_nodos.head())

# print("\nARISTAS:")
# print(df_aristas.head())

# def graph_to_df(G):
#     """
#     Convierte un grafo con atributos en un DataFrame con columnas:
#     ['node', 'tos', 'root', 'trunk', 'branch']
#     """
#     data = []
#     for node, attrs in G.nodes(data=True):
#         data.append({
#             "node": node,  # identificador o nombre del nodo
#             "tos": attrs.get("tos"),
#             "root": attrs.get("root", 0),
#             "trunk": attrs.get("trunk", 0),
#             "branch": attrs.get("branch", None)
#         })
#     return pd.DataFrame(data)

# # Ejemplo de uso
# df_nodos = graph_to_df(G)
# print(df_nodos.head())
