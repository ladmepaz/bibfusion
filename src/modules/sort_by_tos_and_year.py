import re
import pandas as pd

def sort_by_tos_and_year(df):
    """
    Ordena el DataFrame por 'tos' y luego por 'year'.
    Orden de 'tos': root -> trunk -> branch_1 -> branch_2 -> ...
    """
    def tos_sort_key(tos_value):
        if tos_value == "root":
            return (0, 0)  # root primero
        elif tos_value == "trunk":
            return (1, 0)  # trunk después
        elif isinstance(tos_value, str) and tos_value.startswith("branch_"):
            branch_num = int(re.search(r"branch_(\d+)", tos_value).group(1))
            return (2, branch_num)  # luego branch_n en orden numérico
        else:
            return (3, 0)  # lo que no encaje va al final
    
    df_sorted = df.copy()
    df_sorted["tos_order"] = df_sorted["tos"].apply(tos_sort_key)
    
    # Ordena primero por el criterio tos_order, luego por año
    df_sorted = df_sorted.sort_values(by=["tos_order", "year"], ascending=[True, True])
    
    # Quita la columna auxiliar
    df_sorted = df_sorted.drop(columns=["tos_order"])

    df_sorted = df_sorted[df_sorted['doi'].notna() & (df_sorted['doi'].str.strip() != "")]

    return df_sorted