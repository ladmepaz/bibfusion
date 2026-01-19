import re
import pandas as pd

def sort_by_tos_and_year(df):
    """
    Sorts the DataFrame by 'tos' and then by 'year'.
    'tos' order: root -> trunk -> branch_1 -> branch_2 -> ...
    """
    def tos_sort_key(tos_value):
        if tos_value == "root":
            return (0, 0)  # root first
        elif tos_value == "trunk":
            return (1, 0)  # trunk second
        elif isinstance(tos_value, str) and tos_value.startswith("branch_"):
            branch_num = int(re.search(r"branch_(\d+)", tos_value).group(1))
            return (2, branch_num)  # then branch_n in numerical order
        else:
            return (3, 0)  # anything else goes last
    
    df_sorted = df.copy()
    df_sorted["tos_order"] = df_sorted["tos"].apply(tos_sort_key)
    
    # Sort first by tos_order, then by year
    df_sorted = df_sorted.sort_values(by=["tos_order", "year"], ascending=[True, True])
    
    # Remove the auxiliary column
    df_sorted = df_sorted.drop(columns=["tos_order"])

    df_sorted = df_sorted[df_sorted['doi'].notna() & (df_sorted['doi'].str.strip() != "")]

    return df_sorted