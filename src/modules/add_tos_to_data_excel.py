import os
import pandas as pd

def add_tos_to_data_excel(folder_path):
    """
    Finds 'data.xlsx' and 'tos_df.csv' inside a folder and 
    adds the CSV content as a new sheet in the Excel file.

    Parameters:
    - folder_path: str, path to the folder containing 'data.xlsx' and 'tos_df.csv'

    Returns:
    - None (overwrites the Excel file with the new sheet added)
    """
    excel_path = os.path.join(folder_path, "data.xlsx")
    csv_path = os.path.join(folder_path, "TreeOfScience.csv")

    if not os.path.exists(excel_path):
        raise FileNotFoundError(f"'data.xlsx' not found in {folder_path}")
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"'TreeOfScience.csv' not found in {folder_path}")

    # Read CSV
    df_tos = pd.read_csv(csv_path)

    # Append CSV as new sheet to Excel
    with pd.ExcelWriter(excel_path, mode="a", engine="openpyxl", if_sheet_exists="replace") as writer:
        df_tos.to_excel(writer, sheet_name="TreeOfScience", index=False)

    print(f"'TreeOfScience' sheet added to: {excel_path}")
