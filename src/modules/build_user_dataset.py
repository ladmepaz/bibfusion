import os
import pandas as pd

from .to_xlsx import remove_illegal_chars_series


def build_user_dataset_from_all(all_dir: str, out_path: str = None) -> str:
    """
    Create a user-friendly Excel from consolidated All_* CSVs.

    Phase 1 (initial):
    - Read `All_Articles.csv` from `all_dir` and export to a single-sheet Excel
      named 'wos_scopus'.

    Parameters
    - all_dir: directory containing All_Articles.csv (e.g., 'all_data_wos_scopus')
    - out_path: optional explicit path for output .xlsx. If None, writes
      `UserDataset.xlsx` under `all_dir`.

    Returns the path to the generated Excel file.
    """

    if out_path is None:
        out_path = os.path.join(all_dir, "UserDataset.xlsx")

    articles_csv = os.path.join(all_dir, "All_Articles.csv")
    if not os.path.exists(articles_csv):
        raise FileNotFoundError(f"All_Articles.csv not found in {all_dir}")

    df = pd.read_csv(articles_csv)

    # Clean illegal characters per column to avoid Excel write errors
    for col in df.columns:
        df[col] = remove_illegal_chars_series(df[col])

    # Write Excel with a single sheet 'wos_scopus'
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="wos_scopus", index=False)

    return out_path

