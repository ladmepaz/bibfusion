import os
import pandas as pd
import re
import csv

# We compile the regular expression only once for efficiency
ILLEGAL_CHARS_RE = re.compile(r'[\x00-\x08\x0B-\x0C\x0E-\x1F]')

def remove_illegal_chars_series(series: pd.Series) -> pd.Series:
    """
    Cleans illegal characters in a pandas Series using .str.replace,
    which is more efficient and safer than applymap.
    """

    if series.dtype == "object" or pd.api.types.is_string_dtype(series):
        return series.astype(str).str.replace(ILLEGAL_CHARS_RE, "", regex=True)
    return series

def export_csvs_as_excel(ruta_directorio):
    """
    Combines multiple CSV files into a single Excel file,
    each CSV in a separate sheet. Cleans illegal characters before exporting.
    """

    csv_file_names = [
        "Article.csv",
        "scimagodb.csv",
        "ArticleAuthor.csv",
        "journal.csv",
        "Author.csv",
        "Affiliation.csv",
        "Citation.csv",
        "TreeOfScience.csv"
    ]

    output_excel_path = os.path.join(ruta_directorio, "data.xlsx")

    with pd.ExcelWriter(output_excel_path, engine="openpyxl") as writer:
        for csv_file_name in csv_file_names:
            csv.field_size_limit(10**7)  # Increase the limit to 10 million characters
            csv_file_path = os.path.join(ruta_directorio, csv_file_name)
            if os.path.exists(csv_file_path):
                df = pd.read_csv(csv_file_path, sep= ",", engine='python')

                # Clean illegal characters in all string columns
                for col in df.columns:
                    df[col] = remove_illegal_chars_series(df[col])

                sheet_name = os.path.splitext(csv_file_name)[0]  # Remove .csv extension
                df.to_excel(writer, sheet_name=sheet_name, index=False)
            else:
                print(f"File not found: {csv_file_path}")

    print(f"Excel file created: {output_excel_path}")

if __name__ == "__main__":
    ruta = r""
    export_csvs_as_excel(ruta)
