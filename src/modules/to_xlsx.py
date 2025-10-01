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

    nombres_archivos = [
        "Article.csv",
        "scimagodb.csv",
        "ArticleAuthor.csv",
        "journal.csv",
        "Author.csv",
        "Affiliation.csv",
        "Citation.csv",
        "TreeOfScience.csv"
    ]

    ruta_salida = os.path.join(ruta_directorio, "data.xlsx")

    with pd.ExcelWriter(ruta_salida, engine="openpyxl") as writer:
        for nombre in nombres_archivos:
            csv.field_size_limit(10**7)  # ncrease the limit to 10 million characters
            ruta_csv = os.path.join(ruta_directorio, nombre)
            if os.path.exists(ruta_csv):
                df = pd.read_csv(ruta_csv, sep= ",", engine='python')

                # Clean illegal characters in all string columns
                for col in df.columns:
                    df[col] = remove_illegal_chars_series(df[col])

                nombre_hoja = os.path.splitext(nombre)[0]  # Remove .csv extension
                df.to_excel(writer, sheet_name=nombre_hoja, index=False)
            else:
                print(f"File not found: {ruta_csv}")

    print(f"Excel file created: {ruta_salida}")

if __name__ == "__main__":
    ruta = r""
    export_csvs_as_excel(ruta)
