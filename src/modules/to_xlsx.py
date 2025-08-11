import os
import pandas as pd

def export_csvs_as_excel(ruta_directorio):
    """
    Combines multiple CSV files into a single Excel file, with each CSV file
    being saved as a separate sheet in the Excel workbook.
    """
    # Name Expected names of CSV files
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

    # Create the full path for the output Excel file
    ruta_salida = os.path.join(ruta_directorio, "data.xlsx")

    with pd.ExcelWriter(ruta_salida, engine="openpyxl") as writer:
        for nombre in nombres_archivos:

            ruta_csv = os.path.join(ruta_directorio, nombre)
            if os.path.exists(ruta_csv):
                df = pd.read_csv(ruta_csv)
                nombre_hoja = os.path.splitext(nombre)[0]  # remove extension
                df.to_excel(writer, sheet_name=nombre_hoja, index=False)
            else:
                print(f"Archivo no encontrado: {ruta_csv}")

    print(f"Archivo Excel creado: {ruta_salida}")

if __name__ == "__main__":
    ruta=r""
    export_csvs_as_excel(ruta)
