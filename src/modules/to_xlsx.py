import os
import pandas as pd
import re

def remove_illegal_chars(value):
    if isinstance(value, str):
        # Quita caracteres de control ASCII exceptuando tab(0x09), LF(0x0A) y CR(0x0D)
        return re.sub(r'[\x00-\x08\x0B-\x0C\x0E-\x1F]', '', value)
    return value

def export_csvs_as_excel(ruta_directorio):
    """
    Combina múltiples archivos CSV en un solo Excel, 
    cada CSV en una hoja distinta.
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
            ruta_csv = os.path.join(ruta_directorio, nombre)
            if os.path.exists(ruta_csv):
                df = pd.read_csv(ruta_csv)

                # Limpiar caracteres ilegales
                df = df.applymap(remove_illegal_chars)

                nombre_hoja = os.path.splitext(nombre)[0]  # sin extensión
                df.to_excel(writer, sheet_name=nombre_hoja, index=False)
            else:
                print(f"Archivo no encontrado: {ruta_csv}")

    print(f"Archivo Excel creado: {ruta_salida}")

if __name__ == "__main__":
    ruta = r""
    export_csvs_as_excel(ruta)
