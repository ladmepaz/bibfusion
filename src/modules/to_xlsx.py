import os
import pandas as pd

def combinar_csv_a_excel(ruta_directorio):
    # Nombres esperados de archivos CSV
    nombres_archivos = [
        "Article.csv",
        "scimagodb.csv",
        "ArticleAuthor.csv",
        "journal.csv",
        "Author.csv",
        "Affiliation.csv",
        "Citation.csv"
    ]

    # Crear la ruta completa para el archivo Excel de salida
    ruta_salida = os.path.join(ruta_directorio, "data.xlsx")

    with pd.ExcelWriter(ruta_salida, engine="openpyxl") as writer:
        for nombre in nombres_archivos:
            ruta_csv = os.path.join(ruta_directorio, nombre)
            if os.path.exists(ruta_csv):
                df = pd.read_csv(ruta_csv)
                nombre_hoja = os.path.splitext(nombre)[0]  # quitar extensión
                df.to_excel(writer, sheet_name=nombre_hoja, index=False)
            else:
                print(f"Archivo no encontrado: {ruta_csv}")

    print(f"Archivo Excel creado: {ruta_salida}")

# ruta=r"C:\Users\User\Documents\Preprocesamiento\MetricSci\Profesora Paola Ariza\WoS_results"
# combinar_csv_a_excel(ruta)
