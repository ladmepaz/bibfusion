# wos_module.py

import pandas as pd
import re

def wos_df(ruta_txt):
    """
    Lee un archivo de texto de Web of Science y devuelve un DataFrame con la información.

    :param ruta_txt: str - La ruta del archivo de texto a leer.
    :return: DataFrame - Un DataFrame que contiene los datos extraídos del archivo.
    """
    # Crear listas para almacenar los datos
    columns = ["AU", "AF", "CR", "AB", "AR", "BP", "C1", "C3", "CL", "CT", "CY",
               "DA", "DE", "DI", "DT", "EA", "EF", "EI", "EM", "EP", "ER", "FU",
               "FX", "GA", "HC", "HO", "HP", "ID", "IS", "J9", "JI", "LA", "MA",
               "NR", "OA", "OI", "PA", "PD", "PG", "PI", "PM", "PN", "PT", "PU",
               "PY", "RI", "RP", "SC", "SI", "SN", "SO", "SP", "SU", "TC", "TI",
               "U1", "U2", "UT", "VL", "WC", "WE", "Z9", "DB", "AU_UN", "AU1_UN",
               "AU_UN_NR", "SR_FULL_SR", "AU_CO"]
    data = {col: [] for col in columns}

    # Leer el archivo de texto
    with open(ruta_txt, 'r', encoding='utf-8') as file:
        content = file.read()

    # Separar los registros por el marcador 'ER'
    records = content.split('ER\n')

    # Procesar cada registro
    for record in records:
        if record.strip():
            for col in columns:
                pattern = rf"{col}\s(.*)"
                match = re.search(pattern, record, re.MULTILINE)
                data[col].append(match.group(1) if match else "")

    # Crear y devolver un DataFrame
    df = pd.DataFrame(data)
    return df
