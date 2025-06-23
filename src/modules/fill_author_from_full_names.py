import pandas as pd

def fill_author_from_full_names(df):
    def convertir_a_formato_author(nombre_completo):
        partes = nombre_completo.strip().split()
        if len(partes) == 1:
            return partes[0].upper()
        apellido = partes[-1].upper()
        iniciales = ''.join([p[0].upper() + '.' for p in partes[:-1]])
        return f"{apellido} {iniciales}".strip()

    def generar_author(row):
        if pd.isna(row['author']) or row['author'].strip() == '':
            if pd.notna(row['author_full_names']):
                nombres = [n.strip() for n in row['author_full_names'].split(';')]
                return '; '.join([convertir_a_formato_author(nombre) for nombre in nombres])
        return row['author']

    df['author'] = df.apply(generar_author, axis=1)
    return df
