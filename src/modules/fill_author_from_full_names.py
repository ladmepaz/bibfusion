import pandas as pd


def fill_author_from_full_names(df):
    """
    Rellena la columna 'author' usando 'author_full_names'
    cuando 'author' está vacío o es NaN.
    Convierte nombres completos al formato: APELLIDO Iniciales.
    """

    def convertir_a_formato_author(nombre_completo):
        # Validación básica
        if not isinstance(nombre_completo, str):
            return ''

        nombre_completo = nombre_completo.strip()
        if nombre_completo == '':
            return ''

        partes = nombre_completo.split()

        # Caso: solo una palabra
        if len(partes) == 1:
            return partes[0].upper()

        # Apellido = última palabra
        apellido = partes[-1].upper()

        # Iniciales de los nombres
        iniciales = ''.join(
            p[0].upper() + '.'
            for p in partes[:-1]
            if p
        )

        return f"{apellido} {iniciales}".strip()

    def generar_author(row):
        # Solo completar si 'author' está vacío o NaN
        if pd.isna(row['author']) or str(row['author']).strip() == '':
            if pd.notna(row['author_full_names']):
                # Separar autores y eliminar entradas vacías
                nombres = [
                    n.strip()
                    for n in str(row['author_full_names']).split(';')
                    if n.strip() != ''
                ]

                if not nombres:
                    return row['author']

                autores_convertidos = [
                    convertir_a_formato_author(nombre)
                    for nombre in nombres
                ]

                # Eliminar posibles resultados vacíos
                autores_convertidos = [
                    a for a in autores_convertidos if a != ''
                ]

                if autores_convertidos:
                    return '; '.join(autores_convertidos)

        # Si ya existe 'author', se respeta
        return row['author']

    df['author'] = df.apply(generar_author, axis=1)
    return df
