import pandas as pd

def merge_tos_with_articles(df_tos, df_article):
    """
    Une df_tos y df_article usando la columna SR.
    
    Parámetros:
        df_tos (pd.DataFrame): Contiene columnas ['SR', 'tos']
        df_article (pd.DataFrame): Contiene columnas ['SR', 'author', 'title', 'year', 'doi']
    
    Retorna:
        pd.DataFrame: DataFrame combinado con columnas:
            ['SR', 'tos', 'author', 'title', 'year', 'doi']
    """
    # Hacemos un merge asegurándonos que es un left join (mantener todos los SR de df_tos)
    df_result = pd.merge(
        df_tos,
        df_article[['SR', 'author', 'title', 'year', 'doi', 'author_keywords', 'abstract']],
        on='SR',
        how='left'
    )
    # Convertir año a entero si no es NaN
    if 'year' in df_result.columns:
        df_result['year'] = pd.to_numeric(df_result['year'], errors='coerce')
        df_result['year'] = df_result['year'].astype('Int64')  # Entero pero permite NaN
    
    df_result["doi"] = df_result["doi"].astype(str).str.strip()  # quitar espacios inicio/fin
    df_result["doi"] = df_result["doi"].str.replace(r"\s+", " ", regex=True)  # normalizar espacios internos
    df_result["doi"] = df_result["doi"].str.upper()  # opcional: unificar en mayúsculas
    df_result = df_result.drop_duplicates(subset=["doi"], keep="first")
    
    return df_result
