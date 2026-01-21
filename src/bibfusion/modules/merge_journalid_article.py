import pandas as pd

scimago_raw = pd.read_csv(r'tests/files/temp_scimagoraw.csv')
article_df = pd.read_csv(r'tests/files/WoS_DB - Article.csv')

def merge_journalid_article(scimago_raw, articles):
    """
    Añade una columna 'journal_id' al DataFrame articles en base a coincidencias 
    en la columna 'SR' con el DataFrame scimago_raw.

    Parameters:
    scimago_raw (pd.DataFrame): DataFrame que contiene las columnas 'SR' y 'journal_id'.
    articles (pd.DataFrame): DataFrame que contiene la columna 'SR'.

    Returns:
    pd.DataFrame: DataFrame de articles con una nueva columna 'journal_id' añadida.
    """
    
    # Nos aseguramos de que las columnas SR estén en el mismo formato (por ejemplo, sin espacios extra)
    scimago_raw['SR'] = scimago_raw['SR'].str.strip()
    articles['SR'] = articles['SR'].str.strip()

    # Hacemos un merge para añadir journal_id según coincidencias en 'SR'
    merged_df = pd.merge(articles, scimago_raw[['SR', 'journal_id']], on='SR', how='left')
    for col in ['cited_reference_count', 'usage_count_last_180_days', 'usage_count_since_2013', 'journal_id', 'total_times_cited']:
        merged_df[col] = merged_df[col].astype('Int64')  # Tipo entero compatible con NaN


    return merged_df

merged_df = merge_journalid_article(scimago_raw, article_df)

# Guardamos el DataFrame resultante en un archivo CSV
merged_df.to_csv(r'tests/files/temp_mergearticle_df.csv', index=False)
