# import logging
# import pandas as pd

# def merge_scopus_ref(
#     scopus_df: pd.DataFrame,
#     scopus_ref_enriched: pd.DataFrame
# ) -> pd.DataFrame:
#     """
#     Combina el DataFrame de artículos principales (scopus_df) con el de referencias enriquecidas,
#     marcando qué filas son el artículo principal y cuáles son referencias, y evitando duplicados
#     en función de la columna 'SR', dando preferencia a los artículos principales.

#     Además:
#       - Renombra en las referencias:
#           'source_title' → 'journal'
#           'journal_abbr' → 'source_title'
#       - Elimina 'source_title_mainarticle' y 'pages'
#       - Al final, elimina todos los puntos de la columna 'source_title'

#     Parameters:
#     -----------
#     scopus_df : pd.DataFrame
#         DataFrame original de Scopus con columna 'SR'.
#     scopus_ref_enriched : pd.DataFrame
#         DataFrame de referencias enriquecidas con columnas 'SR_ref', 'CR_ref',
#         'source_title', 'journal_abbr', 'source_title_mainarticle', 'pages', etc.

#     Returns:
#     --------
#     pd.DataFrame
#         DataFrame combinado con:
#         - columna 'ismainarticle' indicando TRUE/FALSE
#         - columnas renombradas y ajustadas
#         - sin duplicados en 'SR', priorizando artículos principales
#         - sin puntos en 'source_title'
#     """
#     # Configurar logging
#     logging.basicConfig(
#         filename='merge_scopus_ref.log',
#         filemode='a',
#         format='%(asctime)s - %(levelname)s - %(message)s',
#         level=logging.INFO
#     )
#     logging.info("Iniciando merge_scopus_ref")

#     # 1) Marcar artículos principales y referencias
#     scopus_df = scopus_df.copy()
#     refs = scopus_ref_enriched.copy()
#     scopus_df['ismainarticle'] = 'TRUE'
#     refs['ismainarticle']       = 'FALSE'
#     logging.info("Añadida columna 'ismainarticle'")

#     # 1b) Renombrar columnas en refs
#     rename_map = {
#         'source_title': 'journal',
#         'journal_abbr': 'source_title'
#     }
#     refs = refs.rename(columns=rename_map)
#     # Eliminar columnas innecesarias
#     for drop_col in ('source_title_mainarticle', 'pages'):
#         if drop_col in refs.columns:
#             refs = refs.drop(columns=[drop_col])
#             logging.info(f"Eliminada columna '{drop_col}' de referencias")

#     # 2) Procesar columnas SR en refs
#     if 'SR' in refs.columns:
#         refs = refs.drop(columns=['SR'])
#         logging.info("Eliminada columna 'SR' de referencias")
#     if 'SR_ref' in refs.columns:
#         refs = refs.rename(columns={'SR_ref': 'SR'})
#         logging.info("Renombrada 'SR_ref' a 'SR'")
#     if 'CR_ref' in refs.columns:
#         refs = refs.drop(columns=['CR_ref'])
#         logging.info("Eliminada columna 'CR_ref' de referencias")

#     # 3) Eliminar duplicados en refs basado en 'SR', priorizando DOI válido
#     if 'SR' in refs.columns:
#         refs['doi'] = None
#         def _prioritize(group):
#             valid = group['doi'].notnull() & (group['doi'] != '') & (group['doi'] != '-')
#             if valid.any():
#                 return group[valid].iloc[0]
#             return group.iloc[0]

#         refs_clean = (
#             refs
#             .groupby('SR', as_index=False)
#             .apply(_prioritize)
#             .reset_index(drop=True)
#         )
#         logging.info("Duplicados en referencias eliminados, priorizando DOI")
#     else:
#         refs_clean = refs.copy()
#         logging.warning("No se encontró 'SR' en referencias; no se eliminaron duplicados")

#     # 4) Alinear columnas para concatenar
#     df_cols   = set(scopus_df.columns)
#     refs_cols = set(refs_clean.columns)

#     for col in df_cols - refs_cols:
#         refs_clean[col] = None
#     for col in refs_cols - df_cols:
#         scopus_df[col] = None

#     # Reordenar refs_clean para que coincida con el orden de scopus_df.columns
#     refs_clean = refs_clean[scopus_df.columns]
#     logging.info("Columnas alineadas entre artículos y referencias")

#     # 5) Concatenar
#     combined = pd.concat([scopus_df, refs_clean], ignore_index=True)
#     logging.info("DataFrames concatenados")

#     # 6) Eliminar duplicados en combined basado en 'SR', manteniendo ismainarticle == 'TRUE'
#     if 'SR' in combined.columns:
#         combined['_order'] = combined['ismainarticle'] == 'TRUE'
#         combined = combined.sort_values(by='_order', ascending=False)
#         combined = combined.drop_duplicates(subset='SR', keep='first')
#         combined = combined.drop(columns=['_order'])
#         logging.info("Duplicados en combinado eliminados, priorizando artículos principales")
#     else:
#         logging.warning("No se encontró 'SR' en combinado; no se eliminaron duplicados")

#     # 7) Eliminar puntos en source_title
#     if 'source_title' in combined.columns:
#         combined['source_title'] = (
#             combined['source_title']
#             .astype(str)
#             .str.replace('.', '', regex=False)
#         )
#         logging.info("Eliminados puntos de 'source_title'")

#     logging.info("merge_scopus_ref completado con éxito")
#     return combined
import pandas as pd

def merge_scopus_ref(scopus_df: pd.DataFrame, scopus_ref_enriched: pd.DataFrame) -> pd.DataFrame:
    """
    Fusiona el DataFrame original de Scopus con las referencias enriquecidas de OpenAlex.
    Convierte todo el texto a mayúsculas y ajusta el mapeo de columnas según especificaciones.
    
    Args:
        scopus_df: DataFrame original de Scopus con los datos de los artículos
        scopus_ref_enriched: DataFrame con las referencias enriquecidas de OpenAlex
        
    Returns:
        DataFrame combinado con las columnas correspondientes en mayúsculas
    """
    # Hacer una copia de los DataFrames para evitar modificar los originales
    scopus_copy = scopus_df.copy()
    refs_copy = scopus_ref_enriched.copy()
    
    # Función para convertir a mayúsculas solo las columnas de texto
    def uppercase_text_columns(df):
        for col in df.columns:
            if df[col].dtype == 'object':  # Solo para columnas de texto
                df[col] = df[col].str.upper() if df[col].notna().any() else df[col]
        return df
    
    # Convertir a mayúsculas ambos DataFrames
    scopus_copy = uppercase_text_columns(scopus_copy)
    refs_copy = uppercase_text_columns(refs_copy)
    
    # Renombrar columnas en el DataFrame de referencias según las especificaciones corregidas
    column_mapping = {
        'authors': 'author_full_names',
        # 'journal': 'journal_title',
        # 'source_title': 'journal_abbreviation',
        'page': 'page_start',
        'openalex_url': 'link',
        'SR_ref': 'SR'
    }
    
    refs_copy = refs_copy.rename(columns=column_mapping)
    
    # Eliminar columnas que no se necesitan
    columns_to_drop = ['doi_original', 'openalex_id', 'journal_issue_number', 'SR_original']
    refs_copy = refs_copy.drop(columns=[col for col in columns_to_drop if col in refs_copy.columns])
    
    # Asegurar que las columnas de page_end y page_count estén presentes
    if 'page_start' in refs_copy.columns:
        refs_copy['page_end'] = refs_copy['page_start']  # Asumimos que es la misma página si no hay rango
        refs_copy['page_count'] = 1  # Asumimos 1 página por defecto
    
    # Seleccionar solo las columnas que existen en ambos DataFrames
    common_columns = set(scopus_copy.columns) & set(refs_copy.columns)
    
    # Crear una lista de todas las columnas únicas de ambos DataFrames
    all_columns = list(set(scopus_copy.columns) | set(refs_copy.columns))
    
    # Reindexar ambos DataFrames con todas las columnas posibles
    scopus_copy = scopus_copy.reindex(columns=all_columns)
    refs_copy = refs_copy.reindex(columns=all_columns)
    
    # Concatenar los DataFrames
    merged_df = pd.concat([scopus_copy, refs_copy], ignore_index=True)
    
    # Convertir las columnas de texto nuevamente por si hubo alguna modificación durante el merge
    merged_df = uppercase_text_columns(merged_df)
    
    return merged_df