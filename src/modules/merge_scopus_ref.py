import logging
import pandas as pd

def merge_scopus_ref(
    scopus_df: pd.DataFrame,
    scopus_ref_enriched: pd.DataFrame
) -> pd.DataFrame:
    """
    Combina el DataFrame de artículos principales (scopus_df) con el de referencias enriquecidas,
    marcando qué filas son el artículo principal y cuáles son referencias, y evitando duplicados
    en función de la columna 'SR', dando preferencia a los artículos principales.

    Además:
      - Renombra en las referencias:
          'source_title' → 'journal'
          'journal_abbr' → 'abbreviated_source_title'
      - Elimina 'source_title_mainarticle' y 'pages'
      - Al final, elimina todos los puntos de la columna 'abbreviated_source_title'

    Parameters:
    -----------
    scopus_df : pd.DataFrame
        DataFrame original de Scopus con columna 'SR'.
    scopus_ref_enriched : pd.DataFrame
        DataFrame de referencias enriquecidas con columnas 'SR_ref', 'CR_ref',
        'source_title', 'journal_abbr', 'source_title_mainarticle', 'pages', etc.

    Returns:
    --------
    pd.DataFrame
        DataFrame combinado con:
        - columna 'ismainarticle' indicando TRUE/FALSE
        - columnas renombradas y ajustadas
        - sin duplicados en 'SR', priorizando artículos principales
        - sin puntos en 'abbreviated_source_title'
    """
    # Configurar logging
    logging.basicConfig(
        filename='merge_scopus_ref.log',
        filemode='a',
        format='%(asctime)s - %(levelname)s - %(message)s',
        level=logging.INFO
    )
    logging.info("Iniciando merge_scopus_ref")

    # 1) Marcar artículos principales y referencias
    scopus_df = scopus_df.copy()
    refs = scopus_ref_enriched.copy()
    scopus_df['ismainarticle'] = 'TRUE'
    refs['ismainarticle']       = 'FALSE'
    logging.info("Añadida columna 'ismainarticle'")

    # 1b) Renombrar columnas en refs
    rename_map = {
        'source_title': 'journal',
        'journal_abbr': 'abbreviated_source_title'
    }
    refs = refs.rename(columns=rename_map)
    # Eliminar columnas innecesarias
    for drop_col in ('source_title_mainarticle', 'pages'):
        if drop_col in refs.columns:
            refs = refs.drop(columns=[drop_col])
            logging.info(f"Eliminada columna '{drop_col}' de referencias")

    # 2) Procesar columnas SR en refs
    if 'SR' in refs.columns:
        refs = refs.drop(columns=['SR'])
        logging.info("Eliminada columna 'SR' de referencias")
    if 'SR_ref' in refs.columns:
        refs = refs.rename(columns={'SR_ref': 'SR'})
        logging.info("Renombrada 'SR_ref' a 'SR'")
    if 'CR_ref' in refs.columns:
        refs = refs.drop(columns=['CR_ref'])
        logging.info("Eliminada columna 'CR_ref' de referencias")

    # 3) Eliminar duplicados en refs basado en 'SR', priorizando DOI válido
    if 'SR' in refs.columns:
        def _prioritize(group):
            valid = group['doi'].notnull() & (group['doi'] != '') & (group['doi'] != '-')
            if valid.any():
                return group[valid].iloc[0]
            return group.iloc[0]

        refs_clean = (
            refs
            .groupby('SR', as_index=False)
            .apply(_prioritize)
            .reset_index(drop=True)
        )
        logging.info("Duplicados en referencias eliminados, priorizando DOI")
    else:
        refs_clean = refs.copy()
        logging.warning("No se encontró 'SR' en referencias; no se eliminaron duplicados")

    # 4) Alinear columnas para concatenar
    df_cols   = set(scopus_df.columns)
    refs_cols = set(refs_clean.columns)

    for col in df_cols - refs_cols:
        refs_clean[col] = None
    for col in refs_cols - df_cols:
        scopus_df[col] = None

    # Reordenar refs_clean para que coincida con el orden de scopus_df.columns
    refs_clean = refs_clean[scopus_df.columns]
    logging.info("Columnas alineadas entre artículos y referencias")

    # 5) Concatenar
    combined = pd.concat([scopus_df, refs_clean], ignore_index=True)
    logging.info("DataFrames concatenados")

    # 6) Eliminar duplicados en combined basado en 'SR', manteniendo ismainarticle == 'TRUE'
    if 'SR' in combined.columns:
        combined['_order'] = combined['ismainarticle'] == 'TRUE'
        combined = combined.sort_values(by='_order', ascending=False)
        combined = combined.drop_duplicates(subset='SR', keep='first')
        combined = combined.drop(columns=['_order'])
        logging.info("Duplicados en combinado eliminados, priorizando artículos principales")
    else:
        logging.warning("No se encontró 'SR' en combinado; no se eliminaron duplicados")

    # 7) Eliminar puntos en abbreviated_source_title
    if 'abbreviated_source_title' in combined.columns:
        combined['abbreviated_source_title'] = (
            combined['abbreviated_source_title']
            .astype(str)
            .str.replace('.', '', regex=False)
        )
        logging.info("Eliminados puntos de 'abbreviated_source_title'")

    logging.info("merge_scopus_ref completado con éxito")
    return combined
