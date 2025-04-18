import pandas as pd

def enrich_references_with_journal_abbr(
    scopus_references: pd.DataFrame,
    scopus_df_2: pd.DataFrame,
    scimago_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Añade la columna 'journal_abbr' a scopus_references usando:
      1) la lista de journals extraída de scopus_df (limpia de puntos)
      2) como respaldo, los datos de scimago_df para completar los faltantes

    Parameters:
    -----------
    scopus_references : pd.DataFrame
        DataFrame de referencias (output de process_scopus_references)
        que contiene la columna 'source_title'.
    scopus_df : pd.DataFrame
        DataFrame Scopus original con columnas:
          - 'journal'
          - 'abbreviated_source_title'
    scimago_df : pd.DataFrame
        DataFrame Scimago con columnas:
          - 'Title' (nombre de la revista)
          - 'journal_abbr' (abreviatura)

    Returns:
    --------
    enriched_refs : pd.DataFrame
        scopus_references con la columna adicional 'journal_abbr',
        usando primero scopus_df y luego scimago_df para rellenar
        los valores faltantes.
    """
    # 1) Construir journal_list desde scopus_df
    journal_list = (
        scopus_df_2[['journal', 'abbreviated_source_title']]
        .dropna(subset=['abbreviated_source_title'])
        .assign(
            journal_abbr=lambda df: (
                df['abbreviated_source_title']
                .str.replace('.', '', regex=False)
                .str.strip()
            )
        )
        .drop(columns=['abbreviated_source_title'])
        .drop_duplicates(subset=['journal_abbr'])
    )

    # 2) Merge inicial para añadir journal_abbr desde scopus_df
    enriched = scopus_references.merge(
        journal_list[['journal', 'journal_abbr']],
        left_on='source_title',
        right_on='journal',
        how='left'
    ).drop(columns=['journal'])

    # 3) Preparar scimago para fallback
    scimago_list = (
        scimago_df[['Title', 'journal_abbr']]
        .dropna(subset=['Title', 'journal_abbr'])
        .rename(columns={'Title': 'source_title'})
        .drop_duplicates(subset=['source_title'])
    )

    # 4) Merge de respaldo para obtener abbr desde scimago
    enriched = enriched.merge(
        scimago_list,
        on='source_title',
        how='left',
        suffixes=('', '_scimago')
    )

    # 5) Rellenar journal_abbr faltantes con los de scimago
    enriched['journal_abbr'] = (
        enriched['journal_abbr']
        .fillna(enriched['journal_abbr_scimago'])
    )

    # 6) Eliminar columna auxiliar de scimago
    return enriched.drop(columns=['journal_abbr_scimago'])
