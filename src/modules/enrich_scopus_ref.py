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
    scopus_df_2 : pd.DataFrame
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


def fix_source_titles(
    scopus_references_1: pd.DataFrame,
    scopus_df_2: pd.DataFrame,
    scimago_df: pd.DataFrame
) -> pd.DataFrame:
    df = scopus_references_1.copy()
    df['clean_abbr'] = df['source_title'].str.replace('.', '', regex=False).str.strip()

    # primary mapping from Scopus
    journal_map = (
        scopus_df_2[['journal','abbreviated_source_title']]
        .dropna(subset=['abbreviated_source_title'])
        .assign(
            clean_abbr=lambda d: (
                d['abbreviated_source_title']
                 .str.replace('.', '', regex=False)
                 .str.strip()
            )
        )
        .loc[:, ['clean_abbr','journal']]
        .drop_duplicates(subset=['clean_abbr'])
    )

    # fallback mapping from Scimago, with explicit .astype(str)
    scimago_map = (
        scimago_df[['Title','journal_abbr']]
        .dropna(subset=['journal_abbr'])
        .astype({'journal_abbr': str, 'Title': str})
        .assign(
            clean_abbr=lambda d: (
                d['journal_abbr']
                 .str.replace('.', '', regex=False)
                 .str.strip()
            ),
            full_title=lambda d: d['Title']
        )
        .loc[:, ['clean_abbr','full_title']]
        .drop_duplicates(subset=['clean_abbr'])
    )

    merged = (
        df
        .merge(journal_map, on='clean_abbr', how='left')
        .merge(scimago_map, on='clean_abbr', how='left')
    )

    # choose priority: Scopus->Scimago->original
    def choose_full(row):
        if pd.notna(row['journal']):
            return row['journal']
        if pd.notna(row['full_title']):
            return row['full_title']
        return row['source_title']

    merged['source_title'] = merged.apply(choose_full, axis=1)
    return merged.drop(columns=['clean_abbr','journal','full_title'])


def add_SR_ref(
    df: pd.DataFrame,
    author_col: str = 'author',
    year_col:   str = 'year',
    jabbr_col:  str = 'journal_abbr',
    new_col:    str = 'SR_ref'
) -> pd.DataFrame:
    """
    Given a DataFrame with columns for authors, year, and journal_abbr,
    creates a new column (default name "SR_ref") of the form:
       FIRST_AUTHOR, YEAR, JOURNAL_ABBR

    - FIRST_AUTHOR is the substring before the first ';' in the author_col.
    - YEAR is taken as-is (converted to string).
    - JOURNAL_ABBR is taken as-is.
    """
    df = df.copy()
    # extract first author (everything before the first semicolon)
    df['__first'] = (
        df[author_col]
          .fillna('')                      # avoid NaNs
          .astype(str)                     # ensure it's a str
          .str.split(';', n=1).str[0]      # split once, take first
          .str.strip()                     # trim whitespace
    )

    # now compose the SR_ref
    df[new_col] = (
        df['__first'] + ', '
        + df[year_col].fillna('').astype(str) + ', '
        + df[jabbr_col].fillna('').astype(str)
    )

    return df.drop(columns='__first')


def extract_sr_mapping(scopus_references: pd.DataFrame) -> pd.DataFrame:
    """
    From the enriched references table, pull out a deduplicated mapping
    of main-article SR to reference SR_ref.

    Parameters
    ----------
    scopus_references : pd.DataFrame
        Must contain columns 'SR' and 'SR_ref'.

    Returns
    -------
    pd.DataFrame
        Two‑column DataFrame with columns ['SR','SR_ref'], one row per unique pair.
    """
    # sanity check
    missing = {'SR','SR_ref'} - set(scopus_references.columns)
    if missing:
        raise KeyError(f"Missing columns in input: {missing}")

    mapping = (
        scopus_references[['SR','SR_ref']]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    return mapping
