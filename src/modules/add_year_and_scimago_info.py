import pandas as pd
from typing import Any

def add_year_and_scimago_info(
    scimago_raw: pd.DataFrame,
    wos_df_3: pd.DataFrame,
    scimagojr_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Enrich `scimago_raw` with publication year from WOS and then
    attach the full Scimago data for each journal_id and year.

    Parameters
    ----------
    scimago_raw : pd.DataFrame
        DataFrame with columns ['journal_id', 'SR'].
    wos_df_3 : pd.DataFrame
        Original WOS DataFrame containing at least ['SR', 'year'].
    scimagojr_df : pd.DataFrame
        Scimago DataFrame containing columns including
        ['Sourceid', 'year', ..., 'journal_abbr', '__abbr_norm'].

    Returns
    -------
    pd.DataFrame
        A DataFrame with the same rows as `scimago_raw`, plus:
        - 'year'  (from WOS)
        - all columns from `scimagojr_df` (e.g. 'Rank', 'Title', 'Issn', etc.)
        
        Rows are matched by:
        scimago_raw.journal_id == scimagojr_df.Sourceid  AND
        scimago_raw.year       == scimagojr_df.year
    """
    wos_df_3['year'] = wos_df_3['year'].astype(str)
    scimagojr_df['year'] = scimagojr_df['year'].astype(str)
    # 1) Pull SR → year mapping from wos_df_3
    wos_year = (
        wos_df_3[['SR', 'year']]
        .drop_duplicates(subset='SR')
    )

    # 2) Merge to add 'year' to scimago_raw
    scimago_with_year = scimago_raw.merge(
        wos_year,
        how='left',
        on='SR'
    )
    

    # 3) Left‑merge with scimagojr_df on journal_id=Sourceid and year=year
    enriched = scimago_with_year.merge(
        scimagojr_df,
        how='left',
        left_on=['journal_id', 'year'],
        right_on=['Sourceid', 'year'],
        suffixes=('', '_scimago')
    )

    # 4) Drop the redundant 'Sourceid' column from scimagojr_df
    if 'Sourceid' in enriched.columns:
        enriched = enriched.drop(columns=['Sourceid'])

    # 5) Return with original scimago_raw_1 rows preserved
    #    Columns will be: ['journal_id', 'SR', 'year', <all scimagojr_df columns minus Sourceid>]
    cols = ['journal_id', 'SR', 'year'] + [
        c for c in scimagojr_df.columns if c != 'Sourceid'
    ]
    return enriched[cols]
