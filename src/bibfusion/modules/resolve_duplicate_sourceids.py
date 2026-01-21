import pandas as pd
from typing import Tuple, Any

def resolve_duplicate_sourceids(
    wos_df_6: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Unify and aggregate entries by Sourceid, then split into two tables.

    1) Within each duplicated Sourceid group:
       - source_title: pick the shortest non-null string if multiple.
       - journal: pick the most frequent non-null string if multiple.
       - issn: pick the most frequent non-null string if multiple.
       - eissn: pick the most frequent non-null string if multiple.

    2) Collapse each Sourceid group into a single row by concatenating all SR
       values with "; ", and keeping the unified source_title, journal, issn, eissn.

    3) Rename 'Sourceid' → 'journal_id'.

    4) Split into:
       - **journal**: one row per `journal_id` with columns
         ['journal_id', 'source_title', 'journal', 'issn', 'eissn'].
       - **scimago_raw**: one row per individual `SR`, with columns
         ['journal_id', 'SR'] (i.e. exploded from the concatenated list).

    Parameters
    ----------
    wos_df_6 : pd.DataFrame
        DataFrame with columns ['SR','journal','source_title','issn','eissn','Sourceid'].

    Returns
    -------
    Tuple[pd.DataFrame, pd.DataFrame]
        - journal: DataFrame of unified journal metadata (one row per journal_id).
        - scimago_raw: DataFrame of journal_id–SR pairs (one row per SR).
    """
    df = wos_df_6.copy()

    # helper to pick most frequent non-null
    def pick_most_frequent(s: pd.Series) -> Any:
        nonnull = s.dropna()
        if nonnull.empty:
            return None
        return nonnull.value_counts().idxmax()

    # find Sourceids that occur >1 time
    dup_counts = df['Sourceid'].value_counts()
    dup_sids = dup_counts[dup_counts > 1].index

    # unify fields within each duplicated Sourceid
    for sid in dup_sids:
        mask = df['Sourceid'] == sid

        # source_title → shortest non-null
        titles = df.loc[mask, 'source_title'].dropna().unique()
        if len(titles) > 1:
            df.loc[mask, 'source_title'] = min(titles, key=len)

        # journal → most frequent
        journals = df.loc[mask, 'journal']
        if journals.nunique(dropna=True) > 1:
            df.loc[mask, 'journal'] = pick_most_frequent(journals)

        # issn → most frequent
        issns = df.loc[mask, 'issn']
        if issns.nunique(dropna=True) > 1:
            df.loc[mask, 'issn'] = pick_most_frequent(issns)

        # eissn → most frequent
        eissns = df.loc[mask, 'eissn']
        if eissns.nunique(dropna=True) > 1:
            df.loc[mask, 'eissn'] = pick_most_frequent(eissns)

    # aggregate to one row per Sourceid
    aggregated = (
        df
        .groupby('Sourceid', as_index=False)
        .agg({
            'SR':       lambda s: '; '.join(s.astype(str)),
            'source_title': 'first',
            'journal':      'first',
            'issn':         'first',
            'eissn':        'first'
        })
        .rename(columns={'Sourceid': 'journal_id'})
    )

    # build journal metadata table
    journal = aggregated[['journal_id', 'source_title', 'journal']].copy()

    # build scimago_raw: one row per SR
    scimago_raw = (
        aggregated[['journal_id', 'SR']]
        .assign(SR=lambda df2: df2['SR'].str.split(';'))
        .explode('SR')
        .assign(SR=lambda df2: df2['SR'].str.strip())
        .loc[:, ['journal_id', 'SR']]
        .reset_index(drop=True)
    )

    return journal, scimago_raw
