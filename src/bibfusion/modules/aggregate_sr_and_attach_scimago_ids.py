import pandas as pd

def aggregate_sr_and_attach_scimago_ids(wos_df, scimago):
    """
    1) Collapse all SR values per unique (journal, source_title, issn, eissn) grouping, joining them with "; ".
    2) Left‑join Sourceid from scimago on wos_df.source_title == scimago.journal_abbr.
    3) Fill any remaining missing Sourceid by matching wos_df.journal == scimago.Title.
    4) Fill still-missing Sourceid by matching wos_df.issn == scimago.Issn.
    5) Assign sequential integers (1..n) to any rows still missing Sourceid.
    
    Parameters
    ----------
    wos_df : pd.DataFrame
        Must contain ['SR','journal','source_title','issn','eissn'].
    scimago : pd.DataFrame
        Must contain ['journal_abbr','Title','Issn','Sourceid'].
    
    Returns
    -------
    pd.DataFrame
        One row per unique (journal, source_title, issn, eissn), with columns:
        ['SR','journal','source_title','issn','eissn','Sourceid']
    """
    # 1) sanity-check inputs
    required_wos = {'SR','journal','source_title','issn','eissn'}
    required_sci = {'journal_abbr','Title','Issn','Sourceid'}
    
    if 'eissn' not in wos_df.columns:
        wos_df['eissn'] = pd.NA
    if not required_wos.issubset(wos_df.columns):
        missing = required_wos - set(wos_df.columns)
        raise KeyError(f"wos_df is missing required columns: {missing}")
    if not required_sci.issubset(scimago.columns):
        missing = required_sci - set(scimago.columns)
        raise KeyError(f"scimago is missing required columns: {missing}")
    
    # 2) collapse SR per group
    group_cols = ['journal','source_title','issn','eissn']
    collapsed = (
        wos_df
        .groupby(group_cols, as_index=False)
        .agg({'SR': lambda s: '; '.join(s.astype(str))})
    )
    
    # 3) first-pass merge on source_title -> journal_abbr
    merged = collapsed.merge(
        scimago[['journal_abbr','Sourceid']].drop_duplicates('journal_abbr'),
        how='left',
        left_on='source_title',
        right_on='journal_abbr'
    ).drop(columns='journal_abbr')
    
    # 4) fill missing by journal -> Title
    mask = merged['Sourceid'].isna()
    if mask.any():
        title_map = (
            scimago[['Title','Sourceid']]
            .drop_duplicates('Title')
            .set_index('Title')['Sourceid']
        )
        merged.loc[mask, 'Sourceid'] = merged.loc[mask, 'journal'].map(title_map)
    
    # 5) fill still-missing by issn -> Issn
    mask = merged['Sourceid'].isna()
    if mask.any():
        issn_map = (
            scimago[['Issn','Sourceid']]
            .drop_duplicates('Issn')
            .set_index('Issn')['Sourceid']
        )
        merged.loc[mask, 'Sourceid'] = merged.loc[mask, 'issn'].map(issn_map)
    
    # 6) fill any remaining missing Sourceid with sequential integers
    mask = merged['Sourceid'].isna()
    n_missing = mask.sum()
    if n_missing > 0:
        # assign 1..n_missing to the missing rows
        merged.loc[mask, 'Sourceid'] = pd.Series(
            range(1, n_missing + 1),
            index=merged.index[mask]
        ).astype(int)
    
    # 7) final reorder & return
    return merged[['SR','journal','source_title','issn','eissn','Sourceid']]
