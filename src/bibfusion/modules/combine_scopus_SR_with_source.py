import pandas as pd

def combine_scopus_SR_with_source(scopus_df_2: pd.DataFrame,
                                  scopus_ref_6: pd.DataFrame,
                                  scopus_ref_7: pd.DataFrame) -> pd.DataFrame:
    """
    1) From scopus_df_2 take ['SR', 'journal_abbreviation'].
    2) From scopus_ref_6 take   ['SR_ref', 'journal_abbr'] -> rename to the same columns.
    3) Concat, drop duplicates on (SR, journal_abbreviation).
    4) From scopus_ref_7 take ['journal_abbr', 'Sourceid'] (unique).
    5) Left-join that Sourceid onto the combined DF via journal_abbreviation == journal_abbr.
    6) Drop the journal_abbreviation column.

    Returns:
        DataFrame with columns ['SR', 'Sourceid'].
    """
    # 1) pull and rename from scopus_df_2
    df_main = scopus_df_2[['SR', 'journal_abbreviation']].copy()
    
    # 2) pull and rename from scopus_ref_6
    df_ref = (
        scopus_ref_6[['SR_ref', 'journal_abbr']]
        .rename(columns={
            'SR_ref': 'SR',
            'journal_abbr': 'journal_abbreviation'
        })
    )
    
    # 3) concat & dedupe
    combined = pd.concat([df_main, df_ref], ignore_index=True)
    combined = combined.drop_duplicates(subset=['SR', 'journal_abbreviation'])
    
    # 4) prepare mapping from scopus_ref_7
    mapping = (
        scopus_ref_7[['journal_abbr', 'Sourceid']]
        .drop_duplicates(subset=['journal_abbr'])
        .rename(columns={'journal_abbr': 'journal_abbreviation'})
    )
    
    # 5) merge Sourceid in
    merged = combined.merge(mapping,
                            on='journal_abbreviation',
                            how='left')
    
    # 6) drop the intermediate journal_abbreviation column
    out = merged.drop(columns=['journal_abbreviation'])
    
    return out.reset_index(drop=True)
