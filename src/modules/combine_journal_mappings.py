import pandas as pd

def combine_journal_mappings(scopus_ref_6: pd.DataFrame,
                             scopus_df_2: pd.DataFrame,
                             scimago_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a unified mapping of journal titles → abbreviations → Scimago Sourceid.
    
    1) From scopus_ref_6 pull:
         - journal        → journal_title
         - journal_abbr
    2) From scopus_df_2 pull:
         - journal_title
         - journal_abbreviation → journal_abbr
    3) Concat, drop duplicates
    4) From scimago_df pull (Title, Sourceid), drop duplicate Titles
    5) Left-merge Sourceid by journal_title
    6) Fill missing Sourceid with unique integers 1…n_missing
    
    Returns a DataFrame with columns:
      - journal_title
      - journal_abbr
      - Sourceid   (never NaN)
    """
    # 1) scopus_ref_6 → (journal_title, journal_abbr)
    df1 = (
        scopus_ref_6[['journal', 'journal_abbr']]
        .rename(columns={'journal': 'journal_title'})
    )
    
    # 2) scopus_df_2 → (journal_title, journal_abbr)
    df2 = scopus_df_2[['journal_title', 'journal_abbreviation']].rename(
        columns={'journal_abbreviation': 'journal_abbr'}
    )
    
    # 3) combine and dedupe
    combined = (
        pd.concat([df1, df2], ignore_index=True)
          .drop_duplicates(subset=['journal_title', 'journal_abbr'])
          .reset_index(drop=True)
    )
    
    # 4) build de-duplicated Scimago map
    scimago_map = (
        scimago_df[['Title', 'Sourceid']]
        .drop_duplicates(subset=['Title'])
        .rename(columns={'Title': 'journal_title'})
    )
    
    # 5) merge in Sourceid
    merged = combined.merge(scimago_map, on='journal_title', how='left')
    
    # 6) fill missing Sourceid with 1..n_missing
    missing = merged['Sourceid'].isna()
    n_missing = missing.sum()
    # assign sequential ints to the NaNs
    merged.loc[missing, 'Sourceid'] = range(1, n_missing + 1)
    
    return merged[['journal_title', 'journal_abbr', 'Sourceid']]
