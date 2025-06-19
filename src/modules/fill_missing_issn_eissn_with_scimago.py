import pandas as pd

def fill_missing_issn_eissn_with_scimago(wos_df: pd.DataFrame, scimago: pd.DataFrame) -> pd.DataFrame:
    """
    1) Abbreviation pass: normalize source_title ↔ journal_abbr,
       map each abbr → (Issn, eIssn), merge & fill both issn/eissn.
    2) Title pass: normalize journal ↔ Title,
       map each Title → (Issn, eIssn), merge & fill any remaining issn/eissn.
    Returns same row‑count & columns as wos_df, with blanks in issn/eissn filled where possible.
    """
    df = wos_df.copy()
    sc = scimago.copy()
    
    # Manejo de errores para Scopus DataFrame
    df['issn'] = df['issn'] if 'issn' in df.columns else pd.NA
    df['eissn'] = df['eissn'] if 'eissn' in df.columns else pd.NA

    # helper: pick first non-null
    def first_nonnull(s):
        return s.dropna().iloc[0] if not s.dropna().empty else pd.NA

    # --- STEP 1: abbreviation pass ---
    df['_abbr_key'] = (
        df['source_title'].astype(str)
          .str.replace('.', '', regex=False)
          .str.upper().str.strip()
    )
    sc['_abbr_key'] = (
        sc['journal_abbr'].astype(str)
          .str.replace('.', '', regex=False)
          .str.upper().str.strip()
    )
    abbr_map = (
        sc.groupby('_abbr_key', as_index=False)
          .agg({ 'Issn': first_nonnull, 'eIssn': first_nonnull })
    )
    df = df.merge(abbr_map, how='left', on='_abbr_key', suffixes=('','_abbr'))
    # fill from abbr
    df['issn']  = df['issn'].fillna(df['Issn'])
    df['eissn'] = df['eissn'].fillna(df['eIssn'])
    df.drop(columns=['_abbr_key','Issn','eIssn'], inplace=True, errors='ignore')

    # --- STEP 2: title pass ---
    df['_title_key'] = (
        df['journal'].astype(str)
          .str.upper().str.strip()
    )
    sc['_title_key'] = (
        sc['Title'].astype(str)
          .str.upper().str.strip()
    )
    title_map = (
        sc.groupby('_title_key', as_index=False)
          .agg({ 'Issn': first_nonnull, 'eIssn': first_nonnull })
    )
    df = df.merge(title_map, how='left', on='_title_key', suffixes=('','_title'))
    # fill any remaining
    df['issn']  = df['issn'].fillna(df['Issn'])
    df['eissn'] = df['eissn'].fillna(df['eIssn'])
    df.drop(columns=['_title_key','Issn','eIssn'], inplace=True, errors='ignore')

    # return same columns & order as input
    return df[wos_df.columns]
