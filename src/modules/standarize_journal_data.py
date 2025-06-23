import pandas as pd
import numpy as np

def standarize_journal_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    1) For reference rows (ismainarticle==False), unify 'journal' per source_title by the most frequent
    2) Strip '-' from 'issn' and 'eissn' (if they exist)
    3) Fill missing issn/eissn within source_title groups, then within journal groups
    4) For each valid issn, harmonize source_title and journal to the most common within that issn
    5) Repeat step 4 for eissn (if exists)
    6) Return only ['SR','journal','source_title','issn','eissn'], preserving original row order
    
    Handles cases where eissn might not exist in the DataFrame
    """
    df = df.copy()

    # --- Step 1: fix missing journal in references ---
    if 'ismainarticle' in df.columns:
        ref = df['ismainarticle'] == False

        def pick_expansion(s: pd.Series) -> str:
            v = s.dropna().replace('', np.nan).dropna()
            if v.empty:
                return ''
            freq = v.value_counts().rename_axis('candidate').reset_index(name='count')
            freq['length'] = freq['candidate'].str.len()
            freq = freq.sort_values(['count','length'], ascending=[False,False])
            return freq.iloc[0]['candidate']

        df.loc[ref, 'journal'] = (
            df.loc[ref]
              .groupby('source_title')['journal']
              .transform(pick_expansion)
        )

    # --- Step 2: strip hyphens from issn/eissn if they exist ---
    for col in ['issn', 'eissn']:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace('-', '', regex=False)
                .replace({'nan': np.nan, 'None': np.nan})
            )

    # Reduce to only the columns we want (eissn optional)
    output_cols = ['SR', 'journal', 'source_title', 'issn']
    if 'eissn' in df.columns:
        output_cols.append('eissn')
    out = df[output_cols].copy()

    # --- Helpers for mode-based filling ---
    def fill_with_mode(series: pd.Series) -> pd.Series:
        v = series.dropna()
        if not v.empty:
            mode = v.mode().iloc[0] if not v.mode().empty else np.nan
            return series.fillna(mode)
        return series

    def pick_mode(series: pd.Series) -> pd.Series:
        m = series.mode()
        if not m.empty:
            return m.iloc[0]
        # fallback to first non-null
        return series.dropna().iloc[0] if series.dropna().any() else np.nan

    # --- Steps 3-4: fill missing issn/eissn by source_title then journal ---
    for col in ['issn', 'eissn']:
        if col in out.columns:
            # Fill within source_title groups
            out[col] = (
                out
                .groupby('source_title', group_keys=False)[col]
                .transform(fill_with_mode)
            )
            # Fill within journal groups
            out[col] = (
                out
                .groupby('journal', group_keys=False)[col]
                .transform(fill_with_mode)
            )

    # --- Step 5: harmonize by issn ---
    if 'issn' in out.columns:
        mask = out['issn'].notna()
        for field in ['source_title', 'journal']:
            out.loc[mask, field] = (
                out.loc[mask]
                .groupby('issn', group_keys=False)[field]
                .transform(pick_mode)
            )

    # --- Step 6: harmonize by eissn (if exists) ---
    if 'eissn' in out.columns:
        mask = out['eissn'].notna()
        for field in ['source_title', 'journal']:
            out.loc[mask, field] = (
                out.loc[mask]
                .groupby('eissn', group_keys=False)[field]
                .transform(pick_mode)
            )

    return out