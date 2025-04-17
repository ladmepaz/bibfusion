import pandas as pd
import numpy as np

def standarize_journal_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    1) For reference rows (ismainarticle==False), unify 'journal' per source_title by the most frequent (tiebreak on length).
    2) Strip '-' from 'issn' and 'eissn'.
    3) Fill missing issn/eissn within source_title groups, then within journal groups.
    4) For each valid issn, harmonize source_title and journal to the most common within that issn.
    5) Repeat step 4 for eissn.
    6) Return only ['SR','journal','source_title','issn','eissn'], preserving original row order.
    """
    df = df.copy()

    # --- Step 1: fix missing journal in references ---
    if 'ismainarticle' in df.columns:
        ref = df['ismainarticle']==False

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

    # --- Step 2: strip hyphens ---
    for col in ('issn','eissn'):
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace('-', '', regex=False).replace({'nan':np.nan})

    # Reduce to only the five columns we want
    out = df[['SR','journal','source_title','issn','eissn']].copy()

    # --- Helpers for mode-based filling ---
    def fill_with_mode(series: pd.Series) -> pd.Series:
        v = series.dropna()
        if not v.empty:
            mode = v.mode().iloc[0]
            return series.fillna(mode)
        return series

    def pick_mode(series: pd.Series) -> pd.Series:
        m = series.mode()
        if not m.empty:
            return m.iloc[0]
        # fallback to first non-null
        return series.dropna().iloc[0] if series.dropna().any() else np.nan

    # --- Step 3: fill missing issn/eissn by source_title ---
    for col in ('issn','eissn'):
        if col in out.columns:
            out[col] = (
                out
                .groupby('source_title', group_keys=False)[col]
                .transform(fill_with_mode)
            )

    # --- Step 4: fill missing issn/eissn by journal ---
    for col in ('issn','eissn'):
        if col in out.columns:
            out[col] = (
                out
                .groupby('journal', group_keys=False)[col]
                .transform(fill_with_mode)
            )

    # --- Step 5: harmonize source_title/journal by issn ---
    if 'issn' in out.columns:
        mask = out['issn'].notna()
        # source_title
        out.loc[mask, 'source_title'] = (
            out.loc[mask]
               .groupby('issn', group_keys=False)['source_title']
               .transform(pick_mode)
        )
        # journal
        out.loc[mask, 'journal'] = (
            out.loc[mask]
               .groupby('issn', group_keys=False)['journal']
               .transform(pick_mode)
        )

    # --- Step 6: harmonize source_title/journal by eissn ---
    if 'eissn' in out.columns:
        mask = out['eissn'].notna()
        # source_title
        out.loc[mask, 'source_title'] = (
            out.loc[mask]
               .groupby('eissn', group_keys=False)['source_title']
               .transform(pick_mode)
        )
        # journal
        out.loc[mask, 'journal'] = (
            out.loc[mask]
               .groupby('eissn', group_keys=False)['journal']
               .transform(pick_mode)
        )

    # Ensure we return exactly these five, original row order preserved
    return out
