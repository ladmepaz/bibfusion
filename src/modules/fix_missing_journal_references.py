import pandas as pd
import numpy as np

def fix_missing_journal_references(wos_df_3):
    """
    Group-by 'source_title' in references (where ismainarticle == False),
    and unify/fill the 'journal' column using the most frequent 'journal'
    expansion (with length as a tiebreaker).
    
    Additionally, removes '-' from 'issn' and 'eissn' columns.
    
    In the end, returns a dataframe with these columns (in this order):
    SR
    journal
    source_title
    issn
    eissn
    """
    df = wos_df_3.copy()

    # 1) Identify reference rows
    ref_mask = (df['ismainarticle'] == False)
    # 2) Extract the relevant columns for reference rows
    refs = df.loc[ref_mask, ['source_title', 'journal']].copy()

    # 3) Helper function to pick a single expansion from a group
    def pick_expansion(series: pd.Series) -> str:
        # Drop empty or missing expansions
        valid = series.dropna().replace('', float('nan')).dropna()
        if len(valid) == 0:
            return ''
        freq = valid.value_counts().reset_index()
        freq.columns = ['candidate_journal', 'count']
        freq['length'] = freq['candidate_journal'].str.len()
        # Sort by frequency desc, then length desc
        freq = freq.sort_values(by=['count', 'length'], ascending=[False, False])
        return freq.iloc[0]['candidate_journal']

    # 4) Generate unified journal names
    chosen_expansions = (
        refs.groupby('source_title')['journal']
            .transform(pick_expansion)
    )

    # 5) Overwrite in the original df only for reference rows
    df.loc[ref_mask, 'journal'] = chosen_expansions

    # 6) Remove '-' from 'issn' and 'eissn' columns
    if 'issn' in df.columns:
        df['issn'] = df['issn'].str.replace('-', '', regex=False)
    if 'eissn' in df.columns:
        df['eissn'] = df['eissn'].str.replace('-', '', regex=False)

    # 7) Ensure required columns exist
    final_columns = [
    'SR',
    'journal',
    'source_title',
    'issn',
    'eissn'
    ]
    for col in final_columns:
        if col not in df.columns:
            df[col] = np.nan  # or '' if you prefer empty strings
    
    # 8) Return only the requested columns in the specified order
    return df[final_columns]