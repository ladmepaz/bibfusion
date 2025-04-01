import pandas as pd

def fix_missing_journal_references(wos_df_3):
    """
    Group-by 'source_title' in references (where ismainarticle == False),
    and unify/fill the 'journal' column using the most frequent 'journal'
    expansion (with length as a tiebreaker).
    
    Parameters
    ----------
    wos_df_3 : pd.DataFrame
        The dataframe that contains columns 'ismainarticle', 'source_title', and 'journal'.
        
    Returns
    -------
    pd.DataFrame
        A copy of wos_df_3 with updated 'journal' values in reference rows.
    """
    # 1) Make a copy if we don't want to mutate the original
    df = wos_df_3.copy()

    # 2) Extract only the reference rows
    ref_mask = (df['ismainarticle'] == False)
    refs = df.loc[ref_mask, ['source_title', 'journal']].copy()

    # 3) Define helper function to pick a single expansion from a group
    def pick_expansion(series: pd.Series) -> str:
        """
        Among all non-empty expansions in 'series', pick the best one:
          - The one that occurs most often (highest frequency),
          - If there is a tie on frequency, pick the one with the greatest length.
        If series is entirely missing, returns an empty string or NaN (your choice).
        """
        # Drop empty or fully missing expansions
        valid = series.dropna().replace('', float('nan')).dropna()
        if len(valid) == 0:
            return ''  # or return float('nan') if you prefer
        freq = valid.value_counts()
        freq_df = freq.reset_index()
        freq_df.columns = ['candidate_journal', 'count']
        # measure string length
        freq_df['length'] = freq_df['candidate_journal'].str.len()
        # sort by frequency desc, then length desc
        freq_df = freq_df.sort_values(by=['count', 'length'], ascending=[False, False])
        # top candidate
        best_journal = freq_df.iloc[0]['candidate_journal']
        return best_journal

    # 4) Group by source_title → transform 'journal' via our pick_expansion logic
    chosen_expansions = (
        refs.groupby('source_title')['journal']
            .transform(pick_expansion)
    )
    # 'chosen_expansions' is a series aligned with refs, containing the chosen fill

    # 5) Overwrite the old 'journal' in the reference subset with the new unified values
    df.loc[ref_mask, 'journal'] = chosen_expansions

    df = df[['SR', 'issn', 'source_title', 'journal_abbreviation', 'journal']]

    # Return the updated dataframe
    return df


# ------------------------------------------------------------------------
# Example usage (uncomment if you want to test):
#
# data = {
#     'ismainarticle': [False, False, False, True],
#     'source_title': ['J CONSUM RES', 'J CONSUM RES', 'J CONSUM SCI', 'NONREFERENCE'],
#     'journal': [None, 'JOURNAL OF CONSUMER RESEARCH', '', 'NOT USED'],
# }
# wos_df_3 = pd.DataFrame(data)
# print("Before:")
# print(wos_df_3)
#
# updated = fix_missing_journal_references(wos_df_3)
# print("\nAfter:")
# print(updated)
