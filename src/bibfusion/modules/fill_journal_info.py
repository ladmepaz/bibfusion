import pandas as pd

def fill_journal_info(wos_df_4, scimago):
    """
    1) Fill missing [issn, eissn, journal_abbreviation] within each group of 'journal' in wos_df_4.
       - If a group has exactly one unique non-null value for that column, fill those missing.
    2) Create a minimal scimago DataFrame with columns:
         ['Title','Issn','eIssn','Sourceid','Rank'] (drop duplicates).
       - Rename Title->journal, Issn->issn, eIssn->eissn but keep Sourceid, Rank as-is.
    3) PASS C (by 'journal'):
       - Build scimago_pass_c = scimago_new[['journal','issn','eissn']].drop_duplicates(subset='journal')
       - Merge only rows missing issn in wos_df_4 with scimago_pass_c on 'journal'
         to fill missing issn & eissn. This uses how='left' + validate='many_to_one'.
    4) PASS D (by 'issn'):
       - Build scimago_pass_d = scimago_new[['issn','Sourceid','Rank']].drop_duplicates(subset='issn')
       - Merge final DataFrame on 'issn' to fill Sourceid/Rank (or create them).
         Also uses how='left' + validate='many_to_one'.
    5) Remove 'journal_abbreviation' if it exists at the end.

    Because each scimago Pass subset is drop_duplicates on the merge key, each
    wos_df_4 row can match at most one scimago row => row count is preserved.
    """

    df = wos_df_4.copy()

    # --------------------------
    # STEP A: Local in-group fill
    # --------------------------
    def fill_in_group(group: pd.DataFrame) -> pd.DataFrame:
        for col in ['issn', 'eissn', 'journal_abbreviation']:
            if col in group.columns:
                unique_vals = group[col].dropna().unique()
                if len(unique_vals) == 1:
                    group[col] = group[col].fillna(unique_vals[0])
        return group

    if 'journal' not in df.columns:
        raise ValueError("wos_df_4 must have a 'journal' column to group by.")

    df = df.groupby('journal', group_keys=False).apply(fill_in_group)

    # --------------------------
    # STEP B: Build scimago_new
    # --------------------------
    required = ['Title','Issn','eIssn','Sourceid','Rank']
    for col in required:
        if col not in scimago.columns:
            raise ValueError(f"scimago missing required column '{col}'.")

    scimago_new = scimago[required].copy()
    # We can drop duplicates on ALL columns if we wish,
    # but we will do separate pass subsets below:
    scimago_new.drop_duplicates(inplace=True)

    # Rename Title->journal, Issn->issn, eIssn->eissn
    scimago_new.rename(columns={
        'Title': 'journal',
        'Issn': 'issn',
        'eIssn': 'eissn'
    }, inplace=True)

    # ---------------
    # PASS C: Merge by 'journal' => fill missing issn/eissn
    # ---------------
    if 'issn' not in df.columns:
        raise ValueError("wos_df_4 must have 'issn' column.")

    # Subset scimago to one row per 'journal'
    # This ensures many wos rows => one scimago row => no row inflation
    scimago_pass_c = scimago_new[['journal','issn','eissn']].drop_duplicates(subset='journal')

    missing_issn_mask = df['issn'].isna()
    df_missing_issn = df.loc[missing_issn_mask].copy()
    df_not_missing_issn = df.loc[~missing_issn_mask].copy()

    merged_missing = df_missing_issn.merge(
        scimago_pass_c,
        how='left',
        on='journal',
        validate='many_to_one',   # ensures no row inflation
        suffixes=('', '_scm')
    )

    # fill in missing issn/eissn
    merged_missing['issn'] = merged_missing['issn'].fillna(merged_missing['issn_scm'])
    if 'eissn' in merged_missing and 'eissn_scm' in merged_missing:
        merged_missing['eissn'] = merged_missing['eissn'].fillna(merged_missing['eissn_scm'])

    # drop suffix columns
    drop_cols = [c for c in merged_missing.columns if c.endswith('_scm')]
    merged_missing.drop(columns=drop_cols, inplace=True, errors='ignore')

    df_filled = pd.concat([df_not_missing_issn, merged_missing], ignore_index=True)

    # ---------------
    # PASS D: Merge by 'issn' => fill Sourceid, Rank
    # ---------------
    scimago_pass_d = scimago_new[['issn','Sourceid','Rank']].drop_duplicates(subset='issn')

    final_df = df_filled.merge(
        scimago_pass_d,
        how='left',
        on='issn',
        validate='many_to_one',   # ensures same # of rows as wos_df_4
        suffixes=('', '_scm2')
    )

    # fill or create Sourceid, Rank
    if 'Sourceid' in final_df.columns and 'Sourceid_scm2' in final_df.columns:
        final_df['Sourceid'] = final_df['Sourceid'].fillna(final_df['Sourceid_scm2'])
    else:
        final_df.rename(columns={'Sourceid_scm2': 'Sourceid'}, inplace=True)

    if 'Rank' in final_df.columns and 'Rank_scm2' in final_df.columns:
        final_df['Rank'] = final_df['Rank'].fillna(final_df['Rank_scm2'])
    else:
        final_df.rename(columns={'Rank_scm2': 'Rank'}, inplace=True)

    # drop leftover suffix columns
    leftover_cols = [c for c in final_df.columns if c.endswith('_scm2')]
    final_df.drop(columns=leftover_cols, inplace=True, errors='ignore')

    # ---------------
    # Remove 'journal_abbreviation' if still exists
    # ---------------
    if 'journal_abbreviation' in final_df.columns:
        final_df.drop(columns='journal_abbreviation', inplace=True)

    return final_df
