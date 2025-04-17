import pandas as pd
import numpy as np

def fill_missing_journal_info(wos_df_6, scimago):
    """
    1) Standardize 'journal' by grouping on 'source_title'.
       For each group, pick the most frequent 'journal' value and
       replace all differing or missing ones with it.

    2) Fill missing fields for each 'journal' group:
       If there's exactly one non-null value for a column in the group, 
       fill missing values in that column with that single value.

    3) Merge on 'issn' to fill missing 'Sourceid' from scimago.
       - We drop duplicates in scimago on 'Issn' so each ISSN
         can match to at most one Sourceid row.
       - If wos_df_6 already has 'Sourceid', we fill missing from scimago.
         Otherwise, we create a new column.

    Returns
    -------
    pd.DataFrame
        A copy of wos_df_6 in which 'journal' is standardized,
        missing fields are filled within each 'journal' group,
        and missing 'Sourceid' is filled via scimago on 'issn'.
    """

    df = wos_df_6.copy()

    # --- STEP 1: Standardize 'journal' by grouping on 'source_title' ---
    if 'source_title' not in df.columns or 'journal' not in df.columns:
        raise ValueError("DataFrame must contain 'source_title' and 'journal' columns.")

    def unify_journal(group: pd.DataFrame) -> pd.DataFrame:
        # find the most frequent (non-null) journal
        counts = group['journal'].value_counts(dropna=False)
        if len(counts) == 0:
            return group
        most_freq_j = counts.index[0]
        # replace null or different values with the most freq
        group['journal'] = group['journal'].fillna(most_freq_j)
        group['journal'] = group['journal'].replace(to_replace='.*',
                                                    value=most_freq_j,
                                                    regex=True)
        return group

    df = df.groupby('source_title', group_keys=False).apply(unify_journal)

    # --- STEP 2: Fill missing fields by grouping on 'journal' ---
    def fill_in_journal_group(group: pd.DataFrame) -> pd.DataFrame:
        # For each column except 'journal', if there's exactly
        # one unique non-null value, fill missing with it
        for col in group.columns:
            if col == 'journal':
                continue
            unique_vals = group[col].dropna().unique()
            if len(unique_vals) == 1:
                group[col] = group[col].fillna(unique_vals[0])
        return group

    df = df.groupby('journal', group_keys=False).apply(fill_in_journal_group)

    # --- STEP 3: Merge on 'issn' to fill missing Sourceid ---
    # Let's assume scimago has columns 'Issn' and 'Sourceid'.
    # We rename 'Issn' -> 'issn' for consistency.
    if 'issn' not in df.columns:
        raise ValueError("DataFrame must contain 'issn' for merging with scimago on ISSN.")

    if 'Issn' not in scimago.columns or 'Sourceid' not in scimago.columns:
        raise ValueError("Scimago must contain 'Issn' and 'Sourceid' columns to fill missing Sourceid.")

    # Build scimago subset for the merge
    scimago_new = scimago[['Issn','Sourceid']].copy()
    scimago_new.drop_duplicates(subset=['Issn'], inplace=True)
    scimago_new.rename(columns={'Issn':'issn'}, inplace=True)

    # Perform the left merge to bring in scimago_new.Sourceid by issn
    merged_df = df.merge(
        scimago_new,
        how='left',
        on='issn',
        suffixes=('', '_scm')
    )

    # If wos_df_6 already had a 'Sourceid' column, fill missing from scimago
    if 'Sourceid' in merged_df.columns and 'Sourceid_scm' in merged_df.columns:
        merged_df['Sourceid'] = merged_df['Sourceid'].fillna(merged_df['Sourceid_scm'])
    else:
        # If no 'Sourceid' in original, rename from scimago suffix
        merged_df.rename(columns={'Sourceid_scm':'Sourceid'}, inplace=True)

    # Drop leftover suffix columns if any
    leftover_cols = [c for c in merged_df.columns if c.endswith('_scm')]
    merged_df.drop(columns=leftover_cols, inplace=True, errors='ignore')

    return merged_df
