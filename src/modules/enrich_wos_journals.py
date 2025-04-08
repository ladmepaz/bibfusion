import pandas as pd

def enrich_wos_journals(wos_df_4, scimago):
    """
    1) First merge:
       - Remove dots from 'source_title' in wos_df_4 and 'journal_abbr' in scimago.
       - Match wos_df_4['source_title_no_dots','year'] 
         with scimago['journal_abbr_no_dots','PY'].
       - Left-merge so all rows from wos_df_4 remain.

    2) Second merge (for missing categoria only):
       - Merge on wos_df_4['journal','year'] and scimago['journal','PY'] 
         where wos_df_4['categoria'] is still NaN.

    Returns
    -------
    pd.DataFrame : Merged DataFrame with the same rows as wos_df_4 plus the column 'categoria'.
    """

    # -----------------------------
    # STEP 1: First merge by abbreviations
    # -----------------------------
    # Create helper columns (remove dots, uppercase)
    scimago['journal_abbr_no_dots'] = (
        scimago['journal_abbr']
        .astype(str)                  # ensure string type
        .str.replace('.', '', regex=False)
        .str.upper()
        .str.strip()
    )
    wos_df_4['source_title_no_dots'] = (
        wos_df_4['source_title']
        .astype(str)
        .str.replace('.', '', regex=False)
        .str.upper()
        .str.strip()
    )

    # Perform the left merge on abbreviations
    merged_df = wos_df_4.merge(
        scimago[['journal_abbr_no_dots', 'PY', 'categoria']],
        how='left',
        left_on=['source_title_no_dots', 'year'],
        right_on=['journal_abbr_no_dots', 'PY']
    )

    # Remove temporary columns from Step 1
    merged_df.drop(
        columns=['journal_abbr_no_dots', 'source_title_no_dots', 'PY'],
        inplace=True
    )

    # -----------------------------
    # STEP 2: Second merge by full journal name for rows still missing categoria
    # -----------------------------
    # 2a. Separate rows that have not matched a categoria
    missing_cat_mask = merged_df['categoria'].isna()
    missing_df = merged_df.loc[missing_cat_mask].copy()
    non_missing_df = merged_df.loc[~missing_cat_mask].copy()

    # 2b. Merge missing rows with scimago by 'journal' and 'PY' vs. 'year'
    #     (Note: scimago's full title is also called 'journal')
    updated_missing_df = missing_df.merge(
        scimago[['journal', 'PY', 'categoria']],
        how='left',
        left_on=['journal', 'year'],
        right_on=['journal', 'PY'],
        suffixes=('', '_scimago_journal')
    )

    # 2c. Fill any missing 'categoria' from this second match
    #     If we still don't get a match, it remains NaN.
    updated_missing_df['categoria'] = updated_missing_df['categoria'].fillna(
        updated_missing_df['categoria_scimago_journal']
    )

    # Optionally drop 'journal'/'PY' from scimago if you don't need them
    updated_missing_df.drop(columns=['PY', 'categoria_scimago_journal'], inplace=True)

    # -----------------------------
    # STEP 3: Concatenate updated missing rows with non-missing
    # -----------------------------
    final_df = pd.concat([non_missing_df, updated_missing_df], ignore_index=True)

    return final_df
