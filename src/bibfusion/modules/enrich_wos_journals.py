import pandas as pd

def enrich_wos_journals(wos_df_4, scimago):
    """
    Enriches wos_df_4 with the following columns from scimago:
      'SJR',
      'SJR Best Quartile',
      'H index',
      'Total Docs. (1999)',
      'Total Docs. (3years)',
      'Total Refs.',
      'Total Citations (3years)',
      'Citable Docs. (3years)',
      'Citations / Doc. (2years)',
      'Ref. / Doc.',
      '%Female',
      'Overton',
      'SDG',
      'Country',
      'Region',
      'Publisher',
      'Coverage',
      'Categories',
      'Areas'
    
    Two-stage merge approach:
      1) [source_title_no_dots, year] => [journal_abbr_no_dots, year]
      2) For rows still missing these columns, [journal, year] => [journal_abbr, year]

    At the end, returns the same columns as wos_df_4 plus the new columns above.
    The row count is the same as wos_df_4.
    """

    # Copy input so we don't modify in place
    df = wos_df_4.copy()

    # Convert 'year' to string in both dataframes for consistent merging
    df['year'] = df['year'].astype(str)
    scimago['year'] = scimago['year'].astype(str)

    # Columns we want to bring in from scimago
    new_cols = [
        'SJR',
        'SJR Best Quartile',
        'H index',
        'Total Docs. (1999)',
        'Total Docs. (3years)',
        'Total Refs.',
        'Total Citations (3years)',
        'Citable Docs. (3years)',
        'Citations / Doc. (2years)',
        'Ref. / Doc.',
        '%Female',
        'Overton',
        'SDG',
        'Country',
        'Region',
        'Publisher',
        'Coverage',
        'Categories',
        'Areas'
    ]

    # Ensure these columns exist in scimago (set to NaN if not)
    for col in new_cols:
        if col not in scimago.columns:
            scimago[col] = pd.NA

    # Ensure scimago has 'journal_abbr_no_dots'
    if 'journal_abbr_no_dots' not in scimago.columns:
        # Build it from 'journal_abbr'
        scimago['journal_abbr_no_dots'] = (
            scimago['journal_abbr']
            .astype(str)
            .str.replace('.', '', regex=False)
            .str.upper()
            .str.strip()
        )
    else:
        scimago['journal_abbr_no_dots'] = (
            scimago['journal_abbr_no_dots']
            .astype(str)
            .str.upper()
            .str.strip()
        )

    # Drop duplicates in scimago for pass 1
    scimago_pass1 = scimago.drop_duplicates(subset=['journal_abbr_no_dots', 'year'])

    # Create 'source_title_no_dots' in wos_df_4
    df['source_title_no_dots'] = (
        df['source_title'].astype(str)
        .str.replace('.', '', regex=False)
        .str.upper()
        .str.strip()
    )

    # ----------------------------------------------------------------
    # MERGE PASS 1: [source_title_no_dots, year] => [journal_abbr_no_dots, year]
    # ----------------------------------------------------------------
    # We'll bring 'journal_abbr_no_dots', 'year', and all the new_cols
    pass1_cols = ['journal_abbr_no_dots', 'year'] + new_cols
    merged1 = df.merge(
        scimago_pass1[pass1_cols],
        how='left',
        left_on=['source_title_no_dots', 'year'],
        right_on=['journal_abbr_no_dots', 'year'],
        validate='many_to_one',   # scimago should have 0-1 match
        suffixes=('', '_scm')
    )

    # For each new col, fill the merged1 col from _scm if needed
    # e.g. if merged1['SJR'] is empty, fill from merged1['SJR_scm']
    for col in new_cols:
        if f"{col}_scm" in merged1.columns:
            merged1[col] = merged1[col].fillna(merged1[f"{col}_scm"])
            merged1.drop(columns=[f"{col}_scm"], inplace=True, errors='ignore')

    # Drop temporary columns from pass 1
    merged1.drop(columns=['journal_abbr_no_dots', 'source_title_no_dots'], inplace=True, errors='ignore')

    # ----------------------------------------------------------------
    # MERGE PASS 2: For rows still missing these columns,
    #               [journal, year] => [journal_abbr, year]
    # ----------------------------------------------------------------
    # We'll define a mask for missing in ANY of the new_cols
    # If a row is missing ANY of the new columns, we'll try pass-2 merge
    missing_mask = False
    for col in new_cols:
        missing_mask = missing_mask | merged1[col].isna()

    missing_df = merged1.loc[missing_mask].copy()
    non_missing_df = merged1.loc[~missing_mask].copy()

    # Build pass-2 scimago dropping duplicates on [journal_abbr, year]
    scimago_pass2 = scimago.drop_duplicates(subset=['journal_abbr', 'year'])
    pass2_cols = ['journal_abbr', 'year'] + new_cols

    updated_missing = missing_df.merge(
        scimago_pass2[pass2_cols],
        how='left',
        left_on=['journal', 'year'],
        right_on=['journal_abbr', 'year'],
        validate='many_to_one',
        suffixes=('', '_scm')
    )

    # Fill from pass-2
    for col in new_cols:
        if f"{col}_scm" in updated_missing.columns:
            updated_missing[col] = updated_missing[col].fillna(updated_missing[f"{col}_scm"])
            updated_missing.drop(columns=[f"{col}_scm"], inplace=True, errors='ignore')

    # Drop pass-2 merge columns
    updated_missing.drop(columns=['journal_abbr'], inplace=True, errors='ignore')

    # Combine
    final_df = pd.concat([non_missing_df, updated_missing], ignore_index=True)

    # ----------------------------------------------------------------
    # BUILD FINAL COLUMNS: wos_df_4 + new_cols
    # ----------------------------------------------------------------
    original_cols = list(wos_df_4.columns)
    extended_cols = original_cols[:]
    for col in new_cols:
        if col not in extended_cols:
            extended_cols.append(col)

    # Some ephemeral columns might remain, so let's keep only the columns
    # we want: intersection of extended_cols and final_df.columns
    final_cols = [c for c in extended_cols if c in final_df.columns]

    final_df = final_df.reindex(columns=final_cols)

    return final_df
