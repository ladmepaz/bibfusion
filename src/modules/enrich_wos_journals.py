import pandas as pd
import numpy as np

def enrich_wos_journals(wos_df_4, scimago):
    """
    Enriches wos_df_4 with three columns from scimago: 'SJR', 'quartile', 'h_index'.
    Also creates a new 'journal_id' column (progressive from 1..n), placed first, 
    and moves 'SR' to the end of the DataFrame.

    Steps:
      1) Prepare/rename columns in scimago.
      2) Build dictionary-based lookups to avoid memory-heavy merges.
      3) Fill wos_df_4's missing SJR/quartile/h_index by:
         a) ISSN (no dashes)
         b) 'journal' => scimago['journal_abbr']
         c) 'source_title' => scimago['journal_abbr']
      4) Insert 'journal_id' as first column, reorder so that 'SR' is last.
    """

    df = wos_df_4.copy()

    # ------------------------------------------------------------------
    # 1) Prepare scimago columns
    # ------------------------------------------------------------------
    scimago = scimago.rename(columns={
        'SJR Best Quartile': 'quartile',
        'H index': 'h_index',
        'Issn': 'scimago_issn'
    })

    for col in ['scimago_issn', 'SJR', 'quartile', 'h_index', 'journal_abbr']:
        if col not in scimago.columns:
            scimago[col] = np.nan

    for col in ['SJR', 'quartile', 'h_index']:
        if col not in df.columns:
            df[col] = np.nan

    # ------------------------------------------------------------------
    # 2) Dictionary builders
    # ------------------------------------------------------------------
    def build_dict(df_sc, key_col):
        """
        key_col -> { 'SJR': ..., 'quartile': ..., 'h_index': ... }
        """
        sub = df_sc.dropna(subset=[key_col]).drop_duplicates(subset=[key_col])
        sub_indexed = sub.set_index(key_col)[['SJR', 'quartile', 'h_index']]
        return sub_indexed.to_dict(orient='index')

    # scimago: remove dashes for ISSN
    scimago['issn_no_dash'] = (
        scimago['scimago_issn'].astype(str)
        .str.replace('-', '', regex=False)
        .str.upper()
        .str.strip()
    )
    # scimago: remove dots for journal_abbr
    scimago['journal_abbr_no_dots'] = (
        scimago['journal_abbr'].astype(str)
        .str.replace('.', '', regex=False)
        .str.upper()
        .str.strip()
    )

    # Build dictionaries
    issn_dict = build_dict(scimago, 'issn_no_dash')
    abbr_dict = build_dict(scimago, 'journal_abbr_no_dots')

    # ------------------------------------------------------------------
    # 3) fill_from_dict helper
    # ------------------------------------------------------------------
    def fill_from_dict(df_w, map_dict, key_col, fill_cols=('SJR','quartile','h_index')):
        fill_mask = df_w['SJR'].isna() | df_w['quartile'].isna() | df_w['h_index'].isna()
        fill_info = df_w.loc[fill_mask, key_col].map(map_dict)

        for col in fill_cols:
            new_vals = fill_info.apply(
                lambda record: record.get(col, np.nan)
                if isinstance(record, dict) and pd.notna(record.get(col))
                else np.nan
            )
            df_w.loc[fill_mask, col] = df_w.loc[fill_mask, col].fillna(new_vals)
        return df_w

    # ------------------------------------------------------------------
    # 4) Pass 1: fill by ISSN
    # ------------------------------------------------------------------
    def pick_issn_no_dash(row):
        val = row['issn'] if pd.notna(row['issn']) else row['eissn']
        return str(val).replace('-', '').upper().strip() if pd.notna(val) else ''

    df['issn_no_dash'] = df.apply(pick_issn_no_dash, axis=1)
    df = fill_from_dict(df, issn_dict, 'issn_no_dash')

    # ------------------------------------------------------------------
    # 5) Pass 2: fill by wos_df_4['journal'] => scimago['journal_abbr']
    # ------------------------------------------------------------------
    df['journal_no_dots'] = (
        df['journal'].astype(str)
        .str.replace('.', '', regex=False)
        .str.upper()
        .str.strip()
    )
    df = fill_from_dict(df, abbr_dict, 'journal_no_dots')

    # ------------------------------------------------------------------
    # 6) Pass 3: fill by wos_df_4['source_title'] => scimago['journal_abbr']
    # ------------------------------------------------------------------
    df['source_title_no_dots'] = (
        df['source_title'].astype(str)
        .str.replace('.', '', regex=False)
        .str.upper()
        .str.strip()
    )
    df = fill_from_dict(df, abbr_dict, 'source_title_no_dots')

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------
    df.drop(columns=['issn_no_dash', 'journal_no_dots', 'source_title_no_dots'],
            inplace=True, errors='ignore')
    scimago.drop(columns=['issn_no_dash','journal_abbr_no_dots'],
                 inplace=True, errors='ignore')

    # ------------------------------------------------------------------
    # Insert 'journal_id' as first column, and move 'SR' to the end
    # ------------------------------------------------------------------
    # 1) Create progressive ID
    df.insert(0, 'journal_id', range(1, len(df) + 1))

    # 2) Reorder so 'SR' is last
    cols = list(df.columns)
    cols.remove('SR')
    cols.append('SR')
    df = df[cols]

    return df
