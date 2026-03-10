import pandas as pd
def merge_wos_ref(wos_df: pd.DataFrame, wos_ref_enriched: pd.DataFrame) -> pd.DataFrame:
    """
    Merge the main WoS dataframe (ideally already enriched with
    `enrich_wos_with_openalex_authors`) with the references dataframe
    produced by `enrich_references_with_openalex`.

    Steps:
    - Add 'ismainarticle' column with 'TRUE' to wos_df.
    - Add 'ismainarticle' column with 'FALSE' to wos_ref_enriched.
    - Process wos_ref_enriched: drop 'CR_ref', rename 'SR_ref'→'SR',
      and collapse duplicates on 'SR' preferring rows with valid 'doi'.
    - Align columns across dataframes (including OpenAlex fields like
      'author_id_openalex' and 'openalex_work_id').
    - Concatenate and de-duplicate on 'SR', keeping main articles first.

    Parameters:
    ----------
    wos_df : pd.DataFrame
        The main articles dataframe.
    wos_ref_enriched : pd.DataFrame
        The enriched references dataframe.

    Returns:
    -------
    pd.DataFrame
        The combined dataframe with 'ismainarticle' column and all modifications applied.
    """ 
    
    # Convert every string value to uppercase without affecting NaN or other types
    wos_ref_enriched = wos_ref_enriched.map(
    lambda x: x.upper() if isinstance(x, str) else x
    )

    
    # Step 1: Add 'ismainarticle' Columns to Both DataFrames
    wos_df['ismainarticle'] = 'TRUE'
    wos_ref_enriched['ismainarticle'] = 'FALSE'

    # Step 2: Modify wos_ref_enriched
    # Step 2a: Remove existing 'SR' Column (if present)
    if 'SR' in wos_ref_enriched.columns:
        wos_ref_enriched = wos_ref_enriched.drop(columns=['SR'])
    else:
        None  # 'SR' column not present; no action needed.

    # Step 2b: Rename 'SR_ref' to 'SR'
    if 'SR_ref' in wos_ref_enriched.columns:
        wos_ref_enriched = wos_ref_enriched.rename(columns={'SR_ref': 'SR'})
    else:
        None  # 'SR_ref' column not found; cannot rename to 'SR'.

    # Step 2c: Remove 'CR_ref' Column
    if 'CR_ref' in wos_ref_enriched.columns:
        wos_ref_enriched = wos_ref_enriched.drop(columns=['CR_ref'])
        None  # 'CR_ref' column removed from wos_ref_enriched.
    else:
        None  # 'CR_ref' column not found; cannot remove.

    # Step 2d: Remove Duplicates Based on 'SR' with Specific Logic for 'DI'
    if 'SR' in wos_ref_enriched.columns:
        None  # Handling duplicates in wos_ref_enriched based on 'SR' column.

        def prioritize_entries(group):
            # Entries with valid 'DI' (not null, not '-', not empty)
            valid_di = group['doi'].notnull() & (group['doi'] != '-') & (group['doi'] != '')
            valid_entries = group[valid_di]
            if not valid_entries.empty:
                # If multiple entries have valid 'DI', keep the first one
                return valid_entries.iloc[0]
            else:
                # If no valid 'DI', keep the first entry in the group
                return group.iloc[0]

        # Apply the function to each group of duplicates
        wos_ref_cleaned = wos_ref_enriched.groupby('SR', as_index=False).apply(prioritize_entries).reset_index(drop=True)
        
    else:
        wos_ref_cleaned = wos_ref_enriched.copy()
        

    # Step 3: Align Columns for Concatenation (keep OpenAlex fields)
    # Get list of all columns in both dataframes
    wos_df_columns = set(wos_df.columns)
    wos_ref_columns = set(wos_ref_cleaned.columns)

    # Find columns missing in wos_ref_cleaned
    missing_in_refs = wos_df_columns - wos_ref_columns
    for col in missing_in_refs:
        wos_ref_cleaned[col] = pd.NA  # Ensure consistent missing value

    # Find columns missing in wos_df
    missing_in_wos_df = wos_ref_columns - wos_df_columns
    for col in missing_in_wos_df:
        wos_df[col] = pd.NA  # Ensure consistent missing value

    # Ensure both dataframes have the same column order
    wos_ref_cleaned = wos_ref_cleaned[wos_df.columns]

    # Aligned columns between wos_df and wos_ref_cleaned for concatenation.

    # Step 4: Concatenate DataFrames
    combined_df = pd.concat([wos_df, wos_ref_cleaned], ignore_index=True)
    # "Concatenated wos_df and wos_ref_cleaned into combined_df.")

    # Step 5: Remove reference duplicates WITHOUT touching main articles

    if 'SR' in combined_df.columns:

        main_df = combined_df[combined_df['ismainarticle'] == 'TRUE']
        ref_df  = combined_df[combined_df['ismainarticle'] == 'FALSE']

        # Remove references that have same SR as a main article
        ref_df = ref_df[~ref_df['SR'].isin(main_df['SR'])]

        # Remove duplicate references among themselves
        ref_df = ref_df.drop_duplicates(subset='SR', keep='first')

        # Combine back
        combined_df = pd.concat([main_df, ref_df], ignore_index=True)

    else:
        pass

    return combined_df
