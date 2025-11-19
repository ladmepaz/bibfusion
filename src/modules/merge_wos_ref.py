import pandas as pd
def merge_wos_ref(wos_df: pd.DataFrame, wos_ref_enriched: pd.DataFrame) -> pd.DataFrame:
    """
    Modifies wos_df and wos_ref_enriched dataframes by:
    - Adding 'ismainarticle' column with 'TRUE' to wos_df.
    - Adding 'ismainarticle' column with 'FALSE' to wos_ref_enriched.
    - Processing wos_ref_enriched (removing duplicates, column adjustments).
    - Aligning columns between the two dataframes for concatenation.
    - Concatenating wos_df and wos_ref_enriched into a single dataframe.
    - Removing duplicates in 'SR' column, keeping entries where 'ismainarticle' == 'TRUE'.

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
    
    # Convertir todos los valores tipo string en mayúsculas sin afectar NaN u otros tipos
    wos_ref_enriched = wos_ref_enriched.applymap(lambda x: x.upper() if isinstance(x, str) else x)
    
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
        

    # Step 3: Align Columns for Concatenation
    # Get list of all columns in both dataframes
    wos_df_columns = set(wos_df.columns)
    wos_ref_columns = set(wos_ref_cleaned.columns)

    # Find columns missing in wos_ref_cleaned
    missing_in_refs = wos_df_columns - wos_ref_columns
    for col in missing_in_refs:
        wos_ref_cleaned[col] = None  # Assign None or appropriate default value

    # Find columns missing in wos_df
    missing_in_wos_df = wos_ref_columns - wos_df_columns
    for col in missing_in_wos_df:
        wos_df[col] = None  # Assign None or appropriate default value

    # Ensure both dataframes have the same column order
    wos_ref_cleaned = wos_ref_cleaned[wos_df.columns]

    # Aligned columns between wos_df and wos_ref_cleaned for concatenation.

    # Step 4: Concatenate DataFrames
    combined_df = pd.concat([wos_df, wos_ref_cleaned], ignore_index=True)
    # "Concatenated wos_df and wos_ref_cleaned into combined_df.")

    # Step 5: Remove Duplicates in 'SR' Column, Keeping 'ismainarticle' == 'TRUE'
    if 'SR' in combined_df.columns:
        # Removing duplicates in combined_df based on 'SR', keeping entries where 'ismainarticle' == 'TRUE'.

        # Sort the dataframe so that rows with 'ismainarticle' == 'TRUE' come first
        combined_df['ismainarticle_order'] = combined_df['ismainarticle'] == 'TRUE'
        combined_df = combined_df.sort_values(by=['ismainarticle_order'], ascending=False)

        # Remove duplicates based on 'SR', keeping the first occurrence
        combined_df = combined_df.drop_duplicates(subset='SR', keep='first')

        # Drop the temporary 'ismainarticle_order' column
        combined_df = combined_df.drop(columns=['ismainarticle_order'])

        # Removed duplicates in combined_df based on 'SR'.
    else:
        # "'SR' column not found in combined_df; cannot remove duplicates based on 'SR'."
        None
    return combined_df
