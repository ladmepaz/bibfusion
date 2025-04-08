import logging
import pandas as pd
import numpy as np

def merge_scopus_ref(scopus_df_1: pd.DataFrame, scopus_df_2: pd.DataFrame) -> pd.DataFrame:
    """
    Modifies scopus_df_1 and scopus_df_2 dataframes by:
    - Adding 'ismainarticle' column with 'TRUE' to scopus_df_1.
    - Adding 'ismainarticle' column with 'FALSE' to scopus_df_2.
    - Processing scopus_df_2 (removing duplicates, column adjustments).
    - Preserving all columns from scopus_df_1 while ensuring compatibility with scopus_df_2.
    - Concatenating scopus_df_1 and scopus_df_2 into a single dataframe.
    - Removing duplicates in 'SR' column, keeping entries where 'ismainarticle' == 'TRUE'.

    Parameters:
    ----------
    scopus_df_1 : pd.DataFrame
        The main articles dataframe.
    scopus_df_2 : pd.DataFrame
        The references dataframe.

    Returns:
    -------
    pd.DataFrame
        The combined dataframe with 'ismainarticle' column and all modifications applied,
        preserving all original columns from scopus_df_1.
    """

    # Configure logging if not already configured
    logging.basicConfig(
        filename='merge_scopus_ref.log',  # Log to a file
        filemode='a',                     # Append mode
        format='%(asctime)s - %(levelname)s - %(message)s',
        level=logging.INFO
    )

    logging.info("Starting merge_scopus_ref function.")

    # Step 1: Add 'ismainarticle' Columns to Both DataFrames
    scopus_df_1['ismainarticle'] = 'TRUE'
    scopus_df_2['ismainarticle'] = 'FALSE'
    logging.info("Added 'ismainarticle' column to both scopus_df_1 and scopus_df_2.")

    # Step 2: Modify scopus_df_2
    # Step 2a: Remove existing 'SR' Column (if present)
    if 'SR' in scopus_df_2.columns:
        scopus_df_2 = scopus_df_2.drop(columns=['SR'])
        logging.info("Removed existing 'SR' column from scopus_df_2.")
    else:
        logging.info("No existing 'SR' column to remove from scopus_df_2.")

    # Step 2b: Rename 'SR_ref' to 'SR'
    if 'SR_ref' in scopus_df_2.columns:
        scopus_df_2 = scopus_df_2.rename(columns={'SR_ref': 'SR'})
        logging.info("Renamed 'SR_ref' column to 'SR' in scopus_df_2.")
    else:
        logging.warning("'SR_ref' column not found in scopus_df_2; cannot rename to 'SR'.")

    # Step 2c: Remove 'CR_ref' Column (if applicable)
    if 'CR_ref' in scopus_df_2.columns:
        scopus_df_2 = scopus_df_2.drop(columns=['CR_ref'])
        logging.info("Removed 'CR_ref' column from scopus_df_2.")
    else:
        logging.info("'CR_ref' column not found in scopus_df_2; no need to remove.")

    # Step 2d: Remove Duplicates Based on 'SR' with Specific Logic for 'DI'
    if 'SR' in scopus_df_2.columns:
        logging.info("Handling duplicates in scopus_df_2 based on 'SR' column.")

        def prioritize_entries(group):
            # Entries with valid 'DI' (not null, not '-', not empty)
            if 'doi' in group.columns:
                valid_di = group['doi'].notnull() & (group['doi'] != '-') & (group['doi'] != '')
                valid_entries = group[valid_di]
                if not valid_entries.empty:
                    # If multiple entries have valid 'DI', keep the first one
                    return valid_entries.iloc[0]
            # If no valid 'DI', keep the first entry in the group
            return group.iloc[0]

        # Apply the function to each group of duplicates
        scopus_ref_cleaned = scopus_df_2.groupby('SR', as_index=False).apply(prioritize_entries).reset_index(drop=True)
        logging.info("Removed duplicates in scopus_df_2 based on 'SR' with priority given to entries with valid 'DI'.")
    else:
        scopus_ref_cleaned = scopus_df_2.copy()
        logging.warning("'SR' column not found in scopus_df_2; cannot remove duplicates based on 'SR'.")

    # Step 3: Prepare DataFrames for Concatenation
    # Get all columns from scopus_df_1 to preserve them
    all_columns = set(scopus_df_1.columns)
    
    # Add any columns from scopus_ref_cleaned that aren't in scopus_df_1
    for col in scopus_ref_cleaned.columns:
        if col not in all_columns:
            all_columns.add(col)
    
    # Ensure 'SR' and 'ismainarticle' are included
    all_columns.add('SR')
    all_columns.add('ismainarticle')
    
    all_columns = list(all_columns)
    logging.info(f"Prepared column list for concatenation: {all_columns}")
    
    # Add missing columns to each dataframe with NaN values
    for col in all_columns:
        if col not in scopus_df_1.columns:
            scopus_df_1[col] = np.nan
        if col not in scopus_ref_cleaned.columns:
            scopus_ref_cleaned[col] = np.nan
    
    logging.info("Added missing columns to both dataframes for proper concatenation.")

    # Step 4: Concatenate DataFrames
    combined_df = pd.concat([scopus_df_1, scopus_ref_cleaned], ignore_index=True)
    logging.info("Concatenated scopus_df_1 and scopus_ref_cleaned into combined_df.")

    # Step 5: Remove Duplicates in 'SR' Column, Keeping 'ismainarticle' == 'TRUE'
    if 'SR' in combined_df.columns:
        logging.info("Removing duplicates in combined_df based on 'SR', keeping entries where 'ismainarticle' == 'TRUE'.")

        # Sort the dataframe so that rows with 'ismainarticle' == 'TRUE' come first
        combined_df['ismainarticle_order'] = combined_df['ismainarticle'] == 'TRUE'
        combined_df = combined_df.sort_values(by=['ismainarticle_order'], ascending=False)

        # Remove duplicates based on 'SR', keeping the first occurrence
        combined_df = combined_df.drop_duplicates(subset='SR', keep='first')

        # Drop the temporary column
        combined_df = combined_df.drop(columns=['ismainarticle_order'])

        logging.info("Removed duplicates in combined_df based on 'SR'.")
    else:
        logging.warning("'SR' column not found in combined_df; cannot remove duplicates based on 'SR'.")

    logging.info("merge_scopus_ref function completed successfully.")
    return combined_df