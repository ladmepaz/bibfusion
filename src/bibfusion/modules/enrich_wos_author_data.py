import pandas as pd
import re

def enrich_wos_author_data(wos_authors: pd.DataFrame) -> pd.DataFrame:
    """
    Enriches the wos_authors dataframe by filling missing Orcid and updating AuthorFullName
    where AuthorName equals AuthorFullName with the most common AuthorFullName for that AuthorName.

    Parameters:
    -----------
    wos_authors : pd.DataFrame
        The dataframe output from get_wos_author_data, containing columns:
        ['SR', 'AuthorOrder', 'AuthorName', 'AuthorFullName', 'Affiliation',
         'CorrespondingAuthor', 'Orcid', 'ResearcherID', 'Email']

    Returns:
    --------
    wos_authors_enriched : pd.DataFrame
        The enriched dataframe with filled Orcid and updated AuthorFullName.
    """
    
    # Create a copy to avoid modifying the original dataframe
    enriched_df = wos_authors.copy()
    
    # Step 1: Exclude 'ANONYMOUS' authors
    authors_excl_anonymous = enriched_df[enriched_df['AuthorName'].str.upper() != 'ANONYMOUS']
    if authors_excl_anonymous.empty:
        #print("No authors found excluding 'ANONYMOUS'.")
        return enriched_df
    
    # Step 2: Identify unique AuthorNames (excluding 'ANONYMOUS')
    unique_author_names = authors_excl_anonymous['AuthorName'].unique()
    
    # Step 3: Iterate over each AuthorName to perform enrichment
    for author in unique_author_names:
        # Filter rows for the current AuthorName
        author_rows = enriched_df[enriched_df['AuthorName'] == author]
        
        # Step 3a: Determine the most common AuthorFullName for this AuthorName
        most_common_fullname = author_rows['AuthorFullName'].value_counts().idxmax()
        #print(f"AuthorName '{author}': Most common AuthorFullName is '{most_common_fullname}'")
        
        # Step 3b: Update AuthorFullName where AuthorName equals AuthorFullName
        condition_name_equals_fullname = (
            (enriched_df['AuthorName'] == author) &
            (enriched_df['AuthorFullName'] == author)
        )
        count_equals = condition_name_equals_fullname.sum()
        if count_equals > 0:
            enriched_df.loc[condition_name_equals_fullname, 'AuthorFullName'] = most_common_fullname
            #print(f"Updated {count_equals} rows of AuthorFullName for AuthorName '{author}'")
        
        # Step 3c: Determine the most common Orcid for this AuthorName
        # Exclude 'NO ORCID' and empty entries
        valid_orcids = author_rows[
            (author_rows['Orcid'].str.upper() != 'NO ORCID') &
            (author_rows['Orcid'].notnull()) &
            (author_rows['Orcid'] != '')
        ]['Orcid']
        
        if not valid_orcids.empty:
            most_common_orcid = valid_orcids.value_counts().idxmax()
            #print(f"AuthorName '{author}': Most common Orcid is '{most_common_orcid}'")
        else:
            most_common_orcid = ''
            #print(f"AuthorName '{author}': No valid Orcid found.")
        
        # Step 3d: Fill missing Orcid entries with the most common Orcid
        if most_common_orcid:
            condition_orcid_missing = (
                (enriched_df['AuthorName'] == author) &
                ((enriched_df['Orcid'].str.upper() == 'NO ORCID') |
                 (enriched_df['Orcid'] == '') |
                 (enriched_df['Orcid'].isnull()))
            )
            rows_to_fill = condition_orcid_missing.sum()
            if rows_to_fill > 0:
                enriched_df.loc[condition_orcid_missing, 'Orcid'] = most_common_orcid
                #print(f"Filled Orcid for {rows_to_fill} rows of AuthorName '{author}' with '{most_common_orcid}'.")
        else:
            #print(f"AuthorName '{author}': No Orcid to assign to missing entries.")
            None
    
    # Step 4: Ensure no trailing or leading spaces in 'AuthorName' and 'AuthorFullName'
    enriched_df['AuthorName'] = enriched_df['AuthorName'].astype(str).str.strip()
    enriched_df['AuthorFullName'] = enriched_df['AuthorFullName'].astype(str).str.strip()
    
    # Step 5: Return the enriched dataframe
    wos_authors_enriched = enriched_df.copy()
    
    return wos_authors_enriched
