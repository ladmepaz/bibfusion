import pandas as pd

def merge_scopus_ref(scopus_df: pd.DataFrame, scopus_ref_enriched: pd.DataFrame) -> pd.DataFrame:
    """
        Merges the original Scopus DataFrame with the OpenAlex-enriched references.
        Converts all text to uppercase and adjusts column mapping according to specifications.

        Args:
            scopus_df: Original Scopus DataFrame containing article data
            scopus_ref_enriched: DataFrame with OpenAlex-enriched references

        Returns:
            Combined DataFrame with the corresponding columns in uppercase
    """

    # Make a copy of the DataFrames to avoid modifying the originals
    scopus_copy = scopus_df.copy()
    refs_copy = scopus_ref_enriched.copy()
    # Add 'ismainarticle' column with TRUE for scopus_df and FALSE for scopus_ref_enriched
    scopus_copy['ismainarticle'] = "TRUE"
    refs_copy['ismainarticle'] = "FALSE"

    # Function to convert only text columns to uppercase
    def uppercase_text_columns(df):
        for col in df.columns:
            if df[col].dtype == 'object':  # Only for text columns
                df[col] = df[col].str.upper() if df[col].notna().any() else df[col]
        return df
    
    # Convert both DataFrames to uppercase
    scopus_copy = uppercase_text_columns(scopus_copy)
    refs_copy = uppercase_text_columns(refs_copy)
    
    # Rename columns in the references DataFrame according to corrected specifications
    column_mapping = {
        'authors': 'author_full_names',
        # 'journal': 'journal_title',
        # 'source_title': 'journal_abbreviation',
        'page': 'page_start',
        'openalex_url': 'link',
        'SR_ref': 'SR'
    }
    
    refs_copy = refs_copy.rename(columns=column_mapping)
    
    # Delete columns that are not needed
    columns_to_drop = ['doi_original', 'openalex_id', 'journal_issue_number', 'SR_original']
    refs_copy = refs_copy.drop(columns=[col for col in columns_to_drop if col in refs_copy.columns])
    
    # Ensure that the columns page_end and page_count are present
    if 'page_start' in refs_copy.columns:
        refs_copy['page_end'] = refs_copy['page_start']  # Assume the same page if no range
        refs_copy['page_count'] = 1  # Assume 1 page by default
    
    # Select only the columns that exist in both DataFrames
    common_columns = set(scopus_copy.columns) & set(refs_copy.columns)
    
    # Create a list of all unique columns from both DataFrames
    all_columns = list(set(scopus_copy.columns) | set(refs_copy.columns))
    
    # Reindex both DataFrames with all possible columns
    scopus_copy = scopus_copy.reindex(columns=all_columns)
    refs_copy = refs_copy.reindex(columns=all_columns)
    
    # Concatenate the DataFrames
    merged_df = pd.concat([scopus_copy, refs_copy], ignore_index=True)
    
    # Convert text columns again in case there were any modifications during the merge
    merged_df = uppercase_text_columns(merged_df)
    
    return merged_df