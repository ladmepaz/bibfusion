import pandas as pd

def citation_scopus(df: pd.DataFrame) -> pd.DataFrame:
    """
    Generates a DataFrame of citation relationships between articles.

    Args:
        df: DataFrame containing the SR_original and SR_ref columns

    Returns:
        DataFrame with citation relationships (who cited whom)
        - SR: Source article (formerly SR_original)
        - SR_ref: Cited article
    """
    # Verify that the necessary columns exist
    if not all(col in df.columns for col in ['SR_original', 'SR_ref']):
        raise ValueError("The DataFrame must contain the columns 'SR_original' and 'SR_ref'")
    
    # Create the citation DataFrame
    citation_df = df[['SR_original', 'SR_ref']].copy()
    
    # Rename columns according to specifications
    citation_df = citation_df.rename(columns={'SR_original': 'SR'})
    
    # Remove rows where SR_ref is empty (NaN or None)
    citation_df = citation_df.dropna(subset=['SR_ref'])
    
    # Remove possible duplicates (repeated citations)
    citation_df = citation_df.drop_duplicates()
    
    # Reset index of the resulting DataFrame
    citation_df = citation_df.reset_index(drop=True)
    
    return citation_df