import pandas as pd
import re

def extract_first_author(cr_ref):
    """
    Extracts the first author from a cited reference string.
    Handles both standard authors and anonymized entries like '[ANONYMOUS]'.
    
    Parameters:
    ----------
    cr_ref : str
        A single cited reference string.
    
    Returns:
    -------
    str
        The first author extracted from the reference, or '-' if not found or invalid type.
    """
    if not isinstance(cr_ref, str):
        return '-'  # Return a default value for non-string entries
    
    # Use regex to capture everything before the first comma
    match = re.match(r'\s*([^,]+),', cr_ref)
    if match:
        return match.group(1).strip()
    else:
        # If no comma is found, attempt to return the entire string as author
        # or assign '-' if the string is empty or invalid
        return cr_ref.strip() if cr_ref.strip() else '-'

def clean_author(author):
    """
    Cleans the author string by removing dots and square brackets.
    
    Parameters:
    ----------
    author : str
        The author string to be cleaned.
    
    Returns:
    -------
    str
        The cleaned author string.
    """
    if not isinstance(author, str):
        return '-'  # Return a default value for non-string entries
    
    # Remove dots
    author = author.replace('.', '')
    
    # Remove square brackets
    author = author.replace('[', '').replace(']', '')
    
    # Remove any extra whitespace
    author = author.strip()
    
    return author if author else '-'

def extract_year(cr_ref):
    """
    Extracts the publication year from a cited reference string.
    
    Parameters:
    ----------
    cr_ref : str
        A single cited reference string.
    
    Returns:
    -------
    str
        The publication year extracted from the reference, or '-' if not found.
    """
    if not isinstance(cr_ref, str):
        return '-'  # Return a default value for non-string entries
    
    # Search for a 4-digit year between 1900 and 2099
    match = re.search(r'\b(19|20)\d{2}\b', cr_ref)
    if match:
        return match.group(0)
    else:
        return '-'

def extract_journal(cr_ref, year):
    """
    Extracts the journal name from a cited reference string.
    
    Parameters:
    ----------
    cr_ref : str
        A single cited reference string.
    year : str
        The publication year extracted from the reference.
    
    Returns:
    -------
    str
        The journal name extracted from the reference, or '-' if not found.
    """
    if not isinstance(cr_ref, str):
        return '-'  # Return a default value for non-string entries
    
    # Split the reference by commas and strip whitespace
    parts = [part.strip() for part in cr_ref.split(',')]
    
    if year != '-':
        # Assume the format: Author, Year, Journal, ...
        if len(parts) >= 3:
            return parts[2]
    else:
        # Assume the format: Author, Journal, ...
        if len(parts) >= 2:
            return parts[1]
    
    return '-'  # Default if journal name not found

def extract_doi(cr_ref):
    """
    Extracts the DOI from a cited reference string.
    
    Parameters:
    ----------
    cr_ref : str
        A single cited reference string.
    
    Returns:
    -------
    str
        The DOI extracted from the reference, or '-' if not found.
    """
    if not isinstance(cr_ref, str):
        return '-'  # Return a default value for non-string entries
    
    # Define the DOI regex pattern
    doi_regex = r'\b10\.\d{4,9}/[-._;()/:A-Z0-9]+\b'
    
    # Find all DOI matches (case-insensitive)
    dois = re.findall(doi_regex, cr_ref, re.IGNORECASE)
    
    # Normalize DOIs to lowercase for consistency
    dois = [doi.lower() for doi in dois]
    
    if not dois:
        return '-'  # No DOI found
    
    # Check if all DOIs are identical
    if all(doi == dois[0] for doi in dois):
        return dois[0]
    else:
        # If DOIs are different, return the first one
        return dois[0]

def get_wos_references(wos_df_1):
    """
    Processes the WoS DataFrame to extract cited references with the first author, publication year, journal name, and DOI.
    Selects only the 'source_title' and 'references' columns, splits the 'references' strings by '; ',
    extracts the first author, cleans the 'authors' column by removing dots and brackets,
    extracts the publication year, extracts the journal name, extracts the DOI, and structures the data accordingly.
    Removes rows where all of CR_ref, authors, year, source_title, and doi are '-'.
    Creates a new column 'SR_ref' by concatenating 'authors', 'year', and 'source_title'.
    
    Parameters:
    ----------
    wos_df_1 : pd.DataFrame
        The input DataFrame containing WoS data with multiple columns, including 'source_title' and 'references'.
    
    Returns:
    -------
    pd.DataFrame
        A new DataFrame containing 'source_title', 'CR_ref', 'authors', 'year', 'source_title', 'doi', and 'SR_ref' columns, with 'authors' cleaned.
    
    Raises:
    ------
    ValueError
        If the input DataFrame does not contain the required 'source_title' and 'references' columns.
    """
    required_columns = ['source_title', 'references', 'SR']
    
    # Check if required columns are present in the input DataFrame
    missing_columns = [col for col in required_columns if col not in wos_df_1.columns]
    if missing_columns:
        raise ValueError(f"The input DataFrame is missing the following required column(s): {', '.join(missing_columns)}")
    
    # Select only 'source_title' and 'references' columns
    wos_df = wos_df_1[required_columns].copy()
    
    # Replace NaN values in 'references' with empty strings to prevent errors during splitting
    wos_df['references'] = wos_df['references'].fillna('')
    
    # Ensure all 'references' entries are strings
    wos_df['references'] = wos_df['references'].astype(str)
    
    # Split the 'references' column by '; ' to separate individual references
    wos_df['CR_ref'] = wos_df['references'].str.split('; ')
    
    # Explode the 'CR_ref' column to create a new row for each reference
    wos_exploded_df = wos_df.explode('CR_ref').reset_index(drop=True)
    
    # Remove any leading/trailing whitespace from 'CR_ref'
    wos_exploded_df['CR_ref'] = wos_exploded_df['CR_ref'].str.strip()
    
    # Replace empty 'CR_ref' entries with '-' to indicate missing references
    wos_exploded_df['CR_ref'] = wos_exploded_df['CR_ref'].replace('', '-')
    
    # Apply the 'extract_first_author' function to create the 'authors' column
    wos_exploded_df['authors'] = wos_exploded_df['CR_ref'].apply(extract_first_author)
    
    # Clean the 'authors' column by removing dots and brackets
    wos_exploded_df['authors'] = wos_exploded_df['authors'].apply(clean_author)
    
    # Apply the 'extract_year' function to create the 'year' column
    wos_exploded_df['year'] = wos_exploded_df['CR_ref'].apply(extract_year)
    
    # Apply the 'extract_journal' function to create the 'source_title' column
    wos_exploded_df['source_title'] = wos_exploded_df.apply(lambda row: extract_journal(row['CR_ref'], row['year']), axis=1)
    
    # Apply the 'extract_doi' function to create the 'doi' column
    wos_exploded_df['doi'] = wos_exploded_df['CR_ref'].apply(extract_doi)
    
    # Remove rows where all of CR_ref, authors, year, source_title, and doi are '-'
    wos_exploded_df = wos_exploded_df[~(wos_exploded_df[['CR_ref', 'authors', 'year', 'source_title', 'doi']] == '-').all(axis=1)]
    
    # Create 'SR_ref' by concatenating 'authors', 'year', and 'source_title'
    wos_exploded_df['SR_ref'] = wos_exploded_df[['authors', 'year', 'source_title']].agg(', '.join, axis=1)
    
    # Select and return only the 'source_title', 'CR_ref', 'authors', 'year', 'references', 'doi', and 'SR_ref' columns
    final_df = wos_exploded_df[['source_title', 'CR_ref', 'authors', 'year', 'doi', 'SR_ref']].copy()
    wos_citation = wos_exploded_df[['SR', 'SR_ref']].copy()
    
    return final_df, wos_citation
