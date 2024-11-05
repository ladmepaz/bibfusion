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

def extract_journal(cr_ref, PY):
    """
    Extracts the journal name from a cited reference string.
    
    Parameters:
    ----------
    cr_ref : str
        A single cited reference string.
    PY : str
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
    
    if PY != '-':
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
    Selects only the 'SR' and 'CR' columns, splits the 'CR' strings by '; ',
    extracts the first author, cleans the 'AU' column by removing dots and brackets,
    extracts the publication year, extracts the journal name, extracts the DOI, and structures the data accordingly.
    Removes rows where all of CR_ref, AU, PY, J9, and DI are '-'.
    Creates a new column 'SR_ref' by concatenating 'AU', 'PY', and 'J9'.
    
    Parameters:
    ----------
    wos_df_1 : pd.DataFrame
        The input DataFrame containing WoS data with multiple columns, including 'SR' and 'CR'.
    
    Returns:
    -------
    pd.DataFrame
        A new DataFrame containing 'SR', 'CR_ref', 'AU', 'PY', 'J9', 'DI', and 'SR_ref' columns, with 'AU' cleaned.
    
    Raises:
    ------
    ValueError
        If the input DataFrame does not contain the required 'SR' and 'CR' columns.
    """
    required_columns = ['SR', 'CR']
    
    # Check if required columns are present in the input DataFrame
    missing_columns = [col for col in required_columns if col not in wos_df_1.columns]
    if missing_columns:
        raise ValueError(f"The input DataFrame is missing the following required column(s): {', '.join(missing_columns)}")
    
    # Select only 'SR' and 'CR' columns
    wos_df = wos_df_1[required_columns].copy()
    
    # Replace NaN values in 'CR' with empty strings to prevent errors during splitting
    wos_df['CR'] = wos_df['CR'].fillna('')
    
    # Ensure all 'CR' entries are strings
    wos_df['CR'] = wos_df['CR'].astype(str)
    
    # Split the 'CR' column by '; ' to separate individual references
    wos_df['CR_ref'] = wos_df['CR'].str.split('; ')
    
    # Explode the 'CR_ref' column to create a new row for each reference
    wos_exploded_df = wos_df.explode('CR_ref').reset_index(drop=True)
    
    # Remove any leading/trailing whitespace from 'CR_ref'
    wos_exploded_df['CR_ref'] = wos_exploded_df['CR_ref'].str.strip()
    
    # Replace empty 'CR_ref' entries with '-' to indicate missing references
    wos_exploded_df['CR_ref'] = wos_exploded_df['CR_ref'].replace('', '-')
    
    # Apply the 'extract_first_author' function to create the 'AU' column
    wos_exploded_df['AU'] = wos_exploded_df['CR_ref'].apply(extract_first_author)
    
    # Clean the 'AU' column by removing dots and brackets
    wos_exploded_df['AU'] = wos_exploded_df['AU'].apply(clean_author)
    
    # Apply the 'extract_year' function to create the 'PY' column
    wos_exploded_df['PY'] = wos_exploded_df['CR_ref'].apply(extract_year)
    
    # Apply the 'extract_journal' function to create the 'J9' column
    wos_exploded_df['J9'] = wos_exploded_df.apply(lambda row: extract_journal(row['CR_ref'], row['PY']), axis=1)
    
    # Apply the 'extract_doi' function to create the 'DI' column
    wos_exploded_df['DI'] = wos_exploded_df['CR_ref'].apply(extract_doi)
    
    # Remove rows where all of CR_ref, AU, PY, J9, and DI are '-'
    wos_exploded_df = wos_exploded_df[~(wos_exploded_df[['CR_ref', 'AU', 'PY', 'J9', 'DI']] == '-').all(axis=1)]
    
    # Create 'SR_ref' by concatenating 'AU', 'PY', and 'J9'
    wos_exploded_df['SR_ref'] = wos_exploded_df[['AU', 'PY', 'J9']].agg(', '.join, axis=1)
    
    # Select and return only the 'SR', 'CR_ref', 'AU', 'PY', 'J9', 'DI', and 'SR_ref' columns
    final_df = wos_exploded_df[['SR', 'CR_ref', 'AU', 'PY', 'J9', 'DI', 'SR_ref']].copy()
    
    return final_df
