import pandas as pd
import re
import urllib.parse

def extract_authors(cr_ref):
    """
    Extracts author from the reference string and returns the remaining text.

    Parameters:
    ----------
    cr_ref : str
        A reference string from which to extract author.

    Returns:
    -------
    tuple
        - authors_str: A semicolon-separated string of author in the format 'LASTNAME INITIALS'.
        - remaining_text: The remaining text of cr_ref after the author are extracted.
    """
    tokens = cr_ref.split(', ')
    author_list = []  # Debe mantenerse como lista hasta el return
    i = 0
    while i < len(tokens):
        last_name = tokens[i]
        if i + 1 < len(tokens):
            initials = tokens[i + 1]
            last_name_pattern = r'^[A-Z][A-Za-z\'\-]*$'  # Allows for hyphens and apostrophes
            initials_pattern = r'^([A-Z](?:\.[A-Z])*\.?\s*)+$'  # Matches initials like 'C.M.'
            if re.match(last_name_pattern, last_name) and re.match(initials_pattern, initials):
                # Clean initials by removing periods and spaces
                initials_clean = re.sub(r'[.\s]', '', initials)
                author = f"{last_name} {initials_clean}"  # Crear un string para el autor actual
                author_list.append(author)  # Añadir el autor a la lista
                i += 2
            else:
                break
        else:
            break
    remaining_text = ', '.join(tokens[i:])
    return ';'.join(author_list), remaining_text

def extract_doi(text):
    """
    Extracts the DOI from the text if present and decodes URL-encoded characters.

    Parameters:
    ----------
    text : str
        The text from which to extract the DOI.

    Returns:
    -------
    tuple
        - doi: The extracted and decoded DOI or empty string if not found.
        - text: The text after removing the DOI.
    """
    # Search for 'DOI:' followed by '10.' and non-whitespace characters
    doi_match = re.search(r'DOI:\s*(10\.\S+)', text, flags=re.IGNORECASE)
    if doi_match:
        doi = doi_match.group(1)
        # Remove the DOI from the text
        text = text.replace(doi_match.group(0), '')
    else:
        # Alternatively, search for '10.' directly
        doi_match = re.search(r'(10\.\S+)', text)
        if doi_match:
            doi = doi_match.group(1)
            # Remove the DOI from the text
            text = text.replace(doi, '')
        else:
            doi = ''
    # Decode URL-encoded characters in the DOI
    doi = urllib.parse.unquote(doi)
    # Remove any trailing commas or periods from the DOI
    doi = doi.rstrip('.,')
    text = text.strip(' ,.')
    return doi, text

def clean_remaining_text(text):
    """
    Cleans the remaining text by removing URLs and unnecessary punctuation.

    Parameters:
    ----------
    text : str
        The text to be cleaned.

    Returns:
    -------
    str
        The cleaned text.
    """
    # Remove URLs
    text = re.sub(r'https?://\S+', '', text, flags=re.IGNORECASE)
    # Remove multiple commas and spaces
    text = re.sub(r',\s*,', ',', text)
    text = re.sub(r'\s+', ' ', text)
    text = text.strip(' ,.')
    return text

def extract_year(text):
    """
    Extracts the publication year from the text.

    Parameters:
    ----------
    text : str
        The text from which to extract the year.

    Returns:
    -------
    tuple
        - year: The publication year as a string.
        - text_before_year: Text before the year.
        - text_after_year: Text after the year.
    """
    year_match = re.search(r'\((\d{4})\)', text)
    if year_match:
        year = year_match.group(1)
        text_before_year = text[:year_match.start()].strip(' ,.')
        text_after_year = text[year_match.end():].strip(' ,.')
    else:
        year = ''
        text_before_year = text.strip(' ,.')
        text_after_year = ''
    return year, text_before_year, text_after_year

def extract_title_and_journal(text_before_year, text_after_year):
    """
    Extracts the title and journal name from the text.

    Parameters:
    ----------
    text_before_year : str
        Text before the publication year.
    text_after_year : str
        Text after the publication year.

    Returns:
    -------
    tuple
        - title: The extracted title.
        - journal: The extracted journal name.
    """
    # Clean text_after_year
    text_after_year = clean_remaining_text(text_after_year)
    # Remove volume/issue/page information from text_after_year
    text_after_year_cleaned = re.sub(r'(\b\d+\b\s*\([^)]+\)\s*,?\s*)?|PP?\.\s*\d+.*$', '', text_after_year, flags=re.IGNORECASE).strip(' ,.')

    if text_before_year:
        title = text_before_year.strip(' ,.')
    else:
        # If text_before_year is empty, use text_after_year as title
        title = text_after_year_cleaned
        text_after_year_cleaned = ''

    journal = ''
    if text_after_year_cleaned:
        # Split text_after_year by commas
        parts = [part.strip() for part in text_after_year_cleaned.split(',') if part.strip()]
        if parts:
            # Assume first part is journal name
            journal = parts[0]

    # Clean the title and journal
    title = clean_title(title)
    journal = clean_journal(journal)

    return title, journal

def clean_title(title):
    """
    Cleans the title by removing unwanted trailing information.

    Parameters:
    ----------
    title : str
        The title to be cleaned.

    Returns:
    -------
    str
        The cleaned title.
    """
    patterns = [
        r'PP\..*', r'P\..*', r'DOI:.*', r'https?://.*',
        r'VOL\..*', r'NO\..*', r'ISSUE.*', r'\b\d+\b\s*\([^)]+\)'
    ]
    for pattern in patterns:
        title = re.sub(pattern, '', title, flags=re.IGNORECASE)
    title = title.strip(' ,.')
    return title

def clean_journal(journal):
    """
    Cleans the journal name by removing volume, issue, and page numbers,
    and removes dots from the journal name.

    Parameters:
    ----------
    journal : str
        The journal name to be cleaned.

    Returns:
    -------
    str
        The cleaned journal name.
    """
    patterns = [
        r'\b\d+\b\s*\([^)]+\)', r'\b\d+\b', r'\(.*?\)', r'PP\..*', r'P\..*',
        r'VOL\..*', r'NO\..*', r'ISSUE.*', r'\d+.*$'
    ]
    for pattern in patterns:
        journal = re.sub(pattern, '', journal, flags=re.IGNORECASE)
    # Remove dots from the journal name
    journal = journal.replace('.', '')
    journal = journal.strip(' ,.')
    return journal

def create_SR_column(scopus_df, author_col, year_col, journal_col):
    """
    Creates a new column 'SR' in the dataframe with format 'First_Author, year, journal'.
    
    Parameters:
    ----------
    df : pandas.DataFrame
        The dataframe to modify.
    author_col : str
        Name of the column containing authors (separated by ';').
    year_col : str
        Name of the column containing publication year.
    journal_col : str
        Name of the column containing journal name.
        
    Returns:
    -------
    pandas.DataFrame
        The dataframe with the new 'SR' column.
    """
    # Crear una función auxiliar que procesará cada fila
    def create_SR_ref(row):
        author = row[author_col]
        year = row[year_col]
        journal = row[journal_col]
        # Extraer el primer autor
        first_author = author.split(';')[0] if isinstance(author, str) and ';' in author else author
        return f"{first_author}, {year}, {journal}"
    
    # Aplicar la función a cada fila del DataFrame
    scopus_df['SR'] = scopus_df.apply(create_SR_ref, axis=1)
    
    return scopus_df

def get_scopus_references(scopus_df):
    """
    Processes the Scopus DataFrame to extract author, titles, years, journals, and DOIs,
    removes dots from journal names, creates SR_ref, and rearranges columns.

    Parameters:
    ----------
    scopus_df : pd.DataFrame
        A DataFrame containing at least the columns 'SR' and 'references'.

    Returns:
    -------
    pd.DataFrame
        A DataFrame with columns 'SR', 'SR_ref', 'title', 'author', 'journal', 'year', 'doi', 'CR_ref'.
    """
    print(scopus_df.columns)
    scopus_df = create_SR_column(scopus_df, author_col='author', year_col='year', journal_col='journal')
    scopus_df = scopus_df[['SR', 'references']].copy()
    extracted_refs = []

    for idx, row in scopus_df.iterrows():
        sr_value = row['SR']
        cr_value = row['references']
        references = re.split(r';\s*', str(cr_value))

        for ref in references:
            original_ref = ref.strip()
            if original_ref:
                # Extract author and remaining text
                author, remaining_text = extract_authors(original_ref)
                if not author:
                    continue  # Skip entries without author

                # Extract DOI
                doi, remaining_text = extract_doi(remaining_text)

                # Clean remaining text
                remaining_text = clean_remaining_text(remaining_text)

                # Extract publication year
                year, text_before_year, text_after_year = extract_year(remaining_text)

                # Extract title and journal
                title, journal = extract_title_and_journal(text_before_year, text_after_year)

                if not title:
                    continue  # Skip entries without title

                extracted_refs.append({
                    'SR': sr_value,
                    'CR_ref': original_ref,
                    'author': author,
                    'title': title,
                    'journal': journal if journal else '-',
                    'year': year,
                    'doi': doi
                })

    references_df = pd.DataFrame(extracted_refs)

    # Remove dots from 'journal' column
    references_df['journal'] = references_df['journal'].str.replace('.', '', regex=False)

    # Create 'SR_ref' column as 'First_Author, year, journal'
    references_df['SR_ref'] = references_df.apply(
        lambda row: f"{row['author'].split(';')[0]}, {row['year']}, {row['journal']}",
        axis=1
    )

    # Rearrange columns to the desired order
    desired_order = ['SR', 'SR_ref', 'title', 'author', 'journal', 'year', 'doi', 'CR_ref']
    references_df = references_df[desired_order]

    return references_df
