import pandas as pd
import re
import urllib.parse

# A global set that will hold the country names for _extract_title_journal_volume_pages
LISTA_PAISES = set()

# paises_df = pd.read_csv(
# 	r'C:\Users\User\OneDrive\Documentos\Preprocessing\preprocessing_3\preprocessing\tests\files\country.csv',
#     sep=';',
#     header=None,
#     names=['codigo','pais']
# )

def process_scopus_references(df: pd.DataFrame, paises_df: pd.DataFrame):
    """
    Toma un DataFrame Scopus con columna 'references' y un DataFrame de países,
    devuelve un DataFrame con las referencias parseadas (incluyendo SR) y el DataFrame original con 'source_title'.
    
    Parameters:
    -----------
    df : pd.DataFrame
        DataFrame que debe contener al menos las columnas 'references' y 'SR'.
    paises_df : pd.DataFrame
        DataFrame que debe tener una columna 'pais' con los nombres de los países.
    
    Returns:
    --------
    references_df : pd.DataFrame
        DataFrame con las referencias individuales y sus campos (author, title, etc.) + SR + CR_ref.
    scopus_df : pd.DataFrame
        Copia del df original, con la columna adicional 'source_title'.
    """
    global LISTA_PAISES
    
    if 'references' not in df.columns:
        raise ValueError("El DataFrame debe contener la columna 'references'")
    
    # Construir el conjunto de países
    LISTA_PAISES = set(paises_df['pais'].str.upper().str.strip())
    
    scopus_df = df.copy()
    
    # Generar source_title a partir de abbreviated_source_title
    if 'abbreviated_source_title' in scopus_df.columns:
        scopus_df['source_title'] = scopus_df['abbreviated_source_title'].str.replace('.', '', regex=False)
    else:
        scopus_df['source_title'] = '-'
    
    references_list = []
    
    for _, row in scopus_df.iterrows():
        references_text = str(row['references'])
        individual_refs = [ref.strip() for ref in re.split(r';\s*', references_text) if ref.strip()]
        
        for ref_text in individual_refs:
            reference_data = _parse_reference(ref_text)
            if reference_data:
                reference_data['CR_ref'] = ref_text
                reference_data['SR']     = row.get('SR', '-')
                references_list.append(reference_data)
    
    # Si no se parseó ninguna referencia, devolvemos un DataFrame vacío
    if not references_list:
        return pd.DataFrame(), scopus_df
    
    references_df = pd.DataFrame(references_list)
    references_df['source_title_mainarticle'] = '-'
    
    # Asegurar existencia y orden de columnas
    column_order = [
        'title', 'author', 'source_title', 'source_title_mainarticle',
        'year', 'volume', 'pages', 'doi', 'SR', 'CR_ref'
    ]
    for col in column_order:
        if col not in references_df.columns:
            references_df[col] = '-'
    references_df = references_df[column_order]
    
    return references_df, scopus_df


def _parse_reference(reference: str):
    authors, remaining = _extract_authors(reference)
    if not authors:
        return None
    
    doi, remaining   = _extract_doi(remaining)
    year, remaining  = _extract_year(remaining)
    title, journal, volume, pages = _extract_title_journal_volume_pages(remaining)
    if not title:
        return None
    
    return {
        'author':       authors,
        'title':        title,
        'source_title': journal,
        'year':         year,
        'volume':       volume,
        'pages':        pages,
        'doi':          doi
    }


def _extract_authors(reference: str):
    parts = [p.strip() for p in reference.split(',')]
    if not parts:
        return '', reference
    
    authors = []
    title_index = None
    author_pattern = r'^[A-Z][A-Za-z\'\-]+\s+(?:[A-Z]\.?)+$'
    
    for i, part in enumerate(parts):
        if re.match(author_pattern, part):
            authors.append(part)
        else:
            title_index = i
            break
    
    if not authors and parts:
        authors = [parts[0]]
        title_index = 1
    
    if title_index is None:
        title_index = len(authors)
        authors = authors[:-1]
    
    authors_str = ';'.join(authors)
    remaining = ', '.join(parts[title_index:])
    return authors_str, remaining


def _extract_doi(text: str):
    doi_match = re.search(r'DOI:\s*(10\.\S+)', text, flags=re.IGNORECASE)
    if doi_match:
        doi = doi_match.group(1)
        text = text.replace(doi_match.group(0), '')
    else:
        doi_match = re.search(r'(10\.\S+)', text)
        if doi_match:
            doi = doi_match.group(1)
            text = text.replace(doi, '')
        else:
            doi = ''
    doi = urllib.parse.unquote(doi).rstrip('.,')
    return doi, text.strip(' ,.')


def _extract_year(text: str):
    year_match = re.search(r'\((\d{4})\)', text)
    if year_match:
        year = year_match.group(1)
        text = text.replace(year_match.group(0), '').strip(' ,.')
    else:
        year_match = re.search(r'\b(\d{4})\b', text)
        if year_match and 1800 <= int(year_match.group(1)) <= 2030:
            year = year_match.group(1)
            text = text.replace(year, '').strip(' ,.')
        else:
            year = ''
    return year, text


def _extract_title_journal_volume_pages(text: str):
    global LISTA_PAISES

    text = re.sub(r'\(\d{4}\)', '', text).strip().rstrip(',')

    pages = ""
    match_pages = re.search(r'PP\.\s*([\d\-–]+)', text)
    if match_pages:
        pages = match_pages.group(1).strip()
        text = text.replace(match_pages.group(0), '').strip().rstrip(',')

    components = [comp.strip() for comp in text.split(',') if comp.strip()]

    volume = ""
    i = len(components) - 1
    while i >= 0:
        comp = components[i]
        if re.fullmatch(r'\d+([\-–]\d+)?', comp):
            if not volume:
                volume = comp
            i -= 1
        elif comp.upper() in LISTA_PAISES:
            i -= 1
        else:
            break

    journal = components[i] if i >= 0 else ""
    i -= 1

    title_parts = components[:i+1]
    title_parts = [part for part in title_parts if part.isupper() or ':' in part]
    title = ', '.join(title_parts).strip()

    return title, journal.strip(), volume.strip(), pages.strip()


def _clean_text(text: str):
    text = re.sub(r'\s+', ' ', text)
    return text.strip(' ,.:;-')
