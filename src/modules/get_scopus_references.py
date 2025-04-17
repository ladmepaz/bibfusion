import pandas as pd
import re
import urllib.parse

def process_scopus_references(df):
    if 'references' not in df.columns:
        raise ValueError("El DataFrame debe contener la columna 'references'")
    
    scopus_df = df.copy()
    
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
                references_list.append(reference_data)
    
    if not references_list:
        return pd.DataFrame()
    
    references_df = pd.DataFrame(references_list)

    # Mapear 'source_title' desde scopus_df si fuera necesario
    references_df['source_title_mainarticle'] = '-'

    column_order = ['title', 'author', 'source_title', 'source_title_mainarticle', 
                   'year', 'volume', 'pages', 'doi', 'CR_ref']
    
    for col in column_order:
        if col not in references_df.columns:
            references_df[col] = '-'
    
    return references_df, scopus_df


def _parse_reference(reference):
    authors, remaining = _extract_authors(reference)
    if not authors:
        return None
    
    doi, remaining = _extract_doi(remaining)
    year, remaining = _extract_year(remaining)
    title, journal, volume, pages = _extract_title_journal_volume_pages(remaining)
    if not title:
        return None
    
    return {
        'author': authors,
        'title': title,
        'source_title': journal,
        'year': year,
        'volume': volume,
        'pages': pages,
        'doi': doi
    }

def _extract_authors(reference):
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

def _extract_doi(text):
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
    
    doi = urllib.parse.unquote(doi)
    doi = doi.rstrip('.,')
    text = text.strip(' ,.')
    
    return doi, text

def _extract_year(text):
    year_match = re.search(r'\((\d{4})\)', text)
    if year_match:
        year = year_match.group(1)
        text = text.replace(year_match.group(0), '').strip(' ,.')
    else:
        year_match = re.search(r'\b(\d{4})\b', text)
        if year_match:
            year = year_match.group(1)
            if 1800 <= int(year) <= 2030:
                text = text.replace(year, '').strip(' ,.')
            else:
                year = ''
        else:
            year = ''
    
    return year, text

def _extract_title_journal_volume_pages(text):
    text = re.sub(r'\(\d{4}\)', '', text).strip().rstrip(',')

    pages = ""
    match_pages = re.search(r'PP\.\s*([\d\-–]+)', text)
    if match_pages:
        pages = match_pages.group(1).strip()
        text = text.replace(match_pages.group(0), '').strip().rstrip(',')

    components = [comp.strip() for comp in text.split(',') if comp.strip()]
    
    paises_df = pd.read_csv(r"tests\files\country.csv", sep=';', header=None, names=["codigo", "pais"])
    lista_paises = set(paises_df["pais"].str.upper().str.strip())

    volume = ""
    i = len(components) - 1
    while i >= 0:
        comp = components[i]
        if re.fullmatch(r'\d+([\-–]\d+)?', comp):
            if not volume:
                volume = comp
            i -= 1
        elif comp.upper() in lista_paises:
            i -= 1
        else:
            break

    journal = ""
    if i >= 0:
        journal = components[i]
        i -= 1

    title_parts = components[:i+1]
    title_parts = [part for part in title_parts if part.isupper() or ':' in part]
    title = ', '.join(title_parts).strip()

    return title, journal.strip(), volume.strip(), pages.strip()

def _clean_text(text):
    text = re.sub(r'\s+', ' ', text)
    text = text.strip(' ,.:;-')
    return text
