import pandas as pd
import re
import urllib.parse

# A global set that will hold the country names for _extract_title_journal_volume_pages
LISTA_PAISES = set()

def process_scopus_references(df: pd.DataFrame, paises_df: pd.DataFrame) -> pd.DataFrame:
    """
    Toma un DataFrame Scopus con columna 'references' y un DataFrame de países,
    y devuelve un único DataFrame con las referencias individuales parseadas
    (incluyendo SR y CR_ref).

    Parameters:
    -----------
    df : pd.DataFrame
        DataFrame que debe contener al menos las columnas 'references' y 'SR'.
    paises_df : pd.DataFrame
        DataFrame que debe tener una columna 'pais' con los nombres de los países.

    Returns:
    --------
    references_df : pd.DataFrame
        DataFrame con las referencias individuales y sus campos
        (author, title, source_title, year, volume, pages, doi, SR, CR_ref).
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
        refs_text = str(row['references'])
        indiv = [r.strip() for r in re.split(r';\s*', refs_text) if r.strip()]
        for ref_text in indiv:
            data = _parse_reference(ref_text)
            if data:
                data['CR_ref'] = ref_text
                data['SR']     = row.get('SR', '-')
                references_list.append(data)

    # Si no parseó nada, devuelvo un DataFrame vacío
    if not references_list:
        return pd.DataFrame()

    references_df = pd.DataFrame(references_list)
    references_df['source_title_mainarticle'] = '-'

    # Asegurar existencia y orden de columnas
    cols = [
        'title', 'author', 'source_title', 'source_title_mainarticle',
        'year', 'volume', 'pages', 'doi', 'SR', 'CR_ref'
    ]
    for c in cols:
        if c not in references_df.columns:
            references_df[c] = '-'
    references_df = references_df[cols]

    return references_df


def _parse_reference(reference: str):
    authors, rem = _extract_authors(reference)
    if not authors:
        return None

    doi, rem    = _extract_doi(rem)
    year, rem   = _extract_year(rem)
    title, journal, vol, pages = _extract_title_journal_volume_pages(rem)
    if not title:
        return None

    return {
        'author':       authors,
        'title':        title,
        'source_title': journal,
        'year':         year,
        'volume':       vol,
        'pages':        pages,
        'doi':          doi
    }

def _extract_authors(reference: str):
    parts = [p.strip() for p in reference.split(',')]
    if not parts:
        return '', reference

    authors = []
    idx = None
    pattern = r'^[A-Z][A-Za-z\'\-]+\s+(?:[A-Z]\.?)+$'
    for i, part in enumerate(parts):
        if re.match(pattern, part):
            authors.append(part)
        else:
            idx = i
            break

    if not authors and parts:
        authors = [parts[0]]
        idx = 1

    if idx is None:
        idx = len(authors)
        authors = authors[:-1]

    return ';'.join(authors), ', '.join(parts[idx:])

def _extract_doi(text: str):
    m = re.search(r'DOI:\s*(10\.\S+)', text, flags=re.IGNORECASE)
    if m:
        doi = m.group(1); text = text.replace(m.group(0), '')
    else:
        m = re.search(r'(10\.\S+)', text)
        if m:
            doi = m.group(1); text = text.replace(m.group(1), '')
        else:
            doi = ''
    doi = urllib.parse.unquote(doi).rstrip('.,')
    return doi, text.strip(' ,.')

def _extract_year(text: str):
    m = re.search(r'\((\d{4})\)', text)
    if m:
        y = m.group(1); text = text.replace(m.group(0), '')
        return y, text.strip(' ,.')
    m = re.search(r'\b(\d{4})\b', text)
    if m and 1800 <= int(m.group(1)) <= 2030:
        y = m.group(1); text = text.replace(y, '')
        return y, text.strip(' ,.')
    return '', text

def _extract_title_journal_volume_pages(text: str):
    global LISTA_PAISES
    text = re.sub(r'\(\d{4}\)', '', text).strip().rstrip(',')

    # páginas
    pages = ''
    mp = re.search(r'PP\.\s*([\d\-–]+)', text)
    if mp:
        pages = mp.group(1)
        text = text.replace(mp.group(0), '').strip().rstrip(',')

    comps = [c.strip() for c in text.split(',') if c.strip()]
    vol = ''; i = len(comps) - 1
    while i >= 0:
        c = comps[i]
        if re.fullmatch(r'\d+([\-–]\d+)?', c):
            if not vol: vol = c
            i -= 1
        elif c.upper() in LISTA_PAISES:
            i -= 1
        else:
            break

    journal = comps[i] if i >= 0 else ''
    i -= 1
    title_parts = comps[:i+1]
    title_parts = [p for p in title_parts if p.isupper() or ':' in p]
    title = ', '.join(title_parts).strip()
    return title, journal, vol, pages
