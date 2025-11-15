import requests
import pandas as pd
import time
from typing import List, Optional, Dict, Any

# Utilidad para reconstruir resumen a partir de abstract_inverted_index

def reconstruct_abstract(abstract_inverted_index: Dict[str, List[int]]) -> str:
    if not isinstance(abstract_inverted_index, dict):
        return ''
    abstract = {}
    for word, positions in abstract_inverted_index.items():
        for pos in positions:
            abstract[pos] = word
    return ' '.join(abstract[i] for i in sorted(abstract))

# --- 1) Generar la columna 'referencias_citadas' ---

def get_openalex_references(doi: str) -> List[str]:
    clean = doi.replace('https://doi.org/', '').strip() or None
    if not clean:
        return []
    url = f"https://api.openalex.org/works/doi:{clean}"
    try:
        r = requests.get(url); r.raise_for_status()
        data = r.json()
        refs = data.get("referenced_works", [])
        return [f"https://openalex.org/{r.split('/')[-1]}" for r in refs if r]
    except requests.RequestException as e:
        #print(f"[get_refs] Error {clean}: {e}")
        return []


def generate_references_column(df: pd.DataFrame, doi_col: str = 'doi') -> pd.DataFrame:
    """
    Genera la columna 'referencias_citadas' manteniendo solo 'SR' y las referencias
    
    Args:
        df: DataFrame que debe contener 'doi' y 'SR'
        doi_col: Nombre de la columna con los DOIs
        
    Returns:
        DataFrame con solo las columnas 'SR' y 'referencias_citadas'
    """
    # Verificar columnas requeridas
    required_cols = {'SR', doi_col}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Faltan columnas requeridas: {missing}")
    
    # Generar las referencias
    refs_list = []
    for i, doi in enumerate(df[doi_col].fillna('')):
        if not doi.strip():
            refs_list.append([])
        else:
            refs_list.append(get_openalex_references(doi))
        time.sleep(0.5)  # Respeta el rate limit de la API
    
    # Crear nuevo DataFrame con solo las columnas necesarias
    result_df = pd.DataFrame({
        'SR': df['SR'],
        doi_col: df[doi_col],
        'referencias_citadas': refs_list
    })
    
    return result_df

# --- 2) Fetch y extracción de metadata de cada referencia ---

def fetch_openalex_work(work_id: str) -> Optional[Dict[str, Any]]:
    try:
        r = requests.get(f"https://api.openalex.org/works/{work_id}")
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        #print(f"[fetch_work] Error {work_id}: {e}")
        return None


def extract_work_info(w: Dict[str, Any]) -> Dict[str, Any]:
    if not w:
        return {
            'title': None,
            'doi': None,
            'journal': None,
            'source_title': None,
            'volume': None,
            'issue': None,
            'page': None,
            'journal_issue_number': None,
            'abstract': None,
            'year': None,
            'authors': None,
            'authors_with_affiliations': None,
            'orcids': None,
            'cited_by': None,
            'keywords': None  # 🔹 Añadido
        }

    doi_raw = w.get('doi')
    bib = w.get('biblio') or {}
    primary_location = w.get('primary_location') or {}
    source = primary_location.get('source') or {}
    
    # Reconstrucción de abstract
    inverted = w.get('abstract_inverted_index')
    abstract_text = reconstruct_abstract(inverted) if inverted else None

    # --- Autores ---
    auths, orcids, authors_with_affiliations = [], [], []
    for a in w.get('authorships', []) or []:
        author = a.get('author') or {}
        name = author.get('display_name')
        orcid = author.get('orcid')
        
        if name:
            name_parts = name.split()
            if len(name_parts) >= 2:
                short_name = f"{name_parts[-1].upper()} {name_parts[0][0].upper()}."
            else:
                short_name = name.upper()
            
            auths.append(name)
            orcids.append(orcid if orcid else '')
            
            institutions_info = []
            for inst in (a.get('institutions') or []):
                inst_name = inst.get('display_name')
                country = inst.get('country_code')
                if inst_name:
                    inst_info = f"{inst_name}, {country.upper()}" if country else inst_name
                    institutions_info.append(inst_info)
            affiliation = '; '.join(institutions_info) if institutions_info else 'NO AFFILIATION'
            authors_with_affiliations.append(f"{short_name}, {affiliation}")

    # --- Keywords ---
    keywords = []
    for kw in w.get('keywords', []) or []:
        kw_name = kw.get('display_name')
        if kw_name:
            keywords.append(kw_name)
    keywords_str = '; '.join(keywords) if keywords else None

    return {
        'title': w.get('title'),
        'doi': doi_raw.replace('https://doi.org/', '') if doi_raw else None,
        'journal': source.get('display_name'),
        'source_title': None,
        'volume': bib.get('volume'),
        'issue': bib.get('issue'),
        'page': bib.get('first_page'),
        'journal_issue_number': bib.get('issue'),
        'abstract': abstract_text,
        'year': w.get('publication_year'),
        'authors': '; '.join(auths) if auths else None,
        'authors_with_affiliations': '; '.join(authors_with_affiliations) if authors_with_affiliations else None,
        'orcids': '; '.join(orcids) if orcids else None,
        'cited_by': w.get('cited_by_count'),
        'author_keywords': keywords_str
    }



def openalex_enrich_ref(df: pd.DataFrame, exclude_books: bool = True, audit_dropped_path: Optional[str] = None) -> pd.DataFrame:
    if 'referencias_citadas' not in df.columns:
        raise ValueError("Falta la columna 'referencias_citadas'. Ejecuta generate_references_column primero.")
    
    rows = []
    dropped = []
    for _, row in df.iterrows():
        sr = row.get('SR')
        doi0 = row.get('doi')
        refs = row['referencias_citadas']
        
        if isinstance(refs, str):
            try:
                refs = eval(refs)
            except:
                refs = []

        if not isinstance(refs, list):
            refs = []

        for url in refs:
            wid = url.rstrip('/').split('/')[-1]
            wd = fetch_openalex_work(wid)
            
            if not wd: 
                rows.append({
                    'SR_original': sr,
                    'doi_original': doi0,
                    'openalex_id': wid,
                    'openalex_url': url,
                    'title': None,
                    'doi': None,
                    'journal': None,
                    'source_title': None,
                    'volume': None,
                    'issue': None,
                    'page': None,
                    'journal_issue_number': None,
                    'abstract': None,
                    'year': None,
                    'authors': None,
                    'authors_with_affiliations': None,
                    'orcids': None,
                    'cited_by': None,
                    'author_keywords': None
                })
                continue

            # Optionally exclude book-like types to avoid adding compiled volumes
            if exclude_books:
                wtype = (wd.get('type') or '').lower()
                # Known book-ish types in OpenAlex
                bookish = {
                    'book', 'edited-book', 'monograph', 'reference-book', 'book-section', 'reference-entry'
                }
                if wtype in bookish:
                    dropped.append({
                        'SR_original': sr,
                        'doi_original': doi0,
                        'openalex_id': wid,
                        'openalex_url': url,
                        'type': wtype,
                        'title': wd.get('title')
                    })
                    continue
                
            info = extract_work_info(wd)
            info.update({
                'SR_original': sr,
                'doi_original': doi0,
                'openalex_id': wid,
                'openalex_url': url
            })
            rows.append(info)
            time.sleep(0.3)
    
    enriched = pd.DataFrame(rows)
    # Optionally write audit of dropped items
    if audit_dropped_path and dropped:
        try:
            pd.DataFrame(dropped).to_csv(audit_dropped_path, index=False)
        except Exception:
            pass
    cols = [
        'SR_original', 'doi_original', 'openalex_id', 'openalex_url', 
        'authors', 'authors_with_affiliations', 'orcids',
        'title', 'doi', 'journal', 'source_title', 'volume', 'issue', 'page',
        'journal_issue_number', 'abstract', 'year', 'cited_by', 'author_keywords'
    ]
    return enriched[[c for c in cols if c in enriched.columns]]



# Ejemplo de uso con datos que incluyen SR
data = {
    'doi': [
        '10.1108/QMR-04-2015-0029',
        '10.1080/0965254X.2015.1035036',
        '10.1108/14626001011088705'
    ],
    'SR': [
        'Author1 (2015) QMR Article',
        'Author2 (2015) Marketing Paper',
        'Author3 (2010) Business Research'
    ]
}



# ##################################
# Función para llenar 'source_title' desde scimago
# ##################################


from rapidfuzz import process, fuzz

def fill_source_title_from_scimago(df: pd.DataFrame, scimago: pd.DataFrame, cutoff=95) -> pd.DataFrame:
    """
    Rellena 'source_title' en df usando coincidencia aproximada con scimago, usando rapidfuzz.
    """
    # Preprocesar
    scimago['Title_clean'] = scimago['Title'].str.upper().str.strip()
    title_to_abbr = dict(zip(scimago['Title_clean'], scimago['journal_abbr']))

    scimago_titles = scimago['Title_clean'].tolist()

    df = df.copy()
    df['source_title'] = ''

    for i in range(len(df)):
        journal = str(df.at[i, 'journal']).upper().strip()
        match = process.extractOne(journal, scimago_titles, scorer=fuzz.ratio)
        if match and match[1] >= cutoff:
            df.at[i, 'source_title'] = title_to_abbr[match[0]]
    
    return df





# ##################################
# Función para generar SR
# ##################################

import pandas as pd
import re

def generate_SR_ref(df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate a column 'SR_ref' in format: FIRSTAUTHOR, YEAR, SOURCE_TITLE
    
    Assumes 'authors' column contains "Name Surname; Name Surname" (semicolon-separated).
    Returns a copy of the DataFrame with the new column 'SR_ref'.
    """
    df = df.copy()

    # 1. Process first author
    def process_author(author_str):
        if pd.isna(author_str) or not str(author_str).strip():
            return ''
        first_author = str(author_str).split(';')[0].strip()
        # Eliminar contenido entre paréntesis (como ORCIDs)
        first_author = re.sub(r'\(.*?\)', '', first_author).strip()
        # Normalizar espacios
        first_author = re.sub(r'[,\s]+', ' ', first_author).strip()
        # Dividir en partes
        parts = first_author.split()

        # Eliminar guiones aislados que queden como parte del nombre
        parts = [p for p in parts if p != '-']

        if len(parts) >= 2:
            last_name = parts[-1].upper()
            first_initial = parts[0][0].upper() if parts[0] else ''
            return f"{last_name} {first_initial}" if first_initial else last_name
        elif parts:
            return parts[0].upper()
        else:
            return ''


    # handle if column name is different (optional)
    if 'authors' not in df.columns and 'author' in df.columns:
        df['authors'] = df['author']

    df['first_author'] = df['authors'].apply(process_author)

    # 2. Process year robustly
    # convert to numeric, coerce invalids to NaN
    df['year'] = pd.to_numeric(df['year'], errors='coerce')
    # use nullable integer dtype (keeps NA)
    df['year'] = df['year'].astype('Int64')
    # create clean year string: '2001' or '' if NA
    df['year_clean'] = df['year'].astype('Int64').astype(str).replace('<NA>', '')

    # 3. Process source_title
    df['source_title_clean'] = (
        df.get('source_title', pd.Series([''] * len(df)))  # safe if no column
          .fillna('')
          .astype(str)
          .str.replace('.', '', regex=False)
          .str.strip()
          .str.upper()
    )

    # 4. Combine parts only when present to avoid ", ,"
    def make_sr_ref(row):
        parts = []
        if row.get('first_author'):
            parts.append(row['first_author'])
        if row.get('year_clean'):
            parts.append(row['year_clean'])
        if row.get('source_title_clean'):
            parts.append(row['source_title_clean'])
        return ', '.join(parts)

    df['SR_ref'] = df.apply(make_sr_ref, axis=1)

    # final cleanup: remove extra spaces, trailing commas (shouldn't be needed but safe)
    df['SR_ref'] = (
        df['SR_ref']
        .str.replace(r'\s{2,}', ' ', regex=True)
        .str.strip()
        .str.rstrip(',')
    )

    # drop temporary cols
    return df.drop(columns=['first_author', 'year_clean', 'source_title_clean'], errors='ignore')
