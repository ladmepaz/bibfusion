import pandas as pd
import re
import unicodedata
from rapidfuzz import process, fuzz
import os

def scopus_csv_to_df(file_path, scimago, score_cutoff=85):
    """
    Reads a Scopus CSV file and:
      - normalizes column names,
      - upper-cases key text fields,
      - creates 'abbreviated_source_title' by matching with Scimago data,
      - creates an 'SR' key,
      - renames journal/art identifiers,
      - removes dots from journal abbreviations,
      - replaces NaN with empty strings,
      - and finally reorders columns to the given schema.
    """
    try:
        df = pd.read_csv(file_path)

        # --- Normalize column labels ---
        def transform_column_labels(columns):
            out = []
            for col in columns:
                c = col.lower().replace('(', '').replace(')', '')
                c = c.replace(' ', '_')
                c = re.sub(r'\W+', '_', c)
                out.append(c.strip('_'))
            return out

        df.columns = transform_column_labels(df.columns)

        # --- Uppercase key text fields (ASCII) ---
        MAPEO_ESPECIAL = str.maketrans({
            "Ø": "O", "ø": "o",
            "Æ": "AE", "æ": "ae",
            "Å": "A", "å": "a",
            "Ð": "D", "ð": "d",
            "Þ": "Th", "þ": "th",
            "ß": "ss",
        })

        def ascii_upper(val):
            """
            Normalize to ASCII-only upper-case:
            - NFD to break accents
            - drop combining marks
            - manual mapping for characters that do not decompose (e.g., Ø -> O)
            - encode/decode ASCII ignoring leftovers
            """
            if pd.isna(val):
                return ''
            s = str(val)
            norm = unicodedata.normalize('NFD', s)
            no_marks = ''.join(ch for ch in norm if not unicodedata.combining(ch))
            mapped = no_marks.translate(MAPEO_ESPECIAL)
            return mapped.encode('ascii', 'ignore').decode('ascii').upper()

        cols_to_upper = [
            'authors', 'author_full_names', 'title', 'source_title', 'affiliations',
            'authors_with_affiliations', 'abstract', 'author_keywords', 'index_keywords',
            'references', 'editors', 'publisher', 'sponsors', 'conference_name', 
            'conference_date', 'conference_location', 'language_of_original_document', 'abbreviated_source_title', 
            'document_type', 'publication_stage', 'open_access', 'source'
        ]
        cols_to_upper += [c for c in df.columns if c.startswith('funding_text')]

        for c in cols_to_upper:
            if c in df.columns:
                df[c] = df[c].apply(ascii_upper)

        # --- Pre-SR renames ---
        df.rename(columns={
            'authors': 'author',
            'art_no': 'article_number',
            'language_of_original_document': 'language',
            'open_access': 'open_access_indicator',
            'source_title': 'journal'
        }, inplace=True)

        # --- Crear abbreviated_source_title usando Scimago ---
        if 'journal' in df.columns:
            # Me quedo con la primera ocurrencia de cada Title en Scimago
            scimago_clean = scimago.drop_duplicates(subset='Title', keep='first')
            scimago_titles = scimago_clean['Title'].astype(str).tolist()
            scimago_abbrs = scimago_clean.set_index('Title')['journal_abbr']

            abbreviated_list = []
            for journal in df['journal']:
                if pd.isna(journal):
                    abbreviated_list.append('')
                    continue

                match = process.extractOne(
                    journal,
                    scimago_titles,
                    scorer=fuzz.WRatio,
                    score_cutoff=score_cutoff
                )
                if match:
                    matched_title = match[0]
                    abbr_value = scimago_abbrs.get(matched_title, '')
                    # Si aun así abbr_value es una serie (caso raro), tomo el primero
                    if isinstance(abbr_value, pd.Series):
                        abbr_value = abbr_value.iloc[0]
                    # Normalizar: quitar espacios extra y puntos
                    abbr_value = str(abbr_value).strip().replace('.', '')
                    if abbr_value == 'nan':
                        abbr_value = ''
                    abbreviated_list.append(abbr_value)
                else:
                    abbreviated_list.append('')

            df['abbreviated_source_title'] = abbreviated_list
        # --- Final renames for clarity ---
        df.rename(columns={
            'abbreviated_source_title': 'source_title',
            'art__no': 'article_number'  # if double underscore existed
        }, inplace=True)

        # --- Remove dots from source_title ---
        if 'source_title' in df.columns:
            # Primero reemplazamos NaN por '' para que no queden como 'nan'
            df["source_title"] = df["source_title"].replace("nan", "")
            df['source_title'] = df['source_title'].astype(str).str.replace('.', '', regex=False)
        # Rebuild SR using author_full_names (more robust) -> LAST FIRSTINITIAL, YEAR, SOURCE_TITLE
        def build_sr(row):
            auths = str(row.get('author_full_names') or '').split(';')
            first = auths[0].strip() if auths else ''
            # Remove any IDs in parentheses
            import re
            first_clean = re.sub(r'\\(.*?\\)', '', first).strip()
            # try to parse "LAST, FIRST" or "FIRST LAST"
            if ',' in first_clean:
                last, first_names = [x.strip() for x in first_clean.split(',', 1)]
                init = first_names[0].upper() if first_names else ''
                base = f"{last.upper()} {init}" if init else last.upper()
            else:
                parts = [p.strip() for p in first_clean.split() if p.strip()]
                if len(parts) >= 2:
                    last = parts[-1].upper()
                    init = parts[0][0].upper()
                    base = f"{last} {init}"
                elif parts:
                    base = parts[0].upper()
                else:
                    base = ''
            year = str(row.get('year') or '').strip()
            source = str(row.get('source_title') or '').strip().upper()
            sr_parts = [p for p in [base, year, source] if p]
            return ', '.join(sr_parts)

        df['SR'] = df.apply(build_sr, axis=1)
        df['SR'] = (
            df['SR']
            .str.replace(r'^,\s*', '', regex=True)
            .str.replace(r'\s+,', ', ', regex=True)
            .str.replace(r',\s*,', ', ', regex=True)
            .str.strip(' ,')
        )

        # --- Derive 'country' from 'affiliations' (similar a WoS) ---
        def load_country_map():
            country_map = {}
            try:
                repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
                country_csv = os.path.join(repo_root, 'tests', 'files', 'country.csv')
                if os.path.exists(country_csv):
                    cdf = pd.read_csv(country_csv, sep=';')
                    for name in cdf.get('Name', pd.Series(dtype=str)).astype(str):
                        up = ascii_upper(name)
                        if up:
                            country_map[up] = up
            except Exception:
                country_map = {}
            return country_map

        country_map = load_country_map()
        synonyms = {
            'USA': 'UNITED STATES', 'U S A': 'UNITED STATES', 'UNITED STATES OF AMERICA': 'UNITED STATES',
            'U ARAB EMIR': 'UNITED ARAB EMIRATES', 'UNITED ARAB EMIR': 'UNITED ARAB EMIRATES',
            'PEOPLES R CHINA': 'CHINA', 'PEOPLES REPUBLIC OF CHINA': 'CHINA', 'P R CHINA': 'CHINA',
            'ENGLAND': 'UNITED KINGDOM', 'SCOTLAND': 'UNITED KINGDOM', 'WALES': 'UNITED KINGDOM',
            'NORTHERN IRELAND': 'UNITED KINGDOM',
        }

        def extract_countries(aff_str: str) -> str:
            if not isinstance(aff_str, str) or not aff_str.strip():
                return ''
            s = aff_str.strip()
            segs = [p for p in s.split(';') if p.strip()]
            countries = []
            for seg in segs:
                parts = seg.rsplit(',', 1)
                cand = parts[-1] if parts else seg
                cand = ascii_upper(cand).rstrip('.')
                cand = re.sub(r"\s+", " ", cand)
                if re.search(r"\bUSA\b$", cand):
                    norm = 'UNITED STATES'
                else:
                    m = re.search(r"([A-Z][A-Z ]+)$", cand)
                    tail = m.group(1).strip() if m else cand
                    norm = synonyms.get(tail) or country_map.get(tail)
                    if not norm:
                        last_tok = tail.split()[-1]
                        norm = synonyms.get(last_tok) or country_map.get(last_tok)
                    if not norm:
                        norm = tail
                countries.append(norm)
            return '; '.join(countries) if countries else ''

        try:
            if 'affiliations' in df.columns:
                df['country'] = df['affiliations'].apply(extract_countries)
        except Exception:
            df['country'] = ''

        # --- Reorder to exact schema ---
        desired_order = [
            'author',
            'author_full_names',
            'authors_id',
            'title',
            'year',
            'journal',
            'source_title',
            'volume',
            'issue',
            'article_number',
            'page_start',
            'page_end',
            'page_count',
            'cited_by',
            'doi',
            'affiliations',
            'country',
            'authors_with_affiliations',
            'abstract',
            'author_keywords',
            'index_keywords',
            'funding_texts',
            'references',
            'editors',
            'publisher',
            'sponsors',
            'conference_name',
            'conference_date',
            'conference_location',
            'conference_code',
            'issn',
            'isbn',
            'coden',
            'pubmed_id',
            'language',
            'document_type',
            'publication_stage',
            'open_access_indicator',
            'source',
            'eid',
            'molecular_sequence_numbers',
            'link',
            'chemicals_cas',
            'tradenames',
            'manufacturers',
            'SR'
        ]
        existing = [c for c in desired_order if c in df.columns]
        df = df[existing]

        # Drop fully empty rows and mark main articles
        df = df.dropna(how='all')
        df['ismainarticle'] = True

        return df

    except FileNotFoundError:
        print(f"The file '{file_path}' was not found.")
        raise FileNotFoundError
    except pd.errors.EmptyDataError:
        print(f"The file '{file_path}' is empty or malformed.")
        raise ValueError("Empty or malformed CSV file")
    except Exception as e:
        print(f"Error processing '{file_path}': {e}")
        raise
