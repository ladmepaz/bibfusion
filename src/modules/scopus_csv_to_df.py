import pandas as pd
import re
from rapidfuzz import process, fuzz

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

        # --- Uppercase key text fields ---
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
                df[c] = df[c].astype(str).str.upper()

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
        # --- Build SR ---
        if all(col in df.columns for col in ('author', 'year', 'abbreviated_source_title')):
            df['SR'] = df.apply(
                lambda r: (
                    ", ".join(r['author'].split(';')[0].split(',')[:2]).replace('.', '').strip() +
                    f", {r['year']}, {r['abbreviated_source_title'].replace('.', '')}"
                ),
                axis=1
            )


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
        df['SR'] = (
            df['SR']
            .str.replace(r'^,\s*', '', regex=True)                # elimina la coma inicial y espacios
            .str.replace(r'(?<=,)\s{2,}', ' ', regex=True)        # reemplaza dobles espacios solo después de una coma
        )

        def transform_sr(valor):
            if pd.isna(valor):
                return valor
            partes = [p.strip() for p in valor.split(",")]
            if len(partes) < 4:
                return valor  # por si la cadena no tiene el formato esperado
            inicial = partes[0]
            apellido = partes[1]
            resto = ", ".join(partes[2:])
            return f"{apellido} {inicial}, {resto}"

        df["SR"] = df["SR"].apply(transform_sr)

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
