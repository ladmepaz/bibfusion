import pandas as pd
import re

def scopus_csv_to_df(file_path):
    """
    Reads a Scopus CSV file and:
      - normalizes column names,
      - upper‑cases key text fields,
      - creates an 'SR' key,
      - renames journal/art identifiers,
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
            'funding_details', 'references', 'correspondence_address', 'editors',
            'publisher', 'sponsors', 'conference_name', 'conference_date', 'conference_location',
            'language_of_original_document', 'abbreviated_source_title', 'document_type',
            'publication_stage', 'open_access', 'source'
        ]
        cols_to_upper += [c for c in df.columns if c.startswith('funding_text')]
        typo_fix = {'correspondece_address': 'correspondence_address'}
        cols_to_upper = [typo_fix.get(c, c) for c in cols_to_upper]

        for c in cols_to_upper:
            if c in df.columns:
                df[c] = df[c].astype(str).str.upper()

        # --- Pre‑SR renames ---
        df.rename(columns={
            'authors': 'author',
            'art_no': 'article_number',
            'language_of_original_document': 'language',
            'open_access': 'open_access_indicator',
            'source_title': 'journal'
        }, inplace=True)

        # --- Build SR ---
        if all(col in df.columns for col in ('author', 'year', 'abbreviated_source_title')):
            df['SR'] = df.apply(
                lambda r: f"{r['author'].split(';')[0].replace('.', '').strip()}, "
                          f"{r['year']}, "
                          f"{r['abbreviated_source_title'].replace('.', '')}",
                axis=1
            )

        # --- Final renames for clarity ---
        df.rename(columns={
            'journal': 'journal_title',
            'abbreviated_source_title': 'journal_abbreviation',
            'art__no': 'article_number'  # if double underscore existed
        }, inplace=True)

        # --- Reorder to exact schema ---
        desired_order = [
            'author',
            'author_full_names',
            'authors_id',
            'title',
            'year',
            'journal_title',
            'journal_abbreviation',
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
            'funding_details',
            'funding_texts',
            'references',
            'correspondence_address',
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
        # Keep only those that actually exist in df
        existing = [c for c in desired_order if c in df.columns]
        df = df[existing]

        return df

    except FileNotFoundError:
        print(f"The file '{file_path}' was not found.")
        return None
    except pd.errors.EmptyDataError:
        print(f"The file '{file_path}' is empty or malformed.")
        return None
    except Exception as e:
        print(f"Error processing '{file_path}': {e}")
        return None
