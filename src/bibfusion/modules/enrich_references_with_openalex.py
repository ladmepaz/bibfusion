import requests
import pandas as pd
import time
import unicodedata

from requests.exceptions import ConnectionError, Timeout, HTTPError 
def reconstruct_abstract(abstract_inverted_index):
    if not isinstance(abstract_inverted_index, dict):  # Verifica si es un diccionario
        return ""  # Devuelve un string vacío si no es válido
    abstract = {}
    for word, positions in abstract_inverted_index.items():
        for pos in positions:
            abstract[pos] = word
    return " ".join(abstract[i] for i in sorted(abstract))


def _to_upper_ascii(text: str) -> str:
    if not isinstance(text, str):
        return text
    normalized = unicodedata.normalize('NFKD', text)
    stripped = ''.join(ch for ch in normalized if not unicodedata.combining(ch))
    return stripped.upper()

# Enrich references with OpenAlex
def get_paper_info_from_doi(
    doi,
    sr_ref=None,
    cr_ref=None,
    source_title=None,
    year=None,
    authors=None,
):
    """
    Fetch detailed paper information from OpenAlex using a DOI.
    Always returns a dict with a consistent schema.
    """
    api_url = f"https://api.openalex.org/works/doi:{doi}"

    try:
        response = requests.get(api_url, timeout=30)
        response.raise_for_status()
        data = response.json()

        # ------------------------
        # Basic metadata
        # ------------------------
        title = data.get("title", "")
        publication_year = data.get("publication_year", "")
        openalex_work_id = data.get("id", "")

        source = data.get("primary_location", {}).get("source", {})
        journal = source.get("display_name", "")

        bibliographic_info = data.get("biblio", {})
        volume = bibliographic_info.get("volume", "")
        issue = bibliographic_info.get("issue", "")
        first_page = bibliographic_info.get("first_page", "")
        last_page = bibliographic_info.get("last_page", "")
        page = f"{first_page}-{last_page}" if first_page or last_page else ""

        # ------------------------
        # Authors / ORCID / IDs / Affiliations
        # ------------------------
        author_full_names = []
        orcids = []
        author_ids = []
        affiliations = []

        for auth in data.get("authorships", []):
            author_data = auth.get("author", {})

            name = author_data.get("display_name", "")
            orcid = author_data.get("orcid", "")
            author_id = author_data.get("id", "")

            author_full_names.append(name)
            orcids.append(orcid if orcid else "NO ORCID")
            author_ids.append(author_id)

            insts = auth.get("institutions", [])
            inst_names = [i.get("display_name", "") for i in insts if i.get("display_name")]
            affiliations.append("; ".join(inst_names))

        author_full_names_str = _to_upper_ascii("; ".join(author_full_names))
        affiliation_2_str = _to_upper_ascii("; ".join(affiliations))

        # ------------------------
        # Keywords
        # ------------------------
        keywords_list = data.get("keywords", [])
        keywords = [k.get("display_name", "") for k in keywords_list if k.get("display_name")]
        keywords_str = "; ".join(keywords)

        # ------------------------
        # Abstract (inverted index)
        # ------------------------
        abstract = data.get("abstract_inverted_index", "")

        return {
            "doi": doi,
            "SR_ref": sr_ref,
            "CR_ref": cr_ref,
            "authors": authors,
            "author_full_names": author_full_names_str,
            "orcid": "; ".join(orcids),
            "affiliation_2": affiliation_2_str,
            "title": title,
            "source_title": source_title,
            "journal": journal,
            "journal_issue_number": issue,
            "year": year,
            "year_openalex": publication_year,
            "volume": volume,
            "issue": issue,
            "page": page,
            "keywords": keywords_str,
            "abstract": abstract,
            "author_id_openalex": "; ".join(author_ids),
            "openalex_work_id": openalex_work_id,
        }

    except requests.exceptions.RequestException as e:
        print(f"[OpenAlex ERROR] DOI {doi}: {e}")
        return None

def enrich_references_with_openalex(df):
    """
        Enriches a references DataFrame with information from OpenAlex.

        Args:
            df (pd.DataFrame): DataFrame containing DOIs in the 'doi' column and 'SR_ref'

        Returns:
            pd.DataFrame: DataFrame with enriched document information
    """
    results = []
    
    for index, row in df.iterrows():
        # Checks whether it has a DOI
        if 'doi' not in row or pd.isna(row['doi']) or row['doi'] == '' or row['doi'] == '-':
            # print(f"Row {index}: DOI not found or empty, skipping...")
            results.append(row.to_dict())  # Converts the row to a dictionary and adds it
            continue
            
        doi = row['doi']
        sr_ref = row['SR_ref']  # Use the existing SR_ref column
        cr_ref = row['CR_ref']  # Use the existing CR_ref column
        source_title = row['source_title']  # Use the existing source_title column
        year = row['year']  # Use the existing year column
        authors = row['authors']  # Use the existing authors column

        try:
            paper_info = get_paper_info_from_doi(doi, sr_ref, cr_ref, source_title, year, authors)
            
            if paper_info:
                results.append(paper_info)
            
            # To avoid overloading the API
            time.sleep(0.5)
        
        except Exception as e:
            print(f"Error processing DOI {doi}: {e}")
    
    enrich_references = pd.DataFrame(results)
    enrich_references['abstract'] = enrich_references['abstract'].apply(reconstruct_abstract)
    
    # Reorder columns (including keywords)
    column_order = [
        'doi', 'SR_ref', 'CR_ref',
        'authors', 'author_full_names', 'orcid', 'affiliation_2',
        'title', 'source_title', 'journal', 'journal_issue_number',
        'year', 'year_openalex', 'volume', 'issue', 'page', 'keywords',
        'author_id_openalex', 'openalex_work_id'
    ]

    enrich_references = enrich_references[column_order]
    return enrich_references

# Example usage:
# import pandas as pd
# df = pd.read_csv('references.csv')
# result = enrich_references_with_openalex(df)
# result.to_csv('enriched_references.csv', index=False)