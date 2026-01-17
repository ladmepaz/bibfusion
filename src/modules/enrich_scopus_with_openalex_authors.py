import time
import unicodedata
from typing import Optional, List

import pandas as pd
import requests


def _ascii_upper(text: str) -> str:
    if not isinstance(text, str):
        return text
    norm = unicodedata.normalize('NFKD', text)
    stripped = ''.join(ch for ch in norm if not unicodedata.combining(ch))
    return stripped.upper()


def _clean_doi(doi: str) -> Optional[str]:
    if not isinstance(doi, str):
        return None
    doi = doi.strip()
    if not doi or doi == '-':
        return None
    return doi.replace('https://doi.org/', '').strip()


def _join_semicolon(values: List[str]) -> str:
    return '; '.join([v for v in values if v is not None and str(v).strip() != ''])


def enrich_scopus_with_openalex_authors(
    scopus_df: pd.DataFrame,
    replace: bool = True,
    mailto: Optional[str] = None,
    sleep: float = 0.5,
    timeout: float = 15.0,
    uppercase_ascii: bool = True,
    keep_raw: bool = True,
    only_ids: bool = False,
) -> pd.DataFrame:
    """
    Enrich Scopus rows using OpenAlex by DOI to fetch authorship metadata.

    Adds columns:
      - 'author_id_openalex' (semicolon-separated OpenAlex author IDs)
      - 'openalex_work_id' (OpenAlex work ID)
    Optionally (replace=True and not only_ids):
      - replaces 'author_full_names' with OpenAlex display_name (uppercase ASCII if requested)
      - replaces 'orcid' with ORCIDs aligned to authorship order

    Set only_ids=True to only add author_id_openalex/openalex_work_id without touching names/ORCID.
    """

    if 'doi' not in scopus_df.columns:
        raise ValueError("Input DataFrame must contain a 'doi' column")

    df = scopus_df.copy()

    # Ensure output columns exist
    for col in ['author_id_openalex', 'openalex_work_id']:
        if col not in df.columns:
            df[col] = pd.NA

    session = requests.Session()
    headers = {
        'User-Agent': 'preprocessing-pipeline/1.0 (+OpenAlex enrichment)'
    }

    for idx, doi_raw in df['doi'].items():
        doi = _clean_doi(doi_raw)
        if not doi:
            continue

        url = f"https://api.openalex.org/works/doi:{doi}"
        params = {}
        if mailto:
            params['mailto'] = mailto

        try:
            resp = session.get(url, params=params, headers=headers, timeout=timeout)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException:
            continue

        work_id = data.get('id')

        author_ids: List[str] = []
        names: List[str] = []
        orcids: List[str] = []

        for a in data.get('authorships', []) or []:
            author = a.get('author') or {}
            author_ids.append(author.get('id') or '')
            if not only_ids:
                display_name = author.get('display_name')
                if display_name:
                    names.append(display_name)
                orcid = author.get('orcid') or ''
                orcids.append(orcid if orcid else 'NO ORCID')

        df.at[idx, 'author_id_openalex'] = _join_semicolon(author_ids) if author_ids else pd.NA
        df.at[idx, 'openalex_work_id'] = work_id if work_id else pd.NA

        if not only_ids and replace and names:
            # Replace author_full_names
            if 'author_full_names' in df.columns:
                if keep_raw and 'author_full_names_raw' not in df.columns:
                    df['author_full_names_raw'] = df['author_full_names']
                names_str = _join_semicolon(names)
                df.at[idx, 'author_full_names'] = _ascii_upper(names_str) if uppercase_ascii else names_str
            # Replace orcid
            if 'orcid' in df.columns and orcids:
                if keep_raw and 'orcid_raw' not in df.columns:
                    df['orcid_raw'] = df['orcid']
                df.at[idx, 'orcid'] = _join_semicolon(orcids)

        if sleep and sleep > 0:
            time.sleep(sleep)

    return df

