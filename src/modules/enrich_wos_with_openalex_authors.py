import time
from typing import Optional, List
import unicodedata

import pandas as pd
import requests


def _clean_doi(doi: str) -> Optional[str]:
    if not isinstance(doi, str):
        return None
    doi = doi.strip()
    if not doi or doi == '-':
        return None
    return doi.replace('https://doi.org/', '').strip()


def _join_semicolon(values: List[str]) -> str:
    return '; '.join([v for v in values if v is not None and str(v).strip() != ''])


def _to_upper_ascii(text: str) -> str:
    if not isinstance(text, str):
        return text
    normalized = unicodedata.normalize('NFKD', text)
    stripped = ''.join(ch for ch in normalized if not unicodedata.combining(ch))
    return stripped.upper()


def enrich_wos_with_openalex_authors(
    wos_df: pd.DataFrame,
    replace: bool = True,
    mailto: Optional[str] = None,
    sleep: float = 0.5,
    timeout: float = 15.0,
    only_ids: bool = False,
    keep_raw: bool = True,
    uppercase_ascii: bool = True,
) -> pd.DataFrame:
    """
    Given a WoS dataframe that contains a 'doi' column, query OpenAlex for each work
    and extract authors' display names, ORCIDs, and OpenAlex author IDs.

    Default behavior adds these columns:
      - 'author_id_openalex' ("https://openalex.org/A...; ...")
      - 'openalex_work_id' ("https://openalex.org/W...")

    If replace=True, preserves original WoS columns into '*_raw' and replaces
    'author_full_names' and 'orcid' with the OpenAlex-derived values when available.

    If only_ids=True, the function only adds 'author_id_openalex' and does not
    create/modify any other author-related columns regardless of 'replace'.
    """

    if 'doi' not in wos_df.columns:
        raise ValueError("Input DataFrame must contain a 'doi' column")

    df = wos_df.copy()

    # Prepare output columns
    if only_ids:
        if 'author_id_openalex' not in df.columns:
            df['author_id_openalex'] = pd.NA
    else:
        for col in [
            'author_id_openalex',
            'openalex_work_id',
        ]:
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
            # Keep row as-is on failure
            continue

        # Extract work id and authorship fields
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

        author_ids_str = _join_semicolon(author_ids) if author_ids else pd.NA
        df.at[idx, 'author_id_openalex'] = author_ids_str

        if not only_ids:
            names_str = _join_semicolon(names) if names else pd.NA
            orcids_str = _join_semicolon(orcids) if orcids else pd.NA
            df.at[idx, 'openalex_work_id'] = work_id if work_id else pd.NA

            # Optionally replace display columns with OpenAlex values
            if replace and names:
                # Author full names
                if 'author_full_names' in df.columns:
                    if 'author_full_names_raw' not in df.columns:
                        df['author_full_names_raw'] = df['author_full_names']
                    final_names = _to_upper_ascii(names_str) if uppercase_ascii and pd.notna(names_str) else names_str
                    df.at[idx, 'author_full_names'] = final_names
                else:
                    final_names = _to_upper_ascii(names_str) if uppercase_ascii and pd.notna(names_str) else names_str
                    df.at[idx, 'author_full_names'] = final_names

                # ORCIDs aligned to authorship order
                if orcids:
                    if 'orcid' in df.columns:
                        if 'orcid_raw' not in df.columns:
                            df['orcid_raw'] = df['orcid']
                        df.at[idx, 'orcid'] = orcids_str
                    else:
                        df.at[idx, 'orcid'] = orcids_str

        if sleep and sleep > 0:
            time.sleep(sleep)

    # Optionally drop raw backup columns
    if not keep_raw:
        to_drop = [c for c in ['author_full_names_raw', 'orcid_raw'] if c in df.columns]
        if to_drop:
            df = df.drop(columns=to_drop)

    return df
