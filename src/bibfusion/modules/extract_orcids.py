import requests
import pandas as pd
import time
from typing import Optional, List

def extract_orcids(scopus_df_3: pd.DataFrame, doi_col: str = 'doi', is_main_article_col: str = 'ismainarticle', orcid_col: str = 'orcid') -> pd.DataFrame:
    """
    For rows in `scopus_df_3` where `is_main_article_col` == True, fetches author ORCIDs
    via the OpenAlex API using the DOI. Then writes back to the `orcid_col`, such that
    the ORCIDs for all authors in the row are concatenated with ';', and authors without
    ORCID get the placeholder 'NO ORCID'. Returns the modified DataFrame.
    
    Parameters
    ----------
    scopus_df_3 : pd.DataFrame
        Input DataFrame containing at least the columns `doi_col`, `is_main_article_col`, and the authors list.
    doi_col : str, default 'doi'
        Column name that contains the DOI for the work.
    is_main_article_col : str, default 'ismainarticle'
        Column name (boolean) indicating which rows to process.
    orcid_col : str, default 'orcid'
        Column name in which to store the result ORCID string.
    
    Returns
    -------
    pd.DataFrame
        The same DataFrame with updated `orcid_col` for the processed rows.
    """

    base_url = "https://api.openalex.org/works/doi:"
    
    # Ensure the ORCID column exists
    if orcid_col not in scopus_df_3.columns:
        scopus_df_3[orcid_col] = ""
    
    for idx, row in scopus_df_3.iterrows():
        if row.get(is_main_article_col) == "TRUE":
            doi = row.get(doi_col)
            if not doi or pd.isna(doi):
                scopus_df_3.at[idx, orcid_col] = ""
                continue
            
            # Clean up DOI (remove url prefix if present)
            doi_clean = doi.replace("https://doi.org/", "").strip()
            url = f"{base_url}{doi_clean}"
            
            try:
                r = requests.get(url)
                r.raise_for_status()
                data = r.json()
                print(f"Fetched ORCID data for DOI {doi_clean} (row {idx})")
                print(data)
                # Fetch the authorship list from the work
                authorships = data.get('authorships', [])
                
                orcid_list: List[str] = []
                for auth in authorships:
                    author_info = auth.get('author', {})
                    orcid_full = author_info.get('orcid')
                    if orcid_full:
                        # Normalize to short ORCID (strip prefix if exists)
                        orcid_short = orcid_full.replace("https://orcid.org/", "").strip()
                        orcid_list.append(orcid_short)
                    else:
                        orcid_list.append("NO ORCID")
                
                # Join into single string with semicolon
                orcid_str = "; ".join(orcid_list)
                scopus_df_3.at[idx, orcid_col] = orcid_str
            
            except requests.RequestException as e:
                # In case of error, fill with placeholder or leave blank
                scopus_df_3.at[idx, orcid_col] = "; ".join(["NO ORCID"] * len(row.get('author', "").split('; ')))
                print(f"Warning: could not fetch ORCID for DOI {doi_clean} (row {idx}) – {e}")
            
            # Respect API rate limits
            time.sleep(0.5)
    
    return scopus_df_3
