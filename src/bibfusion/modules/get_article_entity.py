import pandas as pd

def get_article_entity(wos_df_3: pd.DataFrame) -> pd.DataFrame:
    """
    Build the Article dataframe from wos_df_3.
    Deduplicate only rows that have a valid DOI; keep rows without DOI intact.

    Parameters
    ----------
    wos_df_3 : pd.DataFrame
        Input WoS dataframe after merges/enrichment.

    Returns
    -------
    pd.DataFrame
        Article dataframe with selected columns.
    """
    # Deduplicate by DOI only for rows with a valid DOI
    if 'doi' in wos_df_3.columns:
        mask_valid_doi = (
            wos_df_3['doi'].notna()
            & (wos_df_3['doi'].astype(str).str.strip() != '')
            & (wos_df_3['doi'].astype(str).str.strip() != '-')
        )
        with_doi = wos_df_3[mask_valid_doi].copy()
        without_doi = wos_df_3[~mask_valid_doi].copy()
        duplicados = with_doi.duplicated(subset=['doi'], keep='first')
        print(f"{int(duplicados.sum())} duplicate rows found based on 'doi' (only with valid DOI).")
        with_doi = with_doi.drop_duplicates(subset=['doi'], keep='first')
        wos_df_3 = pd.concat([with_doi, without_doi], ignore_index=True)

    columns_to_select = [
        'SR',  # article_id
        'title',
        'author',
        'author_full_names',
        'orcid',
        'abstract',
        'article_number',
        'year',
        'document_type',
        'language',
        'journal',
        'source_title',
        'country',
        'doi',
        'page_start',
        'page_end',
        'early_access_date',
        'author_keywords',
        'index_keywords',
        'web_of_science_categories',
        'subject_category',
        'document_delivery_number',
        'funding_agency',
        'funding_details',
        'ismainarticle',
        'cited_by',
        'open_access_indicator',
        'cited_reference_count',
        'usage_count_last_180_days',
        'usage_count_since_2013',
        'accession_number'
    ]
    existing = [c for c in columns_to_select if c in wos_df_3.columns]
    return wos_df_3[existing]
