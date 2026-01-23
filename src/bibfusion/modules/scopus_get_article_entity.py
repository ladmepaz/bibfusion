import pandas as pd

def scopus_get_article_entity(scopus_df_3):
    """
    Delete duplicate rows based on 'doi'
    Selects specific columns from the wos_df_3 DataFrame.

    Parameters:
        scopus_df_3 (pd.DataFrame): The input DataFrame containing WoS data.

    Returns:
        pd.DataFrame: A DataFrame with the selected columns.
    """
    
    # --- Remove duplicates by DOI without deleting rows with empty DOI ---
    scopus_df_3['doi'] = scopus_df_3['doi'].astype(str).str.strip()

    mask_doi_empty = scopus_df_3['doi'].isin(["", "nan", "None", None])
    df_with_doi = scopus_df_3[~mask_doi_empty]

    duplicate = df_with_doi.duplicated(subset=['doi'], keep='first')
    print(f"Found {duplicate.sum()} duplicate rows based on 'doi' (excluding empty DOIs).")

    df_with_doi = df_with_doi.drop_duplicates(subset=['doi'], keep='first')

    # Rejoin rows with and without DOI
    scopus_df_3 = pd.concat([df_with_doi, scopus_df_3[mask_doi_empty]], ignore_index=True)

    columns_to_select = [
    'SR',
    'title',
    'abstract',
    'year',
    'author',
    'author_full_names',
    'orcid',
    'authors_id',
    'authors_with_affiliations',
    'affiliations',
    'country',
    'document_type',
    'publication_stage',
    'journal',
    'source_title',
    # 'volume',
    # 'issue',
    'page_start',
    'page_end',
    'page_count',
    'article_number',
    'doi',
    'issn',
    'isbn',
    'coden',
    'source',
    'publisher',
    'language',
    'open_access_indicator',
    'cited_by',
    'references',
    'author_keywords',
    'index_keywords',
    'chemicals_cas',
    'molecular_sequence_numbers',
    # 'tradenames',
    # 'manufacturers',
    # 'funding_details',
    'funding_texts',
    # 'correspondence_address',
    'editors',
    'sponsors',
    'conference_name',
    'conference_date',
    'conference_location',
    'conference_code',
    'pubmed_id',
    'eid',
    'link',
    'ismainarticle'
    ]
    return scopus_df_3[columns_to_select]
