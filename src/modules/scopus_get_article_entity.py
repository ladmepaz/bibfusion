def scopus_get_article_entity(scopus_df_3):
    """
    Selects specific columns from the wos_df_3 DataFrame.

    Parameters:
        scopus_df_3 (pd.DataFrame): The input DataFrame containing WoS data.

    Returns:
        pd.DataFrame: A DataFrame with the selected columns.
    """
    columns_to_select = [
    'SR',
    'title',
    'abstract',
    'year',
    'author',
    'author_full_names',
    'authors_id',
    'authors_with_affiliations',
    'affiliations',
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
    'tradenames',
    'manufacturers',
    'funding_details',
    'funding_texts',
    'correspondence_address',
    'editors',
    'sponsors',
    'conference_name',
    'conference_date',
    'conference_location',
    'conference_code',
    'pubmed_id',
    'eid',
    'link'
    ]
    return scopus_df_3[columns_to_select]