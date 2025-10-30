def get_article_entity(wos_df_3):
    """
    Delete duplicate rows based on 'doi'
    Selects specific columns from the wos_df_3 DataFrame.

    Parameters:
        wos_df_3 (pd.DataFrame): The input DataFrame containing WoS data.

    Returns:
        pd.DataFrame: A DataFrame with the selected columns.
    """
    # Verificar duplicados en 'doi'
    duplicados = wos_df_3.duplicated(subset=['doi'], keep='first')
    print(f"Se encontraron {duplicados.sum()} filas duplicadas basadas en 'doi'.")
    
    # Eliminar duplicados (manteniendo la primera ocurrencia)
    wos_df_3 = wos_df_3.drop_duplicates(subset=['doi'], keep='first')

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
    return wos_df_3[columns_to_select]
