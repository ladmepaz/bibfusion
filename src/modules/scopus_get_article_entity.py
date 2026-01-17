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
    
    # --- Eliminar duplicados por DOI sin borrar filas con DOI vacío ---
    scopus_df_3['doi'] = scopus_df_3['doi'].astype(str).str.strip()

    mask_doi_vacio = scopus_df_3['doi'].isin(["", "nan", "None", None])
    df_con_doi = scopus_df_3[~mask_doi_vacio]

    duplicados = df_con_doi.duplicated(subset=['doi'], keep='first')
    print(f"Se encontraron {duplicados.sum()} filas duplicadas basadas en 'doi' (excluyendo DOIs vacíos).")

    df_con_doi = df_con_doi.drop_duplicates(subset=['doi'], keep='first')

    # Volver a unir filas con y sin DOI
    scopus_df_3 = pd.concat([df_con_doi, scopus_df_3[mask_doi_vacio]], ignore_index=True)

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
    'correspondence_address',
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
