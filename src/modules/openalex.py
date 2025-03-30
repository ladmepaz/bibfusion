import requests
import pandas as pd
import time

def reconstruct_abstract(abstract_inverted_index):
    if not isinstance(abstract_inverted_index, dict):  # Verifica si es un diccionario
        return ""  # Devuelve un string vacío si no es válido
    abstract = {}
    for word, positions in abstract_inverted_index.items():
        for pos in positions:
            abstract[pos] = word
    return " ".join(abstract[i] for i in sorted(abstract))

# Enrich references with OpenAlex
def get_paper_info_from_doi(doi, sr_ref=None, cr_ref=None, source_title=None, year=None, authors=None):
    """
    Obtiene información detallada de un DOI usando la API de OpenAlex.
    
    Args:
        doi (str): DOI del documento
        sr_ref (str, opcional): Referencia del documento original
    
    Returns:
        dict: Diccionario con información del documento
    """
    api_url = f"https://api.openalex.org/works/doi:{doi}"
    
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()
        
        # Extracción de información básica
        title = data.get("title", "N/A")
        publication_year = data.get("publication_year", "N/A")
        source = data.get("primary_location", {}).get("source", {})
        
        # Preparar lista de autores y ORCIDs

        author_full_names = []
        orcids = []
        affiliations = []
        
        authorships = data.get("authorships", [])
        for auth in authorships:
            author_data = auth.get("author", {})
            author_name = author_data.get("display_name", "N/A")
            author_orcid = author_data.get("orcid", "N/A")
            
            # Recolectar información de autores
            author_full_names.append(author_name)
            orcids.append(author_orcid)
            
            # Recolectar afiliaciones
            author_affiliations = [
                aff.get("display_name", "N/A") 
                for aff in auth.get("institutions", [])
            ]
            affiliations.append("; ".join(author_affiliations))
        
        # Información bibliográfica
        bibliographic_info = data.get("biblio", {})
        orcids = ["NO ORCID" if not orcid else orcid for orcid in orcids]
        return {
            #"authors": "; ".join(filter(None, authors)),  # Filtra valores None
            "authors": authors,
            "doi": doi,
            "SR_ref": sr_ref,
            "CR_ref": cr_ref,
            "year": year,
            "source_title": source_title,
            "author_full_names": "; ".join(filter(None, author_full_names)),
            "journal": source.get("display_name", "N/A"),
            "year_openalex": publication_year,
            "abstract": data.get("abstract_inverted_index", "N/A"),
            "title": title,
            "volume": bibliographic_info.get("volume", "N/A"),
            "issue": bibliographic_info.get("issue", "N/A"),
            "page": f"{bibliographic_info.get('first_page', 'N/A')}-{bibliographic_info.get('last_page', 'N/A')}",
            "journal_issue_number": bibliographic_info.get("issue", "N/A"),
            "orcid": "; ".join(filter(None, orcids)),
            "affiliations": "; ".join(filter(None, affiliations)),
        }
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for DOI {doi}: {e}")
        return None

def enrich_references_with_openalex(df):
    """
    Enriquece un DataFrame de referencias con información de OpenAlex.
    
    Args:
        df (pd.DataFrame): DataFrame con DOIs en la columna 'doi' y 'SR_ref'
    
    Returns:
        pd.DataFrame: DataFrame con información enriquecida de los documentos
    """
    results = []
    
    for index, row in df.iterrows():
        # Verifica si tiene un DOI
        if 'doi' not in row or pd.isna(row['doi']) or row['doi'] == '' or row['doi'] == '-':
            print(f"Fila {index}: DOI no encontrado o vacío, saltando...")
            results.append(row.to_dict())  # Convierte la fila a diccionario y la agrega
            continue
            
        doi = row['doi']
        sr_ref = row['SR_ref']  # Usar la columna SR_ref existente
        cr_ref = row['CR_ref']  # Usar la columna CR_ref existente
        source_title = row['source_title']  # Usar la columna source_title existente
        year = row['year']  # Usar la columna year existente
        authors = row['authors']  # Usar la columna authors existente

        try:
            paper_info = get_paper_info_from_doi(doi, sr_ref, cr_ref, source_title, year, authors)
            
            if paper_info:
                results.append(paper_info)
            
            # Para evitar sobrecargar la API
            time.sleep(0.5)
        
        except Exception as e:
            print(f"Error procesando DOI {doi}: {e}")
    enrich_references = pd.DataFrame(results)
    enrich_references['abstract'] = enrich_references['abstract'].apply(reconstruct_abstract)
    # Reordenar columnas
    column_order = [
        'doi', 'SR_ref', 'CR_ref',  
        'authors', 'author_full_names', 'orcid', 'affiliations',  
        'title', 'source_title', 'journal', 'journal_issue_number',  
        'year', 'year_openalex', 'volume', 'issue', 'page'
    ]

    enrich_references = enrich_references[column_order]
    return enrich_references

# Ejemplo de uso:
# import pandas as pd
# df = pd.read_csv('referencias.csv')
# resultado = enrich_references_with_openalex(df)
# resultado.to_csv('referencias_enriquecidas.csv', index=False)