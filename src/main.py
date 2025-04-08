# ==========================
#  Web of Science (WoS)
# ==========================

from modules.wos_txt_to_df import wos_txt_to_df                    # Convierte archivos WoS .txt a DataFrame
from modules.get_wos_references import get_wos_references          # Extrae referencias bibliográficas de WoS
from modules.enrich_wos_ref import update_wos_ref_with_crossref   # Enriquecimiento de referencias WoS con Crossref
from modules.merge_wos_ref import merge_wos_ref                    # Une referencias WoS (múltiples fuentes o duplicados)
from modules.enrich_wos_journals import enrich_wos_journals        # Añade información adicional de revistas WoS
from modules.get_wos_author_data import get_wos_author_data        # Obtiene datos de autores desde WoS
from modules.enrich_wos_author_data import enrich_wos_author_data  # Enriquecimiento de datos de autores WoS
from modules.get_country_affiliation import extract_countries  # Obtiene datos de afiliación de autores desde WoS

# ==========================
#  Scopus
# ==========================

from modules.scopus_csv_to_df import scopus_csv_to_df              # Convierte archivos Scopus .csv a DataFrame
from modules.get_scopus_references import get_scopus_references    # Extrae referencias bibliográficas de Scopus
from modules.enrich_scopus_ref import update_scopus_ref_with_crossref  # Enriquecimiento de referencias Scopus con Crossref
from modules.merge_scopus_ref import merge_scopus_ref              # Une referencias Scopus
from modules.get_scopus_author_data import get_scopus_author_data

# ==========================
#  Comunes o compartidas
# ==========================

from modules.duplicates import remove_duplicates_df                # Elimina duplicados entre WoS y Scopus
from modules.fix_missing_journal_references import fix_missing_journal_references  # Corrige referencias de revistas faltantes
from modules.openalex import enrich_references_with_openalex       # Enriquecimiento general de referencias con OpenAlex
from modules.unify_author_fullname_and_orcid import unify_author_fullname_and_orcid  # Unifica nombres de autores y ORCID

import pandas as pd

def preprocesing_df(path_wos=None,path_scopus=None):
    
    if path_wos:
        print("""
              ============================
                Leyendo archivo de WoS...
              ============================
              """)
        
        # Dataframe	
        wos_df = wos_txt_to_df(path_wos)
        print("1. Dataframe de WoS hecho")
        
        # Remove duplicates
        wos_df = remove_duplicates_df(wos_df)
        print("2. Duplicados removidos")

        # Get references
        wos_references, wos_citation = get_wos_references(wos_df)
        print("3. Referencias de WoS hecha")
        
        # Enrich references with Openalex
        enrich_wos_ref = pd.read_csv('tests/files/enrich_wos_ref.csv')
        print("4. Referencias de WoS enriquecidas con OpenAlexf")

        # Merge WoS and references
        wos_df_3 = merge_wos_ref(wos_df,enrich_wos_ref)
        print("5. Dataframe de WoS y referencias unidos")
        
        # fix missing journal references
        wos_df_4 = fix_missing_journal_references(wos_df_3)
        print("6. Fix missing journal references")

        # Enrich wos journals with Scimago
        scimago = pd.read_csv('tests/files/scimago_2025_combined.csv')
        print(wos_df_4.columns)
        
        # wos_df_5 = enrich_wos_journals(wos_df_4, scimago)
        print("7. Enriched wos journals with Scimago")
        
        # Get author data
        print(wos_df_3.columns)
        wos_author_raw = get_wos_author_data(wos_df_3)
        print("8. Generado 'wos_author_raw'")
        
        # # Enrich author data
        wos_author_enriched = enrich_wos_author_data(wos_author_raw)
        print("9. Enriched 'wos_author_raw'")
        
        # # Merge WoS and author data
        wos_author, articleauthor, wos_author_affiliation = unify_author_fullname_and_orcid(wos_author_enriched)
        print("10. Unificado 'wos_author_enriched'")

        # Get country affiliation
        country_codes_file = r"tests\files\country.csv"
        #wos_author_affiliation = extract_countries(wos_author_affiliation, country_codes_file)
        print("11. Paises de afiliación extraídos")

     
    else:
        print("""
              No se ha ingresado un archivo de WoS
              """)
    
    
    if path_scopus:
        print("""
              ===============================
                Leyendo archivo de Scopus...
              ===============================
              """)
        
        # Dataframe
        scopus_df = scopus_csv_to_df(path_scopus)
        print("1. Dataframe de Scopus hecho")

        # Remove duplicates
        scopus_df = remove_duplicates_df(scopus_df)
        print("2. Duplicados removidos")

        # Get references
        scopus_references, scopus_df = get_scopus_references(scopus_df)
        print("3. Referencias de Scopus hecha")
        # Enrich references with Crossref
        enrich_scopus_ref = update_scopus_ref_with_crossref(scopus_references)
        print("4. Referencias de Scopus Enriquecidas con Crossref")
        
        # Merge Scopus and references
        scopus_df_3 = merge_scopus_ref(scopus_df,enrich_scopus_ref)
        print("5. Dataframe de Scopus y referencias unidos")
        
        # fix missing journal references
        scopus_df_2 = fix_missing_journal_references(scopus_df)
        print("6. Fix missing journal references")	
        
        # Get author data
        scopus_author_raw = get_scopus_author_data(scopus_df_3)
        print("8. Generado 'scopus_author_raw'")
    else:
        print("""
              No se ha ingresado un archivo de Scopus
              """) 
 
    
    
    return None


preprocesing_df(r'tests\files\wos_3results.txt','tests/files/scopus.csv')



