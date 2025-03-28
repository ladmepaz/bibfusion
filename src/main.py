from modules.scopus_csv_to_df import scopus_csv_to_df
from modules.wos_txt_to_df import wos_txt_to_df
from modules.duplicates import remove_duplicates_df
from modules.get_wos_references import get_wos_references
from modules.get_scopus_references import get_scopus_references
from modules.enrich_wos_ref import update_wos_ref_with_crossref
from modules.enrich_scopus_ref import update_scopus_ref_with_crossref
from modules.merge_wos_ref import merge_wos_ref
from modules.merge_scopus_ref import merge_scopus_ref
#from modules.get_country import get_country
from modules.get_wos_author_data import get_wos_author_data
from modules.enrich_wos_author_data import enrich_wos_author_data

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
        wos_references = get_wos_references(wos_df)
        print("3. Referencias de WoS hecha")

        # Enrich references with Crossref
        enrich_wos_ref = update_wos_ref_with_crossref(wos_references,'doi')
        print("4. Referencias de WoS enriquecidas con Crossref")

        # Merge WoS and references
        wos_df = merge_wos_ref(wos_df,enrich_wos_ref)
        print("5. Dataframe de WoS y referencias unidos")
        
        # Get author country
        print("6. Get author country")

        # Get author data
        wos_author_raw = get_wos_author_data(wos_df)
        print("7. Generado 'wos_author_raw'")
        
        # Enrich author data
        wos_author_enriched = enrich_wos_author_data(wos_author_raw)
        print("8. Enriched 'wos_author_raw'")
        print(wos_author_enriched.columns)
        
        
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
        enrich_scopus_ref = update_scopus_ref_with_crossref(scopus_references,'doi')
        print("4. Referencias de Scopus Enriquecidas con Crossref")
        
        # Merge Scopus and references
        scopus_df = merge_scopus_ref(scopus_df,enrich_scopus_ref)
        print("5. Dataframe de Scopus y referencias unidos")
        
    else:
        print("""
              No se ha ingresado un archivo de Scopus
              """) 
 
    
    
    return None


preprocesing_df('tests/files/wos_5results.txt','tests/files/scopus.csv')



