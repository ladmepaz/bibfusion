from modules.scopus_csv_to_df import scopus_csv_to_df
from modules.wos_txt_to_df import wos_txt_to_df
from modules.duplicates import remove_duplicates_df
from modules.get_wos_references import get_wos_references
from modules.get_scopus_references import get_scopus_references
from modules.enrich_wos_ref import update_wos_ref_with_crossref


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
        print("3. Referencias de WoS hecha")
        wos_references = get_wos_references(wos_df)
        enrich_wos_ref = update_wos_ref_with_crossref(wos_references,'doi')
        enrich_wos_ref.to_csv('tests/files/enrich_wos_ref.csv', index=False)
        # print(enrich_wos_ref_result)
        
        
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
        print("1. Dataframe de Scopus hecho")
        scopus_df = scopus_csv_to_df(path_scopus)
        # Remove duplicates
        print("2. Duplicados removidos")
        scopus_df = remove_duplicates_df(scopus_df)
        # Get references
        print("3. Referencias de Scopus hecha")
        scopus_references, scopus_df = get_scopus_references(scopus_df)
        # print(scopus_references.columns)
        # print(scopus_df.columns)
    else:
        print("""
              No se ha ingresado un archivo de Scopus
              """)
        
 
    
    
    return None


preprocesing_df('tests/files/wos.txt','tests/files/scopus.csv')





#wos_references.to_csv('tests/files/wos_references.csv', index=False)


