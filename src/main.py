from modules.scopus_csv_to_df import scopus_csv_to_df
from modules.wos_txt_to_df import wos_txt_to_df
from modules.duplicates import remove_duplicates_df
from modules.get_wos_references import get_wos_references
from modules.get_scopus_references import get_scopus_references


def preprocesing_df(path_wos=None,path_scopus=None):
    
    if path_wos:
        # Dataframe	
        wos_df = wos_txt_to_df(path_wos)
        # Remove duplicates
        wos_df = remove_duplicates_df(wos_df)
        # Get references
        wos_references = get_wos_references(wos_df)
    
    
    if path_scopus:
        # Dataframe
        scopus_df = scopus_csv_to_df(path_scopus)
        # Remove duplicates
        print(f"Estas son las columnas al princicio {scopus_df.columns}\n\n")
        scopus_df = remove_duplicates_df(scopus_df)
        # Get references
        scopus_references = get_scopus_references(scopus_df)
        print(scopus_references)
    
 
    
    
    return None


preprocesing_df('tests/files/wos.txt','tests/files/scopus.csv')





#wos_references.to_csv('tests/files/wos_references.csv', index=False)


