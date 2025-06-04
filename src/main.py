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
from modules.standarize_journal_data import standarize_journal_data  # Estandariza datos de revistas WoS
from modules.aggregate_sr_and_attach_scimago_ids import aggregate_sr_and_attach_scimago_ids  # Agrega SR y adjunta IDs de Scimago
from modules.fill_missing_issn_eissn_with_scimago import fill_missing_issn_eissn_with_scimago  # Rellena ISSN/EISSN faltantes con Scimago
from modules.resolve_duplicate_sourceids import resolve_duplicate_sourceids # Resuelve IDs de fuente duplicados en WoS
from modules.add_year_and_scimago_info import add_year_and_scimago_info
from modules.get_country_affiliation import fill_missing_affiliations, extract_countries  # Extrae países de afiliación de autores
from modules.get_article_entity import get_article_entity  # Obtiene entidad de artículo desde WoS

# ==========================
#  Scopus
# ==========================

from modules.scopus_csv_to_df import scopus_csv_to_df              # Convierte archivos Scopus .csv a DataFrame
from modules.get_scopus_references import process_scopus_references    # Extrae referencias bibliográficas de Scopus
from modules.enrich_scopus_ref import enrich_references_with_journal_abbr  # Enriquecimiento de referencias Scopus con Crossref
from modules.merge_scopus_ref import merge_scopus_ref              # Une referencias Scopus
from modules.get_scopus_author_data import get_scopus_author_data
from modules.enrich_scopus_author_data import enrich_scopus_author_data  # Enriquecimiento de datos de autores
# from modules.NEW_get_scopus_references import process_scopus_references

# ==========================
#  Comunes o compartidas
# ==========================

from modules.duplicates import remove_duplicates_df                # Elimina duplicados entre WoS y Scopus
from modules.fix_missing_journal_references import fix_missing_journal_references  # Corrige referencias de revistas faltantes
from modules.enrich_references_with_openalex import enrich_references_with_openalex       # Enriquecimiento general de referencias con OpenAlex
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
        wos_df.to_csv('tests/files/WoS/1_temp_wos_df.csv', index=False)
        print("1. Dataframe de WoS hecho")
        
        # Remove duplicates
        wos_df = remove_duplicates_df(wos_df)
        wos_df.to_csv('tests/files/WoS/2_temp_wos_df_removeDuplicates.csv', index=False)
        print("2. Duplicados removidos")

        # Get references
        wos_references, wos_citation = get_wos_references(wos_df)
        wos_references.to_csv('tests/files/WoS/3_temp_wos_references.csv', index=False)
        wos_df.to_csv('tests/files/WoS/3_temp_wos_df.csv', index=False)
        print("3. Referencias de WoS hecha")
        
        # Enrich references with Openalex
        enrich_wos_ref = pd.read_csv('tests/files/WoS/enrich_wos_ref.csv')
        print("4. Referencias de WoS enriquecidas con OpenAlex")
      

        # Merge WoS and references
        wos_df_3 = merge_wos_ref(wos_df, enrich_wos_ref)
        print("5. Dataframe de WoS y referencias unidos")
        wos_df_3.to_csv('tests/files/WoS/5_temp_wos_df_merged.csv', index=False)
        
        ##############################################
        #           Scimago Dataframe
        ##############################################
        
        scimago = pd.read_csv('tests/files/scimago/scimago.csv')

        wos_df_4 = standarize_journal_data(wos_df_3)
        print("5.1. Estandarización de datos de revistas WoS")
        wos_df_4.to_csv('tests/files/WoS/5_temp_wos_df_standarized.csv', index=False)


        # fill missing issn and eissn with Scimago
        wos_df_5 = fill_missing_issn_eissn_with_scimago(wos_df_4, scimago)
        print("6. Rellenado de ISSN/EISSN faltantes con Scimago")
        wos_df_5.to_csv('tests/files/WoS/6_temp_wos_df_fixmissingjournalreferences.csv', index=False)

        wos_df_6 = aggregate_sr_and_attach_scimago_ids(wos_df_5, scimago)
        print("6.1. Agregado SR y adjuntado IDs de Scimago")
        wos_df_6.to_csv('tests/files/WoS/6_temp_wos_df_aggregated.csv', index=False)

        journal, scimago_raw = resolve_duplicate_sourceids(wos_df_6)
        print("6.2. Resuelto IDs de fuente duplicados en WoS")
        journal.to_csv('tests/files/WoS/6_temp_wos_journal.csv', index=False)
        scimago_raw.to_csv('tests/files/WoS/6_temp_scimago_raw.csv', index=False)
        
        scimagodb = add_year_and_scimago_info(scimago_raw, wos_df_3, scimago)



        ##############################################
        #        Article and Author Dataframe
        ##############################################
      
       
        
        # Get author data
        wos_author_raw = get_wos_author_data(wos_df_3)
        print("8. Generado 'wos_author_raw'")
        wos_author_raw.to_csv('tests/files/WoS/8_temp_wos_author_raw.csv', index=False)
        
        # Enrich author data
        wos_author_enriched = enrich_wos_author_data(wos_author_raw)
        print("9. Enriched 'wos_author_raw'")
        wos_author_enriched.to_csv('tests/files/WoS/9_temp_wos_author_enriched.csv', index=False)
        
        # Merge WoS and author data
        wos_author, articleauthor_wos, wos_author_affiliation_no_country = unify_author_fullname_and_orcid(wos_author_enriched)
        print("10. Generado wos_author, articleauthor_wos y wos_author_affiliation")
        wos_author.to_csv('tests/files/WoS/10_temp_wos_author.csv', index=False)
        articleauthor_wos.to_csv('tests/files/WoS/10_temp_articleauthor_wos.csv', index=False)
        wos_author_affiliation_no_country.to_csv('tests/files/WoS/10_temp_wos_author_affiliation.csv', index=False)

        # Get country affiliation
        country_codes_file = r"tests\files\country.csv"
        affiliation_0 = fill_missing_affiliations(wos_author_affiliation_no_country)
        affiliation = extract_countries(affiliation_0, country_codes_file)
        print("11. Paises de afiliación extraídos")
        affiliation.to_csv('tests/files/WoS/11_temp_wos_country.csv', index=False)
        
        article = get_article_entity(wos_df_3)
        print("12. Entidad de artículo obtenida")
        article.to_csv('tests/files/WoS/12_temp_wos_article.csv', index=False)

     
    else:
        print("""
              ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
               No se ha ingresado un archivo de WoS
              ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
              """)
    
    
    if path_scopus:
        print("""
              ===============================
                Leyendo archivo de Scopus...
              ===============================
              """)
        
        # Dataframe
        scopus_df = scopus_csv_to_df(path_scopus)
        scopus_df.to_csv('tests/files/1_temp_scopus_df.csv', index=False)
        print("1. Dataframe de Scopus hecho")

        # Remove duplicates
        scopus_df = remove_duplicates_df(scopus_df)
        scopus_df.to_csv('tests/files/2_temp_scopus_df_removeDuplicates.csv', index=False)
        print("2. Duplicados removidos")

        # Get references
        
        # scopus_references, scopus_df = process_scopus_references(scopus_df)
        # scopus_references.to_csv('tests/files/3_temp_scopus_references_NEW.csv', index=False)
        # scopus_df.to_csv('tests/files/3_temp_scopus_df.csv', index=False) # Dataframe de Scopus con referencias
        scopus_df = pd.read_csv('tests/files/3_temp_scopus_df.csv')
        scopus_references = pd.read_csv('tests/files/3_temp_scopus_references_NEW.csv')
        print("3. Referencias de Scopus hecha")
        
        # Enrich references with Crossref
        enrich_scopus_ref = enrich_references_with_journal_abbr(scopus_references, 'doi')
        enrich_scopus_ref.to_csv('tests/files/4_temp_enrich_scopus_ref.csv', index=False)
        print("4. Referencias de Scopus Enriquecidas con Crossref")
        
        # Merge Scopus and references
        scopus_df_3 = merge_scopus_ref(scopus_df, enrich_scopus_ref)
        scopus_df_3.to_csv('tests/files/5_temp_scopus_df_merged.csv', index=False)
        print("5. Dataframe de Scopus y referencias unidos")
        
        # fix missing journal references
        scopus_df_2 = fix_missing_journal_references(scopus_df)
        scopus_df_2.to_csv('tests/files/6_temp_scopus_df_fixmissingjournalreferences.csv', index=False)
        print("6. Fix missing journal references")	
        
        # =======================================================
        # scopus_df_3 = enrich_wos_journals(scopus_df_2, scimago)
        # =======================================================
        print("7. Enriched Scopus journals with Scimago")

        # Get author data
        scopus_author_raw = get_scopus_author_data(scopus_df_3)
        scopus_author_raw.to_csv('tests/files/8_temp_scopus_author_raw.csv', index=False)
        print("8. Generado 'scopus_author_raw'")

        # Enrich author data
        scopus_author_enriched = enrich_scopus_author_data(scopus_author_raw)
        scopus_author_enriched.to_csv('tests/files/9_temp_scopus_author_enriched.csv', index=False)
        print("9. Enriched 'wos_author_raw'")

        # Merge Scopus and author data
        scopus_author, articleauthor_scopus, scopus_author_affiliation = unify_author_fullname_and_orcid(scopus_author_enriched)
        scopus_author.to_csv('tests/files/10_temp_scopus_author.csv', index=False)
        articleauthor_scopus.to_csv('tests/files/10_temp_articleauthor_scopus.csv', index=False)
        scopus_author_affiliation.to_csv('tests/files/10_temp_scopus_author_affiliation.csv', index=False)
        print("10. Unificado 'wos_author_enriched'")
        
        # Get country affiliation
        country_codes_file = r"tests\files\country.csv"
        scopus_author_affiliation = extract_countries(scopus_author_affiliation, country_codes_file)
        scopus_author_affiliation.to_csv('tests/files/11_temp_scopus_country.csv', index=False)
        print("11. Paises de afiliación extraídos")

        

    else:
        print("""
              No se ha ingresado un archivo de Scopus
              """) 
 
    
    
    return None


preprocesing_df('tests/files/wos.txt', None)
