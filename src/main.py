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
from modules.standarize_journal_data import standarize_journal_data  # Estandariza datos de revistas WoS
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
from modules.split_scopus_references import split_scopus_references  # Divide referencias Scopus en múltiples filas si es necesario
from modules.split_and_extract_year import split_and_extract_year  # Extrae año de referencias Scopus
from modules.extract_first_author import extract_first_author  # Extrae primer autor de referencias Scopus
from modules.extract_journal import extract_journal  # Extrae información de revista de referencias Scopus
from modules.merge_with_scimago import merge_with_scimago  # Une referencias Scopus con Scimago
from modules.add_SR_ref import add_SR_ref  # Agrega SR a referencias Scopus
from modules.extract_title import extract_title  # Extrae títulos de referencias Scopus
from modules.enrich_with_scimago import enrich_with_scimago  # Enriquecimiento de revistas Scopus con Scimago
# from modules.NEW_get_scopus_references import process_scopus_references

# ==========================
#  Comunes o compartidas
# ==========================

from modules.duplicates import remove_duplicates_df                # Elimina duplicados entre WoS y Scopus
# Parece que ya no se usa from modules.fix_missing_journal_references import fix_missing_journal_references  # Corrige referencias de revistas faltantes
from modules.enrich_references_with_openalex import enrich_references_with_openalex       # Enriquecimiento general de referencias con OpenAlex
from modules.unify_author_fullname_and_orcid import unify_author_fullname_and_orcid  # Unifica nombres de autores y ORCID
from modules.aggregate_sr_and_attach_scimago_ids import aggregate_sr_and_attach_scimago_ids  # Agrega SR y adjunta IDs de Scimago
from modules.fill_missing_issn_eissn_with_scimago import fill_missing_issn_eissn_with_scimago  # Rellena ISSN/EISSN faltantes con Scimago
from modules.resolve_duplicate_sourceids import resolve_duplicate_sourceids # Resuelve IDs de fuente duplicados en WoS
from modules.add_year_and_scimago_info import add_year_and_scimago_info

import pandas as pd

import os

def preprocesing_df(path_wos=None,path_scopus=None):

    if path_wos:
        PATH = "tests/files/WoS_results"
        if not os.path.exists(PATH):
            os.makedirs(PATH)
        print("""
              ============================
                Leyendo archivo de WoS...
              ============================
              """)
        
        # Dataframe	
        wos_df = wos_txt_to_df(path_wos)
        # wos_df.to_csv('tests/files/WoS/1_temp_wos_df.csv', index=False)
        print("1. Dataframe de WoS hecho")
        
        # Remove duplicates
        wos_df = remove_duplicates_df(wos_df)
        # wos_df.to_csv('tests/files/WoS/2_temp_wos_df_removeDuplicates.csv', index=False)
        print("2. Duplicados removidos")

        # Get references
        wos_references, wos_citation = get_wos_references(wos_df)
        wos_citation.to_csv('tests/files/WoS_results/Citation.csv', index=False)
        # wos_references.to_csv('tests/files/WoS/3_temp_wos_references.csv', index=False)

        print("3. Referencias de WoS hecha")
        
        # Enrich references with Openalex
        wos_ref_enriched = enrich_references_with_openalex(wos_references)
        wos_ref_enriched.to_csv('tests/files/WoS_results/enrich_wos_ref.csv')
        # enrich_wos_ref = pd.read_csv('tests/files/WoS/enrich_wos_ref.csv')
        print("4. Referencias de WoS enriquecidas con OpenAlex")
      

        # Merge WoS and references
        wos_df_3 = merge_wos_ref(wos_df, wos_ref_enriched)
        print("5. Dataframe de WoS y referencias unidos")
        # wos_df_3.to_csv('tests/files/WoS/5_temp_wos_df_merged.csv', index=False)
        
        ##############################################
        #           Scimago Dataframe
        ##############################################
        
        scimago = pd.read_csv('tests/files/scimago/scimago.csv')

        wos_df_4 = standarize_journal_data(wos_df_3)
        print("5.1. Estandarización de datos de revistas WoS")
        # wos_df_4.to_csv('tests/files/WoS/5_temp_wos_df_standarized.csv', index=False)


        # fill missing issn and eissn with Scimago
        wos_df_5 = fill_missing_issn_eissn_with_scimago(wos_df_4, scimago)
        print("6. Rellenado de ISSN/EISSN faltantes con Scimago")
        # wos_df_5.to_csv('tests/files/WoS/6_temp_wos_df_fixmissingjournalreferences.csv', index=False)

        wos_df_6 = aggregate_sr_and_attach_scimago_ids(wos_df_5, scimago)
        print("6.1. Agregado SR y adjuntado IDs de Scimago")
        # wos_df_6.to_csv('tests/files/WoS/6_temp_wos_df_aggregated.csv', index=False)

        journal, scimago_raw = resolve_duplicate_sourceids(wos_df_6)
        print("6.2. Resuelto IDs de fuente duplicados en WoS")
        journal.to_csv('tests/files/WoS_results/Journal.csv', index=False)
        # scimago_raw.to_csv('tests/files/WoS/6_temp_scimago_raw.csv', index=False)
        
        scimagodb = add_year_and_scimago_info(scimago_raw, wos_df_3, scimago)
        scimagodb.to_csv('tests/files/WoS_results/scimagodb.csv', index=False)



        ##############################################
        #        Article and Author Dataframe
        ##############################################
      
       
        
        # Get author data
        wos_author_raw = get_wos_author_data(wos_df_3)
        print("8. Generado 'wos_author_raw'")
        # wos_author_raw.to_csv('tests/files/WoS/8_temp_wos_author_raw.csv', index=False)
        
        # Enrich author data
        wos_author_enriched = enrich_wos_author_data(wos_author_raw)
        print("9. Enriched 'wos_author_raw'")
        # wos_author_enriched.to_csv('tests/files/WoS/9_temp_wos_author_enriched.csv', index=False)
        
        # Merge WoS and author data
        wos_author, articleauthor_wos, wos_author_affiliation_no_country = unify_author_fullname_and_orcid(wos_author_enriched)
        print("10. Generado wos_author, articleauthor_wos y wos_author_affiliation")
        wos_author.to_csv('tests/files/WoS_results/Author.csv', index=False)
        articleauthor_wos.to_csv('tests/files/WoS_results/ArticleAuthor_wos.csv', index=False)
        # wos_author_affiliation_no_country.to_csv('tests/files/WoS/10_temp_wos_author_affiliation.csv', index=False)

        # Get country affiliation
        country_codes_file = r"tests\files\country.csv"
        affiliation_0 = fill_missing_affiliations(wos_author_affiliation_no_country)
        affiliation = extract_countries(affiliation_0, country_codes_file)
        print("11. Paises de afiliación extraídos")
        affiliation.to_csv('tests/files/WoS_results/Affiliation.csv', index=False)
        
        article = get_article_entity(wos_df_3)
        print("12. Entidad de artículo obtenida")
        article.to_csv('tests/files/WoS_results/Article.csv', index=False)

     
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
        scopus_df.to_csv('tests/files/Scopus_tests/1_temp_scopus_df.csv', index=False)
        print("1. Dataframe de Scopus hecho")

        # Remove duplicates
        scopus_df_2 = remove_duplicates_df(scopus_df)
        scopus_df_2.to_csv('tests/files/Scopus_tests/2_temp_scopus_df_2.csv', index=False)
        print("2. Duplicados removidos")

        # Get references
        
        scopus_ref_1 = split_scopus_references(scopus_df_2)
        scopus_ref_1.to_csv('tests/files/Scopus_tests/3_temp_scopus_ref.csv', index=False)
        print("3. Referencias de Scopus extraídas")
        
        
        scopus_ref_2 = split_and_extract_year(scopus_ref_1)
        scopus_ref_2.to_csv('tests/files/Scopus_tests/3_temp_scopus_ref_2_split.csv', index=False)
        print("3.1. Referencias de Scopus divididas y año extraído")


        scopus_ref_3 = extract_first_author(scopus_ref_2)
        scopus_ref_3.to_csv('tests/files/Scopus_tests/3_temp_scopus_ref_3_firstauthor.csv', index=False)
        print("3.2. Primer autor extraído de las referencias de Scopus")


        scopus_ref_4 = extract_journal(scopus_ref_3)
        scopus_ref_4.to_csv('tests/files/Scopus_tests/3_temp_scopus_ref_4_journal.csv', index=False)
        print("3.3. Información de revista extraída de las referencias de Scopus")
        

        scimago = pd.read_csv('tests/files/scimago/scimago.csv')
        scopus_ref_5 = merge_with_scimago(scopus_ref_4, scimago)
        scopus_ref_5.to_csv('tests/files/Scopus_tests/3_temp_scopus_ref_5_scimago.csv', index=False)


        scopus_ref_6 = add_SR_ref(scopus_ref_5)
        scopus_ref_6.to_csv('tests/files/Scopus_tests/3_temp_scopus_ref_6_SR.csv', index=False)
        print("3.4. Referencias de Scopus enriquecidas con SR")


        # scopus_title_1 = extract_title(scopus_ref_6)
        # scopus_title_1.to_csv('tests/files/Scopus_tests/4_temp_scopus_title_1.csv', index=False)
        print("4. Títulos de Scopus extraídos")


        scopus_df_3 = merge_scopus_ref(scopus_df_2, scopus_ref_6)
        scopus_df_3.to_csv('tests/files/Scopus_tests/5_temp_scopus_df_3_merged.csv', index=False)
        print("5. Dataframe de Scopus y referencias unidos")


        scopus_journals_1 = enrich_with_scimago(scopus_df_3, scimago)
        scopus_journals_1.to_csv('tests/files/Scopus_tests/6_temp_scopus_journals_1.csv', index=False)
        print("6. Enriquecimiento de revistas Scopus con Scimago")

        # En vez de scopus_journals_1 estaba scopus_df_3
        scopus_df_4 = fill_missing_issn_eissn_with_scimago(scopus_journals_1, scimago)
        scopus_df_4.to_csv('tests/files/Scopus_tests/6_temp_scopus_df_4_fillmissingjournalreferences.csv', index=False)
        print("6.1. Rellenado de ISSN/EISSN faltantes con Scimago")


        scopus_df_5 = aggregate_sr_and_attach_scimago_ids(scopus_df_4, scimago)
        scopus_df_5.to_csv('tests/files/Scopus_tests/6_temp_scopus_df_5_aggregated.csv', index=False)
        print("6.2. Agregado SR y adjuntado IDs de Scimago")


        journal, scimago_raw = resolve_duplicate_sourceids(scopus_df_5)
        journal.to_csv('tests/files/Scopus_tests/Journal.csv', index=False)
        scimago_raw.to_csv('tests/files/Scopus_tests/scimago_raw.csv', index=False)
        print("6.3. Resuelto IDs de fuente duplicados en Scopus")


        scimagodb = add_year_and_scimago_info(scimago_raw, scopus_df_3, scimago)
        scimagodb.to_csv('tests/files/Scopus_tests/scimagodb.csv', index=False)
        print("7. Agregado año y datos de Scimago a scimagodb")



        # Get author data
        scopus_author_raw = get_scopus_author_data(scopus_df_3)
        scopus_author_raw.to_csv('tests/files/Scopus_tests/8_temp_scopus_author_raw.csv', index=False)
        print("8. Generado 'scopus_author_raw'")



        # Enrich author data
        scopus_author_enriched = enrich_scopus_author_data(scopus_author_raw)
        scopus_author_enriched.to_csv('tests/files/Scopus_tests/9_temp_scopus_author_enriched.csv', index=False)
        print("9. Enriched 'scopus_author_raw'")



        # Merge Scopus and author data
        scopus_author, articleauthor_scopus, scopus_author_affiliation = unify_author_fullname_and_orcid(scopus_author_enriched)
        scopus_author.to_csv('tests/files/Scopus_tests/scopus_author.csv', index=False)
        articleauthor_scopus.to_csv('tests/files/Scopus_tests/Articleauthor_scopus.csv', index=False)
        scopus_author_affiliation.to_csv('tests/files/Scopus_tests/Scopus_author_affiliation.csv', index=False)
        print("10. Unificado 'wos_author_enriched'")

        
        # Get country affiliation
        country_codes_file = r"tests\files\country.csv"
        scopus_author_affiliation = extract_countries(scopus_author_affiliation, country_codes_file)
        scopus_author_affiliation.to_csv('tests/files/Scopus_tests/11_temp_scopus_country.csv', index=False)
        print("11. Paises de afiliación extraídos")

        

    else:
        print("""
              No se ha ingresado un archivo de Scopus
              """) 
 
    
    
    return None


preprocesing_df(None, None)
