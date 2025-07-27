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
from modules.get_openalex_data import generate_references_column, generate_SR_ref, openalex_enrich_ref, fill_source_title_from_scimago
from modules.citation_scopus import citation_scopus  # Extrae citas de Scopus
from modules.scopus_get_article_entity import scopus_get_article_entity  # Obtiene entidad de artículo desde Scopus
from modules.fill_author_from_full_names import fill_author_from_full_names  # Rellena nombres de autores desde nombres completos
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
import os
import time
def medir_tiempo(func):
    def wrapper(*args, **kwargs):
        inicio = time.perf_counter()
        resultado = func(*args, **kwargs)
        fin = time.perf_counter()
        print(f"{func.__name__}' ejecutado en {fin - inicio:.2f} segundos")
        return resultado
    return wrapper
import pandas as pd

@medir_tiempo
def preprocesing_df(path_wos=None,path_scopus=None):

    if path_wos:
        # Obtener la carpeta donde está el archivo path_wos
        base_dir = os.path.dirname(path_wos)
        output_dir = os.path.join(base_dir, "WoS_results")
        # Crear la carpeta si no existe
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        print("""
              ============================
                Leyendo archivo de WoS...
              ============================
              """)
        
        # Dataframe	
        wos_df = wos_txt_to_df(path_wos)
        wos_df.to_csv(os.path.join(output_dir, "1_temp_wos_df.csv"), index=False)
        print("1. Dataframe de WoS hecho")
        
        # Remove duplicates
        wos_df = remove_duplicates_df(wos_df)
        wos_df.to_csv(os.path.join(output_dir, "2_temp_wos_df_removeDuplicates.csv"), index=False)
        print("2. Duplicados removidos")

        # Get references
        wos_references, wos_citation = get_wos_references(wos_df)
        wos_citation.to_csv(os.path.join(output_dir, "Citation.csv"), index=False)
        wos_references.to_csv(os.path.join(output_dir, "3_temp_wos_references.csv"), index=False)
        print("3. Referencias de WoS hecha")
        
        # Enrich references with Openalex
        wos_ref_enriched = enrich_references_with_openalex(wos_references)
        wos_ref_enriched.to_csv(os.path.join(output_dir,'enrich_wos_ref.csv'))
        # wos_ref_enriched = pd.read_csv(os.path.join(output_dir,'enrich_wos_ref.csv'))
        print("4. Referencias de WoS enriquecidas con OpenAlex")
      
        # Merge WoS and references
        wos_df_3 = merge_wos_ref(wos_df, wos_ref_enriched)
        wos_df_3.to_csv(os.path.join(output_dir,'5_temp_wos_df_merged.csv'), index=False)
        # wos_df_3 = pd.read_csv(os.path.join(output_dir,'5_temp_wos_df_merged.csv'))
        print("5. Dataframe de WoS y referencias unidos")
        
        
        ##############################################
        #           Scimago Dataframe
        ##############################################

        scimago = pd.read_csv('tests/files/scimago/scimago.csv')

        wos_df_4 = standarize_journal_data(wos_df_3)
        wos_df_4.to_csv(os.path.join(output_dir,'5_temp_wos_df_standarized.csv'), index=False)
        # wos_df_4 = pd.read_csv(os.path.join(output_dir,'5_temp_wos_df_standarized.csv'))
        print("5.1. Estandarización de datos de revistas WoS")

        # fill missing issn and eissn with Scimago
        wos_df_5 = fill_missing_issn_eissn_with_scimago(wos_df_4, scimago)
        wos_df_5.to_csv(os.path.join(output_dir,'6_temp_wos_df_fixmissingjournalreferences.csv'), index=False)
        # wos_df_5 = pd.read_csv(os.path.join(output_dir,'6_temp_wos_df_fixmissingjournalreferences.csv'))
        print("6. Rellenado de ISSN/EISSN faltantes con Scimago")

        wos_df_6 = aggregate_sr_and_attach_scimago_ids(wos_df_5, scimago)
        wos_df_6.to_csv(os.path.join(output_dir,'6_temp_wos_df_aggregated.csv'), index=False)
        # wos_df_6 = pd.read_csv(os.path.join(output_dir,'6_temp_wos_df_aggregated.csv'))
        print("6.1. Agregado SR y adjuntado IDs de Scimago")

        journal, scimago_raw = resolve_duplicate_sourceids(wos_df_6)
        journal.to_csv(os.path.join(output_dir,'Journal.csv'), index=False)
        scimago_raw.to_csv(os.path.join(output_dir,'6_temp_scimago_raw.csv'), index=False)
        # journal = pd.read_csv(os.path.join(output_dir,'Journal.csv'))
        # scimago_raw = pd.read_csv(os.path.join(output_dir,'6_temp_scimago_raw.csv'))
        print("6.2. Resuelto IDs de fuente duplicados en WoS")
        
        scimagodb = add_year_and_scimago_info(scimago_raw, wos_df_3, scimago)
        scimagodb.to_csv(os.path.join(output_dir,'scimagodb.csv'), index=False)
        # scimagodb = pd.read_csv(os.path.join(output_dir,'scimagodb.csv'))


        ##############################################
        #        Article and Author Dataframe
        ##############################################
        
        # Get author data
        wos_author_raw = get_wos_author_data(wos_df_3)
        wos_author_raw.to_csv(os.path.join(output_dir,'8_temp_wos_author_raw.csv'), index=False)
        # wos_author_raw = pd.read_csv(os.path.join(output_dir,'8_temp_wos_author_raw.csv'))
        print("8. Generado 'wos_author_raw'")
        
        # Enrich author data
        wos_author_enriched = enrich_wos_author_data(wos_author_raw)
        wos_author_enriched.to_csv(os.path.join(output_dir,'9_temp_wos_author_enriched.csv'), index=False)
        # wos_author_enriched = pd.read_csv(os.path.join(output_dir,'9_temp_wos_author_enriched.csv'))
        print("9. Enriched 'wos_author_raw'")
        
        # Merge WoS and author data
        wos_author, articleauthor_wos, wos_author_affiliation_no_country = unify_author_fullname_and_orcid(wos_author_enriched)
        print("10. Generado wos_author, articleauthor_wos y wos_author_affiliation")
        wos_author.to_csv(os.path.join(output_dir,'Author.csv'), index=False)
        articleauthor_wos.to_csv(os.path.join(output_dir,'ArticleAuthor_wos.csv'), index=False)
        wos_author_affiliation_no_country.to_csv(os.path.join(output_dir,'10_temp_wos_author_affiliation.csv'), index=False)

        # Get country affiliation
        country_codes_file = "tests/files/country.csv"
        affiliation_0 = fill_missing_affiliations(wos_author_affiliation_no_country)
        affiliation = extract_countries(affiliation_0, country_codes_file)
        print("11. Paises de afiliación extraídos")
        affiliation.to_csv(os.path.join(output_dir,'Affiliation.csv'), index=False)

        article = get_article_entity(wos_df_3)
        print("12. Entidad de artículo obtenida")
        article.to_csv(os.path.join(output_dir,'Article.csv'), index=False)

     
    else:
        print("""
              ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
               No se ha ingresado un archivo de WoS
              ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
              """)
    
    
    if path_scopus:
        # Obtener la carpeta donde está el archivo path_wos
        base_dir = os.path.dirname(path_wos)
        output_dir = os.path.join(base_dir, "Scopus_results")
        # Crear la carpeta si no existe
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        print("""
              ===============================
                Leyendo archivo de Scopus...
              ===============================
              """)
        
        # Dataframe
        scopus_df = scopus_csv_to_df(path_scopus)
        scopus_df.to_csv(os.path.join(output_dir,'1_temp_scopus_df.csv'), index=False)
        # scopus_df = pd.read_csv(os.path.join(output_dir,'1_temp_scopus_df.csv'))
        print("1. Dataframe de Scopus hecho")

        # Remove duplicates
        scopus_df_2 = remove_duplicates_df(scopus_df)
        scopus_df_2.to_csv(os.path.join(output_dir,'2_temp_scopus_df_2.csv'), index=False)
        # scopus_df_2 = pd.read_csv(os.path.join(output_dir,'2_temp_scopus_df_2.csv'))
        print("2. Duplicados removidos")

        
        # extract references
        extraction_linksref_openalex = generate_references_column(scopus_df_2)
        extraction_linksref_openalex.to_csv(os.path.join(output_dir,'3_temp_extraction_linksref_openalex.csv'), index=False)
        # extraction_linksref_openalex = pd.read_csv(os.path.join(output_dir,'3_temp_extraction_linksref_openalex.csv'))
        print("3. Extracción de referencias de Scopus hecha")

        # Enrich references with OpenAlex
        enrich_ref = openalex_enrich_ref(extraction_linksref_openalex)
        enrich_ref.to_csv(os.path.join(output_dir,'4_temp_extraction_linksref_openalex_enriched.csv'))
        # enrich_ref = pd.read_csv(os.path.join(output_dir,'4_temp_extraction_linksref_openalex_enriched.csv'))
        enrich_ref = enrich_ref.rename(columns={'orcids': 'orcid'})
        print("4. Enriquecimiento de referencias de Scopus con OpenAlex hecho")


        scimago = pd.read_csv('tests/files/scimago/scimago.csv')

        df_enriched_1 = fill_source_title_from_scimago(enrich_ref, scimago)
        df_enriched_1.to_csv(os.path.join(output_dir,'5_temp_extraction_linksref_openalex_sourcetitle.csv'), index=False)
        # df_enriched_1 = pd.read_csv(os.path.join(output_dir,'5_temp_extraction_linksref_openalex_sourcetitle.csv'))
        print("5. Llenada columna 'source_title' desde Scimago")

        # Generate SR_ref
        df_with_sr = generate_SR_ref(df_enriched_1)
        df_with_sr.to_csv(os.path.join(output_dir,'6_scopus_ref_enriched.csv'), index=False)
        df_with_sr = pd.read_csv(os.path.join(output_dir,'6_scopus_ref_enriched.csv'))
        print("6. Generada columna 'SR_ref' con formato: PRIMERAUTOR, AÑO, SOURCE_TITLE")

        # Generate Citation DataFrame
        scopus_citation = citation_scopus(df_with_sr)
        scopus_citation.to_csv(os.path.join(output_dir,'Citation.csv'), index=False)
        # scopus_citation = pd.read_csv(os.path.join(output_dir,'Citation.csv'))
        print("7. DataFrame de citas de Scopus generado")

        # Enrich references with journal abbreviation
        scopus_df_3 = merge_scopus_ref(scopus_df_2, df_with_sr)
        print("8. Dataframe de Scopus y referencias unidos")

        scopus_df_3 = fill_author_from_full_names(scopus_df_3)
        scopus_df_3.to_csv(os.path.join(output_dir,'7_temp_scopus_df_3.csv'), index=False)
        # scopus_df_3 = pd.read_csv(os.path.join(output_dir,'7_temp_scopus_df_3.csv'))
        print("8.1. Nombres de autores rellenados desde nombres completos")

        # Article Dataframe
        article = scopus_get_article_entity(scopus_df_3)
        article.to_csv(os.path.join(output_dir,'Article.csv'), index=False)
        # article = pd.read_csv(os.path.join(output_dir,'Article.csv'))
        print("9. Entidad de artículo obtenida")

        ##############################################
        #           Scimago Dataframe
        ##############################################

        scimago = pd.read_csv('tests/files/scimago/scimago.csv')

        # Standarize journal data
        scopus_df_4 = standarize_journal_data(scopus_df_3)
        scopus_df_4.to_csv(os.path.join(output_dir,'8_temp_scopus_df_4.csv'), index=False)
        # scopus_df_4 = pd.read_csv(os.path.join(output_dir,'8_temp_scopus_df_4.csv'))
        print("10. Estandarización de datos de revistas Scopus")

        # Fill missing ISSN with Scimago
        scopus_df_5 = fill_missing_issn_eissn_with_scimago(scopus_df_4, scimago)
        scopus_df_5.to_csv(os.path.join(output_dir,'9_temp_scopus_df_5.csv'), index=False)
        # scopus_df_5 = pd.read_csv(os.path.join(output_dir,'9_temp_scopus_df_5.csv'))
        print("11. Rellenado de ISSN/EISSN faltantes con Scimago")

        # Adjust scopus_df_5
        scopus_df_5 = scopus_df_5.rename(columns={'issn':'eissn'})
        scopus_df_5['issn'] = pd.NA
        scopus_df_5['issn'] = scopus_df_5['issn'].astype(str).str.strip()
        scimago['Issn'] = scimago['Issn'].astype(str).str.strip()
        scopus_df_5.to_csv(os.path.join(output_dir,'10_temp_scopus_df_5_cleaned.csv'), index=False)


        # Add SR and attach Scimago IDs
        scopus_df_6 = aggregate_sr_and_attach_scimago_ids(scopus_df_5, scimago)
        scopus_df_6.to_csv(os.path.join(output_dir,'10_temp_scopus_df_6.csv'), index=False)
        print("12. Agregado SR y adjuntado IDs de Scimago")

        # Resolve duplicate source IDs
        journal, scimago_raw = resolve_duplicate_sourceids(scopus_df_6)
        journal.to_csv(os.path.join(output_dir,'Journal.csv'), index=False)
        scimago_raw.to_csv(os.path.join(output_dir,'scimago_raw.csv'), index=False)
        print("13. Resuelto IDs de fuente duplicados en Scopus")
        
        scopus_df_3['year'] = pd.to_numeric(scopus_df_3['year'], errors='coerce')
        scopus_df_3['year'] = scopus_df_3['year'].dropna().astype(int).astype(str).reindex(scopus_df_3.index)

        scimago['year'] = pd.to_numeric(scimago['year'], errors='coerce')
        scimago['year'] = scimago['year'].dropna().astype(int).astype(str).reindex(scimago.index)

        # Enrich with Scimago
        scimagodb = add_year_and_scimago_info(scimago_raw, scopus_df_3, scimago)
        scimagodb.to_csv(os.path.join(output_dir,'scimagodb.csv'), index=False)
        print("14. Añadido año e información de Scimago a scimagodb")

        ##############################################
        #        Author Dataframes
        ##############################################

        # Get author data
        scopus_author_raw = get_scopus_author_data(scopus_df_3)
        scopus_author_raw.to_csv(os.path.join(output_dir,'scopus_author_raw.csv'), index=False)
        print("15. Generado 'scopus_author_raw'")


        # Enrich author data
        scopus_author_enriched = enrich_scopus_author_data(scopus_author_raw)
        scopus_author_enriched.to_csv(os.path.join(output_dir,'scopus_author_enriched.csv'), index=False)
        print("16. Enriched 'scopus_author_raw'")


        # Merge Scopus and author data
        scopus_author, articleauthor_scopus, scopus_author_affiliation_no_country = unify_author_fullname_and_orcid(scopus_author_enriched)
        scopus_author.to_csv(os.path.join(output_dir,'Author.csv'), index=False)
        articleauthor_scopus.to_csv(os.path.join(output_dir,'ArticleAuthor_scopus.csv'), index=False)
        print("17. Generado scopus_author, articleauthor_scopus y scopus_author_affiliation")


        # Get country affiliation
        country_codes_file = "tests/files/country.csv"
        affiliation_0 = fill_missing_affiliations(scopus_author_affiliation_no_country)
        affiliation = extract_countries(affiliation_0, country_codes_file)
        affiliation.to_csv(os.path.join(output_dir,'Affiliation.csv'), index=False)
        print("18. Paises de afiliación extraídos")
        

    else:
        print("""
              No se ha ingresado un archivo de Scopus
              """) 
 
    
    
    return None

# preprocesing_df( path_wos, path_scopus)
preprocesing_df( None, 
                None)