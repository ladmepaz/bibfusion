# ==========================
#  Web of Science (WoS)
# ==========================

from .modules import wos_txt_to_df  # Converts WoS .txt files to DataFrame
from .modules import get_wos_references  # Extracts bibliographic references from WoS
from .modules import merge_wos_ref  # Merges WoS references (multiple sources or duplicates)
from .modules import get_wos_author_data  # Gets author data from WoS
from .modules import enrich_wos_author_data  # Enriches WoS author data
from .modules import get_article_entity  # Gets article entity from WoS
from .modules import enrich_references_with_openalex # General reference enrichment with OpenAlex
from .modules import enrich_wos_with_openalex_authors  # Enrich WoS authors (names/ORCIDs/IDs) with OpenAlex

# ==========================
#  Scopus
# ==========================

from .modules import scopus_csv_to_df # Converts Scopus .csv files to DataFrame
from .modules import merge_scopus_ref # Merges Scopus references
from .modules import get_scopus_author_data # Gets author data from Scopus
from .modules import enrich_scopus_author_data  # Enriches Scopus author data
from .modules import enrich_scopus_with_openalex_authors  # Enriches Scopus authors from OpenAlex
from .modules import generate_references_column, generate_SR_ref, openalex_enrich_ref, fill_source_title_from_scimago # Reference extraction and enrichment for Scopus
from .modules import citation_scopus  # Extracts citations from Scopus
from .modules import scopus_get_article_entity  # Gets article entity from Scopus
from .modules import fill_author_from_full_names  # Fills author names from full names

# ==========================
#  Common or shared
# ==========================

from .modules import fill_missing_affiliations, extract_countries  # Extracts country affiliations from authors
from .modules import standarize_journal_data  # Standardizes WoS journal data
from .modules import remove_duplicates_df # Removes duplicates between WoS and Scopus
from .modules import unify_author_fullname_and_orcid # Unifies author names and ORCID
from .modules import aggregate_sr_and_attach_scimago_ids # Aggregates SR and attaches Scimago IDs
from .modules import fill_missing_issn_eissn_with_scimago # Fills missing ISSN/EISSN with Scimago
from .modules import resolve_duplicate_sourceids # Resolves duplicate source IDs in WoS
from .modules import add_year_and_scimago_info # Adds year and Scimago info to scimagodb
from .modules import merge_all_entities  # Merge WoS + Scopus entities into consolidated All_* CSVs
from .modules import consolidate_authors  # Consolidate authors into person-level identities

# ============================
# utils
# ============================

import os
import time
import pandas as pd

def measure_time(func):
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        end = time.perf_counter()
        print(f"'{func.__name__}' executed in {end - start:.2f} seconds")
        return result
    return wrapper

@measure_time
def preprocessing_df(path_wos=None, path_scopus=None, path_scimago=None, path_country=None, API_KEY_OPENALEX=None):
    """
    Preprocessing the DataFrames of WoS and Scopus.
    """
    if path_wos:
        if isinstance(path_wos, str):
            path_wos = [path_wos]

        # Take the first file to obtain the base folder (they are all in the same folder)
        base_dir = os.path.dirname(path_wos[0])
        output_dir = os.path.join(base_dir, "WoS_results")
        wos_output_dir = output_dir

        # Create the folder if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        print("""
              ============================
                Reading WoS file...
              ============================
              """)
        
        # Dataframe	
        wos_df = wos_txt_to_df(path_wos)
        wos_df.to_csv(os.path.join(output_dir, "1_temp_wos_df.csv"), index=False)
        print("1. WoS DataFrame successfully created")
        
        # Enrich WoS main articles with OpenAlex authors (names in uppercase ASCII, replace ORCID)
        wos_df = enrich_wos_with_openalex_authors(
            wos_df,
            replace=True,
            keep_raw=False,
            uppercase_ascii=True,
            API_KEY_OPENALEX=API_KEY_OPENALEX
        )
        wos_df.to_csv(os.path.join(output_dir, "2-1_temp_wos_df_openalex_authors.csv"), index=False)
        # wos_df = pd.read_csv(os.path.join(output_dir, "2-1_temp_wos_df_openalex_authors.csv"))
        print("2. WoS authors enriched with OpenAlex (names/ORCID/IDs)")

        # Get references
        wos_references, wos_citation = get_wos_references(wos_df)
        wos_citation.to_csv(os.path.join(output_dir, "Citation.csv"), index=False)
        wos_references.to_csv(os.path.join(output_dir, "3_temp_wos_references.csv"), index=False)
        print("3. WoS references extracted")
        
        # Enrich references with Openalex
        wos_ref_enriched = enrich_references_with_openalex(wos_references, API_KEY_OPENALEX)
        wos_ref_enriched.to_csv(os.path.join(output_dir,'enrich_wos_ref.csv'))
        # wos_ref_enriched = pd.read_csv(os.path.join(output_dir,'enrich_wos_ref.csv'))
        print("4. WoS references enriched with OpenAlex")
      
        # Merge WoS and references
        wos_df_3 = merge_wos_ref(wos_df, wos_ref_enriched)
        wos_df_3.to_csv(os.path.join(output_dir,'5_temp_wos_df_merged.csv'), index=False)
        # wos_df_3 = pd.read_csv(os.path.join(output_dir,'5_temp_wos_df_merged.csv'))
        print("5. WoS and references DataFrame merged")
        
        
        ##############################################
        #           Scimago Dataframe
        ##############################################

        # Load Scimago reference data (correct relative path)
        scimago = pd.read_csv(path_scimago)

        wos_df_4 = standarize_journal_data(wos_df_3)
        wos_df_4.to_csv(os.path.join(output_dir,'5-1_temp_wos_df_standarized.csv'), index=False)
        # wos_df_4 = pd.read_csv(os.path.join(output_dir,'5-1_temp_wos_df_standarized.csv'))
        print("5.1. WoS journal data standardized")

        # fill missing issn and eissn with Scimago
        wos_df_5 = fill_missing_issn_eissn_with_scimago(wos_df_4, scimago)
        wos_df_5.to_csv(os.path.join(output_dir,'6_temp_wos_df_fixmissingjournalreferences.csv'), index=False)
        # wos_df_5 = pd.read_csv(os.path.join(output_dir,'6_temp_wos_df_fixmissingjournalreferences.csv'))
        print("6. Missing ISSN/EISSN filled with Scimago")

        wos_df_6 = aggregate_sr_and_attach_scimago_ids(wos_df_5, scimago)
        wos_df_6.to_csv(os.path.join(output_dir,'6_temp_wos_df_aggregated.csv'), index=False)
        # wos_df_6 = pd.read_csv(os.path.join(output_dir,'6_temp_wos_df_aggregated.csv'))
        print("6.1. SR aggregated and Scimago IDs attached")

        journal, scimago_raw = resolve_duplicate_sourceids(wos_df_6)
        journal.to_csv(os.path.join(output_dir,'Journal.csv'), index=False)
        scimago_raw.to_csv(os.path.join(output_dir,'6_temp_scimago_raw.csv'), index=False)
        # journal = pd.read_csv(os.path.join(output_dir,'Journal.csv'))
        # scimago_raw = pd.read_csv(os.path.join(output_dir,'6_temp_scimago_raw.csv'))
        print("6.2. Duplicate source IDs resolved in WoS")
        
        scimagodb = add_year_and_scimago_info(scimago_raw, wos_df_3, scimago)
        scimagodb.to_csv(os.path.join(output_dir,'scimagodb.csv'), index=False)
        # scimagodb = pd.read_csv(os.path.join(output_dir,'scimagodb.csv'))


        ##############################################
        #        Article and Author Dataframe
        ##############################################
        
        # Get author data
        wos_author_raw = get_wos_author_data(wos_df_3)
        wos_author_raw.to_csv(os.path.join(output_dir,'7_temp_wos_author_raw.csv'), index=False)
        # wos_author_raw = pd.read_csv(os.path.join(output_dir,'7_temp_wos_author_raw.csv'))
        print("7. WoS author raw data generated")
        
        # Enrich author data
        wos_author_enriched = enrich_wos_author_data(wos_author_raw)
        wos_author_enriched.to_csv(os.path.join(output_dir,'8_temp_wos_author_enriched.csv'), index=False)
        # wos_author_enriched = pd.read_csv(os.path.join(output_dir,'8_temp_wos_author_enriched.csv'))
        print("8. WoS author data enriched")
        
        # Merge WoS and author data
        wos_author, articleauthor_wos, wos_author_affiliation_no_country = unify_author_fullname_and_orcid(wos_author_enriched)
        print("9. WoS author, ArticleAuthor, and affiliation data generated")
        wos_author.to_csv(os.path.join(output_dir,'Author.csv'), index=False)
        articleauthor_wos.to_csv(os.path.join(output_dir,'ArticleAuthor.csv'), index=False)
        wos_author_affiliation_no_country.to_csv(os.path.join(output_dir,'9_temp_wos_author_affiliation.csv'), index=False)

        # Consolidate authors to person-level identities and export auxiliary mappings
        try:
            author_person, author_alias, author_conflicts = consolidate_authors(wos_author)
            author_person.to_csv(os.path.join(output_dir, 'AuthorPerson.csv'), index=False)
            author_alias.to_csv(os.path.join(output_dir, 'AuthorAlias.csv'), index=False)
            author_conflicts.to_csv(os.path.join(output_dir, 'AuthorConflicts.csv'), index=False)
            print("9.1. Author consolidation generated: AuthorPerson/AuthorAlias/AuthorConflicts")

            # Add PersonID back into Author and ArticleAuthor for stronger joins
            alias_map = author_alias[['AuthorID', 'PersonID']].drop_duplicates()
            wos_author_pid = wos_author.merge(alias_map, on='AuthorID', how='left')
            articleauthor_pid = articleauthor_wos.merge(alias_map, on='AuthorID', how='left')

            # Overwrite with PersonID-enriched versions
            wos_author_pid.to_csv(os.path.join(output_dir,'Author.csv'), index=False)
            articleauthor_pid.to_csv(os.path.join(output_dir,'ArticleAuthor.csv'), index=False)
            print("9.2. Added PersonID to Author and ArticleAuthor")
        except Exception as e:
            print(f"[WARN] Author consolidation failed: {e}")

        # Get country affiliation
        country_codes_file = path_country
        affiliation_0 = fill_missing_affiliations(wos_author_affiliation_no_country)
        affiliation = extract_countries(affiliation_0, path_country)
        print("10. Affiliation countries extracted")
        affiliation.to_csv(os.path.join(output_dir,'Affiliation.csv'), index=False)

        article = get_article_entity(wos_df_3)
        print("11. Article entity obtained")
        article.to_csv(os.path.join(output_dir,'Article.csv'), index=False)

    else:
        print("""
              ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
                    No WoS file has been entered
              ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
              """)
    
    
    if path_scopus:
        # Obtain the folder where the path_scopus file is located
        base_dir = os.path.dirname(path_scopus)
        output_dir = os.path.join(base_dir, "Scopus_results")
        scopus_output_dir = output_dir
        
        # Create the folder if it doesn't exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        print("""
              ===============================
                Reading Scopus file...
              ===============================
              """)
        
        # Load Scimago df
        scimago = pd.read_csv(path_scimago)
        
        # Dataframe
        scopus_df = scopus_csv_to_df(path_scopus, scimago)
        scopus_df.to_csv(os.path.join(output_dir,'1_temp_scopus_df.csv'), index=False)
        # scopus_df = pd.read_csv(os.path.join(output_dir,'1_temp_scopus_df.csv'))
        print("1. Scopus DataFrame created")

        # Enrich Scopus main articles with OpenAlex authors (names uppercase ASCII, ORCID, IDs)
        scopus_df = enrich_scopus_with_openalex_authors(
            scopus_df,
            replace=True,
            uppercase_ascii=True,
            keep_raw=False,
            API_KEY_OPENALEX=API_KEY_OPENALEX
        )
        scopus_df.to_csv(os.path.join(output_dir,'2-1_temp_scopus_df_openalex_authors.csv'), index=False)
        # scopus_df_2 = pd.read_csv(os.path.join(output_dir,'2-1_temp_scopus_df_openalex_authors.csv'))
        print("2. Scopus authors enriched with OpenAlex (names/ORCID/IDs)")

        
        # Extract references
        extraction_linksref_openalex = generate_references_column(scopus_df, API_KEY_OPENALEX)
        extraction_linksref_openalex.to_csv(os.path.join(output_dir,'3_temp_extraction_linksref_openalex.csv'), index=False)
        # extraction_linksref_openalex = pd.read_csv(os.path.join(output_dir,'3_temp_extraction_linksref_openalex.csv'))
        print("3. Scopus references extracted")

        # Enrich references with OpenAlex
        enrich_ref = openalex_enrich_ref(extraction_linksref_openalex, API_KEY_OPENALEX)
        enrich_ref.to_csv(os.path.join(output_dir,'4_temp_extraction_linksref_openalex_enriched.csv'), index=False)
        # enrich_ref = pd.read_csv(os.path.join(output_dir,'4_temp_extraction_linksref_openalex_enriched.csv'))
        enrich_ref = enrich_ref.rename(columns={'orcids': 'orcid'})
        print("4. Scopus references enriched with OpenAlex")



        df_enriched_1 = fill_source_title_from_scimago(enrich_ref, scimago)
        df_enriched_1.to_csv(os.path.join(output_dir,'5_temp_extraction_linksref_openalex_sourcetitle.csv'), index=False)
        # df_enriched_1 = pd.read_csv(os.path.join(output_dir,'5_temp_extraction_linksref_openalex_sourcetitle.csv'))
        print("5. 'source_title' column filled from Scimago")

        # Generate SR_ref
        df_with_sr = generate_SR_ref(df_enriched_1)
        df_with_sr.to_csv(os.path.join(output_dir,'6_scopus_ref_enriched.csv'), index=False)
        # df_with_sr = pd.read_csv(os.path.join(output_dir,'6_scopus_ref_enriched.csv'))
        print("6. 'SR_ref' column generated with format: FIRSTAUTHOR, YEAR, SOURCE_TITLE")

        # Generate Citation DataFrame
        scopus_citation = citation_scopus(df_with_sr)
        scopus_citation.to_csv(os.path.join(output_dir,'Citation.csv'), index=False)
        # scopus_citation = pd.read_csv(os.path.join(output_dir,'Citation.csv'))
        print("7. Scopus citation DataFrame generated")

        # Enrich references with journal abbreviation
        scopus_df_3 = merge_scopus_ref(scopus_df, df_with_sr)
        print("8. Scopus and references DataFrames merged")

        scopus_df_3 = fill_author_from_full_names(scopus_df_3)
        scopus_df_3.to_csv(os.path.join(output_dir,'7_temp_scopus_df_3.csv'), index=False)
        # scopus_df_3 = pd.read_csv(os.path.join(output_dir,'7_temp_scopus_df_3.csv'))
        print("8.1. Author names filled from full names")

        # Extract country from affiliations (similar to WoS)
        def _extract_country_from_aff(aff_str: str) -> str:
            if not isinstance(aff_str, str) or not aff_str.strip():
                return ''
            parts = [p.strip() for p in aff_str.split(';') if p.strip()]
            countries = []
            for p in parts:
                segs = [s.strip() for s in p.split(',') if s.strip()]
                if segs:
                    countries.append(segs[-1].upper())
            return '; '.join(countries)

        if 'affiliations' in scopus_df_3.columns:
            scopus_df_3['country'] = scopus_df_3['affiliations'].apply(_extract_country_from_aff)

        # Article Dataframe
        article = scopus_get_article_entity(scopus_df_3)
        article.to_csv(os.path.join(output_dir,'Article.csv'), index=False)
        # article = pd.read_csv(os.path.join(output_dir,'Article.csv'))
        print("9. Article entity obtained")

        ##############################################
        #           Scimago Dataframe
        ##############################################


        # Standarize journal data
        scopus_df_4 = standarize_journal_data(scopus_df_3)
        scopus_df_4.to_csv(os.path.join(output_dir,'8_temp_scopus_df_4.csv'), index=False)
        # scopus_df_4 = pd.read_csv(os.path.join(output_dir,'8_temp_scopus_df_4.csv'))
        print("10. Standardize Scopus journal data")

        # Fill missing ISSN with Scimago
        scopus_df_5 = fill_missing_issn_eissn_with_scimago(scopus_df_4, scimago)
        scopus_df_5.to_csv(os.path.join(output_dir,'9_temp_scopus_df_5.csv'), index=False)
        # scopus_df_5 = pd.read_csv(os.path.join(output_dir,'9_temp_scopus_df_5.csv'))
        print("11. Filled missing ISSN/EISSN with Scimago")

        # Adjust scopus_df_5
        scopus_df_5 = scopus_df_5.rename(columns={'issn':'eissn'})
        scopus_df_5['issn'] = pd.NA
        scopus_df_5['issn'] = scopus_df_5['issn'].astype(str).str.strip()
        scimago['Issn'] = scimago['Issn'].astype(str).str.strip()
        scopus_df_5.to_csv(os.path.join(output_dir,'10_temp_scopus_df_5_cleaned.csv'), index=False)


        # Add SR and attach Scimago IDs
        scopus_df_6 = aggregate_sr_and_attach_scimago_ids(scopus_df_5, scimago)
        scopus_df_6.to_csv(os.path.join(output_dir,'10_temp_scopus_df_6.csv'), index=False)
        print("12. SR added and Scimago IDs attached")

        # Resolve duplicate source IDs
        journal, scimago_raw = resolve_duplicate_sourceids(scopus_df_6)
        journal.to_csv(os.path.join(output_dir,'Journal.csv'), index=False)
        scimago_raw.to_csv(os.path.join(output_dir,'scimago_raw.csv'), index=False)
        print("13. Duplicate source IDs resolved in Scopus")
        
        scopus_df_3['year'] = pd.to_numeric(scopus_df_3['year'], errors='coerce')
        scopus_df_3['year'] = scopus_df_3['year'].dropna().astype(int).astype(str).reindex(scopus_df_3.index)

        scimago['year'] = pd.to_numeric(scimago['year'], errors='coerce')
        scimago['year'] = scimago['year'].dropna().astype(int).astype(str).reindex(scimago.index)

        # Enrich with Scimago
        scimagodb = add_year_and_scimago_info(scimago_raw, scopus_df_3, scimago)
        scimagodb.to_csv(os.path.join(output_dir,'scimagodb.csv'), index=False)
        print("14. Year and Scimago information added to scimagodb")

        ##############################################
        #        Author Dataframes
        ##############################################

        # Get author data
        scopus_author_raw = get_scopus_author_data(scopus_df_3)
        scopus_author_raw.to_csv(os.path.join(output_dir,'scopus_author_raw.csv'), index=False)
        print("15. 'scopus_author_raw' generated")

        # Enrich author data
        scopus_author_enriched = enrich_scopus_author_data(scopus_author_raw)
        scopus_author_enriched.to_csv(os.path.join(output_dir,'scopus_author_enriched.csv'), index=False)
        print("16. Enriched 'scopus_author_raw'")


        # Merge Scopus and author data
        scopus_author, articleauthor_scopus, scopus_author_affiliation_no_country = unify_author_fullname_and_orcid(scopus_author_enriched)
        scopus_author.to_csv(os.path.join(output_dir,'Author.csv'), index=False)
        articleauthor_scopus.to_csv(os.path.join(output_dir,'ArticleAuthor.csv'), index=False)
        print("17. Scopus_author, articleauthor_scopus, and scopus_author_affiliation generated")


        # Consolidate Scopus authors and propagate PersonID
        try:
            author_person, author_alias, author_conflicts = consolidate_authors(scopus_author)
            author_person.to_csv(os.path.join(output_dir, 'AuthorPerson.csv'), index=False)
            author_alias.to_csv(os.path.join(output_dir, 'AuthorAlias.csv'), index=False)
            author_conflicts.to_csv(os.path.join(output_dir, 'AuthorConflicts.csv'), index=False)
            alias_map = author_alias[['AuthorID','PersonID']].drop_duplicates()
            scopus_author_pid = scopus_author.merge(alias_map, on='AuthorID', how='left')
            articleauthor_scopus_pid = articleauthor_scopus.merge(alias_map, on='AuthorID', how='left')
            scopus_author_pid.to_csv(os.path.join(output_dir,'Author.csv'), index=False)
            articleauthor_scopus_pid.to_csv(os.path.join(output_dir,'ArticleAuthor.csv'), index=False)
            print("17.1. Consolidation of authors (Scopus) generated and PersonID propagated")
        except Exception as e:
            print(f"[WARN] Failed the consolidation of authors (Scopus): {e}")

        # Get country affiliation
        country_codes_file = path_country
        affiliation_0 = fill_missing_affiliations(scopus_author_affiliation_no_country)
        affiliation = extract_countries(affiliation_0, country_codes_file)
        affiliation.to_csv(os.path.join(output_dir,'Affiliation.csv'), index=False)
        print("18. Affiliation countries extracted")

        # Consolidated All_* outputs when both sources are available
        try:
            if path_wos:
                base_dir_w = os.path.dirname(path_wos[0]) if isinstance(path_wos, list) else os.path.dirname(path_wos)
                all_dir = os.path.join(base_dir_w, 'all_data_wos_scopus')
                merge_all_entities(wos_output_dir, scopus_output_dir, all_dir)
                print("20. All_* (WoS+Scopus) generated in 'all_data_wos_scopus'")
        except Exception as e:
            raise print(f"[WARN] Failed to merge WoS+Scopus: {e}")

    else:
        print("""
              ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
                No Scopus file has been entered
              ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
              """) 
 
    if path_wos and path_scopus:

        print("""
              =================================================
               Both WoS and Scopus files have been processed.
              =================================================
              """)

    
    return None

# preprocesing_df(r"path_wos" or [r"path_wos",r"path_wos_2"], r"path_scopus")
if __name__ == "__main__":
    # Example/manual run (disabled by default)
    # preprocessing_df(
    #     path_wos=[None],
    #     path_scopus=None,
    #     path_scimago=r"..\..\data\scimago.csv",
    #     path_country=r"..\..\data\country.csv",
    # )
    pass
