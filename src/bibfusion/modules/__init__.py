"""
This is the main module of the Preprocessing application.  

It is a package designed to manage functions for processing bibliographic data from WoS and Scopus.
"""

__version__ = "0.0.2"

# ==========================
#  Web of Science (WoS)
# ==========================

from .wos_txt_to_df import wos_txt_to_df  # Converts WoS .txt files to DataFrame
from .get_wos_references import get_wos_references  # Extracts bibliographic references from WoS
from .merge_wos_ref import merge_wos_ref  # Merges WoS references (multiple sources or duplicates)
from .get_wos_author_data import get_wos_author_data  # Gets author data from WoS
from .enrich_wos_author_data import enrich_wos_author_data  # Enriches WoS author data
from .standarize_journal_data import standarize_journal_data  # Standardizes WoS journal data
from .get_country_affiliation import (
    fill_missing_affiliations,
    extract_countries
)  # Extracts country affiliations from authors
from .get_article_entity import get_article_entity  # Gets article entity from WoS

# ==========================
#  Scopus
# ==========================

from .scopus_csv_to_df import scopus_csv_to_df  # Converts Scopus .csv files to DataFrame
from .merge_scopus_ref import merge_scopus_ref  # Merges Scopus references
from .get_scopus_author_data import get_scopus_author_data  # Gets author data from Scopus
from .enrich_scopus_author_data import enrich_scopus_author_data  # Enriches author data
from .enrich_scopus_with_openalex_authors import enrich_scopus_with_openalex_authors  # Enrich Scopus authors via OpenAlex
from .get_openalex_data import (
    generate_references_column,
    generate_SR_ref,
    openalex_enrich_ref,
    fill_source_title_from_scimago
)
from .citation_scopus import citation_scopus  # Extracts citations from Scopus
from .scopus_get_article_entity import scopus_get_article_entity  # Gets article entity from Scopus
from .fill_author_from_full_names import fill_author_from_full_names  # Fills author names from full names
from .extract_orcids import extract_orcids  # Extracts ORCID IDs from Scopus data

# ==========================
#  Common or shared
# ==========================

from .duplicates import remove_duplicates_df  # Removes duplicates between WoS and Scopus
from .enrich_references_with_openalex import enrich_references_with_openalex  # General reference enrichment with OpenAlex
from .unify_author_fullname_and_orcid import unify_author_fullname_and_orcid  # Unifies author names and ORCID
from .enrich_wos_with_openalex_authors import enrich_wos_with_openalex_authors  # Enrich WoS authors via OpenAlex by DOI
from .consolidate_authors import consolidate_authors, consolidate_authors_from_csv  # Post-process consolidation
from .aggregate_sr_and_attach_scimago_ids import aggregate_sr_and_attach_scimago_ids  # Aggregates SR and attaches Scimago IDs
from .fill_missing_issn_eissn_with_scimago import fill_missing_issn_eissn_with_scimago  # Fills missing ISSN/EISSN with Scimago
from .resolve_duplicate_sourceids import resolve_duplicate_sourceids  # Resolves duplicate source IDs
from .add_year_and_scimago_info import add_year_and_scimago_info  # Adds year and Scimago info to scimagodb
from .to_xlsx import export_csvs_as_excel  # Combines CSV files into an Excel file
from .build_user_dataset import build_user_dataset_from_all  # Build UserDataset.xlsx from All_*.csv (phase 1)
from .cross_consolidate_all_authors import cross_consolidate_all_authors  # Cross-source author identity consolidation
from .build_coauthor_edges import build_coauthor_edges_links  # Build simple coauthor edges (from,to,year,SR,openalex_work_id)
from .build_coauthor_network import build_coauthor_network_for_gephi  # Build aggregated coauthor network (edges/nodes) for Gephi

# ==========================
#  Tree of Science
# ==========================
from .get_citation_network import get_citation_network  # Gets the citation network (root, trunk, branches)
from .clean_citation_network import clean_citation_network  # Cleans the citation network
from .add_community_branch import add_community_branch  # Adds a community branch to the citation network
from .get_tos import get_tos
from .get_tos_df import get_tos_df
from .graph_to_df import graph_to_df  # Converts the citation network to a DataFrame
from .merge_tos_with_articles import merge_tos_with_articles  # Merges ToS with articles
from .sort_by_tos_and_year import sort_by_tos_and_year  # Sorts by ToS and year
from .add_tos_to_data_excel import add_tos_to_data_excel  # Adds ToS data to Excel

# ============================
# DEFINING EXPORTS WITH __all__
# ============================
__all__ = [
    # ==========================
    #  Web of Science (WoS)
    # ==========================
    "wos_txt_to_df",
    "get_wos_references",
    "merge_wos_ref",
    "get_wos_author_data",
    "enrich_wos_author_data",
    "standarize_journal_data",
    "fill_missing_affiliations",
    "extract_countries",
    "get_article_entity",

    # ==========================
    #  Scopus
    # ==========================
    "scopus_csv_to_df",
    "merge_scopus_ref",
    "get_scopus_author_data",
    "enrich_scopus_author_data",
    "generate_references_column",
    "generate_SR_ref",
    "openalex_enrich_ref",
    "fill_source_title_from_scimago",
    "citation_scopus",
    "scopus_get_article_entity",
    "fill_author_from_full_names",

    # ==========================
    #  Common or shared
    # ==========================
    "remove_duplicates_df",
    "enrich_references_with_openalex",
    "unify_author_fullname_and_orcid",
    "enrich_wos_with_openalex_authors",
    "consolidate_authors",
    "consolidate_authors_from_csv",
    "aggregate_sr_and_attach_scimago_ids",
    "fill_missing_issn_eissn_with_scimago",
    "resolve_duplicate_sourceids",
    "add_year_and_scimago_info",
    "export_csvs_as_excel",
    "build_user_dataset_from_all",
    "cross_consolidate_all_authors",
    "build_coauthor_edges_links",
    "build_coauthor_network_for_gephi",
]

from .merge_sources import (normalize_doi as normalize_cross_doi, merge_articles, merge_authors, merge_from_outputs)


from .merge_sources import merge_all_entities
