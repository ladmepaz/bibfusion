"""
This is the main module of the Preprocessing application.  

It is a package designed to manage functions for processing bibliographic data from different sources.
"""

__version__ = "0.0.1"

# ============================
# MODULES FOR CITATION AND REFERENCE PROCESSING
# ============================
from modules.get_scopus_references import *
from modules.get_wos_references import *
from modules.get_wos_author_data import *
from modules.scopus_ref import scopus_refs
from modules.merge_scopus_ref import merge_scopus_ref
from modules.merge_wos_ref import merge_wos_ref
from modules.enrich_wos_ref import *

# ============================
# MODULES FOR DATA CONVERSION AND CLEANING
# ============================
from modules.scopus_bib_to_df import scopus_bib_to_df
from modules.scopus_csv_to_df import scopus_csv_to_df
from modules.scopus_df import bib_to_df
from modules.wos_df import wos_df
from modules.wos_txt_to_df import wos_txt_to_df
from modules.duplicates import remove_duplicates_df

# ============================
# MODULES FOR CITATION NETWORK ANALYSIS
# ============================
from modules.get_citation_network import get_citation_network
from modules.clean_citation_network import clean_citation_network

# ============================
# MODULES FOR DATA ENRICHMENT AND QUERIES
# ============================
from modules.crossref import doi_crossref
from modules.get_crossref_data import *
from modules.scopus_get_author_country import scopus_get_author_country

# ============================
# AUXILIARY MODULES
# ============================
from modules.get_tos_df import get_tos_df
from modules.get_tos import get_tos
from modules.add_community_branch import add_community_branch

# ============================
# DEFINING EXPORTS WITH __all__
# ============================
__all__ = [
    # Data conversion and processing
    "scopus_csv_to_df",
    "wos_txt_to_df",
    "scopus_bib_to_df",
    "bib_to_df",
    "wos_df",
    "remove_duplicates_df",
    
    # Citation networks
    "get_citation_network",
    "clean_citation_network",
    
    # References and duplicates
    "scopus_refs",
    "merge_scopus_ref",
    "merge_wos_ref",
    "remove_duplicates",
    
    # External data queries
    "doi_crossref",
    "get_crossref_data",
    "scopus_get_author_country",
    
    # ToS and community
    "get_tos_df",
    "get_tos",
    "add_community_branch"
]
