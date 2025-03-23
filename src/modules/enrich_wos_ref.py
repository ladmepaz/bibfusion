import pandas as pd
import requests
import re
import logging
from modules.get_crossref_data_updated import get_crossref_data_updated

# Configure logging to capture information and errors
logging.basicConfig(
    filename='crossref_update.log',  # Log to a file
    filemode='a',                     # Append mode
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

def enrich_wos_ref(doi: str) -> dict:
    """
    Queries the CrossRef API to extract selected information from a DOI.
    
    Parameters:
    ----------
    doi : str
        The DOI of the work to query.
    
    Returns:
    -------
    dict
        A dictionary containing extracted information or an error message.
    """
    url = f'https://api.crossref.org/works/{doi}'
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            message = data.get('message', {})
            
            # Extract authors
            authors = message.get('author', [])
            author_names = []
            orcid_formatted = []
            affiliations_formatted = []
            
            for author in authors:
                given = author.get('given', '').strip()
                family = author.get('family', '').strip()
                
                # Format author name
                if given and family:
                    formatted_name = f"{family.upper()}, {given.upper()}"
                elif family:
                    formatted_name = f"{family.upper()}, NO GIVEN NAME"
                elif given:
                    formatted_name = f"NO FAMILY NAME, {given.upper()}"
                else:
                    formatted_name = "NO NAME"
                author_names.append(formatted_name)
                
                # Format ORCID
                orcid_url = author.get('ORCID', '').strip()
                if orcid_url:
                    orcid_match = re.search(r'(\d{4}-\d{4}-\d{4}-\d{3}[0-9Xx])', orcid_url)
                    if orcid_match:
                        orcid_id = orcid_match.group(1).upper()
                        orcid_entry = f"{formatted_name}/{orcid_id}"
                    else:
                        orcid_entry = f"{formatted_name}/INVALID ORCID FORMAT"
                else:
                    orcid_entry = f"{formatted_name}/NO ORCID"
                orcid_formatted.append(orcid_entry)
                
                # Format Affiliations
                affiliations = author.get('affiliation', [])
                if affiliations:
                    for aff in affiliations:
                        aff_name = aff.get('name', '').strip().upper()
                        if aff_name:
                            affil_entry = f"[{formatted_name}] {aff_name}."
                        else:
                            affil_entry = f"[{formatted_name}] NO AFFILIATION."
                        affiliations_formatted.append(affil_entry)
                else:
                    affil_entry = f"[{formatted_name}] NO AFFILIATION."
                    affiliations_formatted.append(affil_entry)
            
            # Compile fields
            af = '; '.join(author_names) if author_names else 'NO AUTHOR'
            oi = '; '.join(orcid_formatted) if orcid_formatted else 'NO ORCID'
            c1 = ' '.join(affiliations_formatted) if affiliations_formatted else 'NO AFFILIATION'
            
            # Extract publication info
            so = message.get('container-title', ['NO JOURNAL'])[0].upper()
            issued = message.get('issued', {}).get('date-parts', [[None]])
            py = f"{issued[0][0]}" if issued and issued[0] and issued[0][0] else 'NO DATE'
            di = message.get('DOI', 'NO DOI')
            if di == 'NO DOI':
                return {"error": f"DOI {doi} does not exist in CrossRef."}
            abstract = message.get('abstract', 'NO ABSTRACT')
            if abstract != 'NO ABSTRACT':
                abstract = re.sub(r'<[^>]+>', '', abstract).strip().upper()
            ti = message.get('title', ['NO TITLE'])[0].upper()
            vl = message.get('volume', 'NO VOLUME')
            is_ = message.get('issue', 'NO ISSUE')
            pg = message.get('page', 'NO PAGE')
            
            # Compile extracted data
            info = {
                'AF': af,
                'SO': so,
                'PY': py,
                'DI': di,
                'AB': abstract,
                'TI': ti,
                'VL': vl,
                'IS': is_,
                'PG': pg,
                'OI': oi,
                'C1': c1,
            }
            return info
        else:
            return {"error": f"Error {response.status_code}: Unable to fetch DOI {doi}."}
    except requests.exceptions.RequestException as e:
        return {"error": f"Request exception occurred: {e}"}
    except ValueError as e:
        return {"error": f"Value error: {e}"}
    except Exception as e:
        return {"error": f"An unexpected error occurred: {e}"}

def update_wos_ref_with_crossref(wos_ref: pd.DataFrame, doi_column: str = 'doi') -> pd.DataFrame:
    """
    Updates the wos_ref dataframe with additional information from CrossRef based on unique DOIs.

    Parameters:
    ----------
    wos_ref : pd.DataFrame
        The original dataframe containing at least the DOI column and potentially 'PY' and 'J9'.
    doi_column : str, optional
        The name of the column containing DOIs. Default is 'DI'.

    Returns:
    -------
    pd.DataFrame
        The updated dataframe with new columns from CrossRef and filled 'PY' and 'J9' where applicable.
    """
    # Step 1: Verify DOI Column Exists
    if doi_column not in wos_ref.columns:
        raise KeyError(f"The specified DOI column '{doi_column}' does not exist in the dataframe.")
    else:
        logging.info(f"Using '{doi_column}' as the DOI column for merging.")
    
    # Step 2: Ensure 'DI' is of string type and strip whitespace
    wos_ref[doi_column] = wos_ref[doi_column].astype(str).str.lower().str.strip()
    
    # Step 3: Ensure uniqueness of DOIs
    unique_dois = wos_ref[doi_column].dropna().unique()
    logging.info(f"Found {len(unique_dois)} unique DOIs to process.")
    
    # Filter out DOIs that are '-' or empty strings
    valid_dois = [doi for doi in unique_dois if doi != '-' and doi != '']
    logging.info(f"Found {len(valid_dois)} valid DOIs after excluding '-' and empty strings.")
    
    # Step 4: Fetch CrossRef Data for Each Valid DOI
    crossref_data_list = []
    for doi in valid_dois:
        data = get_crossref_data_updated(doi)
        if 'error' not in data:
            crossref_data_list.append(data)
            logging.info(f"Successfully fetched data for DOI: {doi}")
        else:
            logging.warning(f"Error fetching DOI {doi}: {data['error']}")
    
    # Step 5: Convert Retrieved Data into a DataFrame
    if crossref_data_list:
        crossref_df = pd.DataFrame(crossref_data_list)
        logging.info(f"CrossRef data collected for {len(crossref_df)} DOIs.")
    else:
        crossref_df = pd.DataFrame()
        logging.warning("No CrossRef data was collected. Proceeding without merging.")
    
    # Step 6: Merge CrossRef Data Back into wos_ref
    if not crossref_df.empty:
        # Ensure 'DI' is the key for merging
        wos_ref_updated = wos_ref.merge(
            crossref_df,
            on='doi',
            how='left',
            suffixes=('', '_crossref')
        )
        logging.info("Merged CrossRef data into wos_ref dataframe.")
    else:
        wos_ref_updated = wos_ref.copy()
        logging.info("No CrossRef data to merge. Returning original wos_ref dataframe.")
    
    # Step 7: Fill Missing 'PY' and 'J9' Values
    # Fill 'PY' from CrossRef if missing
    if 'PY_crossref' in wos_ref_updated.columns:
        wos_ref_updated['PY'] = wos_ref_updated['PY'].fillna(wos_ref_updated['PY_crossref'])
        logging.info("Filled missing 'PY' values from CrossRef data.")
    else:
        logging.warning("'PY_crossref' column not found in merged dataframe.")
    
    # Handle 'J9' - CrossRef does not provide 'J9', so we need a mapping from 'SO' to 'J9'
    journal_abbreviation_mapping = {
        'MOLECULES': 'MOLECULES',
        'BUSINESS RESEARCH': 'BUS RES',
        'J MARKET THEORY PRACTICE': 'J MKT THEOR PRAC',
        'J STRATEG MARK': 'J STRATEG MARK',
        'JOURNAL OF STRATEGIC MARKETING': 'J STRATEG MARK',
        'JOURNAL OF MARKET THEORY PRACTICE': 'J MKT THEOR PRAC',
        # Add more mappings as needed
    }
    
    # Create a new column 'J9_crossref' by mapping 'SO' using the dictionary
    if 'journal' in wos_ref_updated.columns:
        wos_ref_updated['J9_crossref'] = wos_ref_updated['journal'].map(journal_abbreviation_mapping)
        logging.info("Mapped 'SO' to 'J9_crossref' using journal_abbreviation_mapping.")
    else:
        wos_ref_updated['J9_crossref'] = None
        logging.warning("'SO' column not found in merged dataframe. 'J9_crossref' set to None.")
    
    # Fill 'J9' from 'J9_crossref' where 'J9' is missing
    if 'J9_crossref' in wos_ref_updated.columns:
        wos_ref_updated['source_title'] = wos_ref_updated['source_title'].fillna(wos_ref_updated['J9_crossref']).fillna('UNKNOWN')
        logging.info("Filled missing 'J9' values from CrossRef data (using 'SO' mapping).")
    else:
        logging.warning("'J9_crossref' column not found in merged dataframe.")
    
    # Step 8: Drop Temporary Columns from CrossRef Merge
    columns_to_drop = ['PY_crossref', 'J9_crossref']
    existing_columns_to_drop = [col for col in columns_to_drop if col in wos_ref_updated.columns]
    wos_ref_updated.drop(columns=existing_columns_to_drop, inplace=True, errors='ignore')
    logging.info("Dropped temporary CrossRef columns from the dataframe.")
    
    return wos_ref_updated
