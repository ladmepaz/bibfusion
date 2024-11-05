import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import re
import requests
import requests_cache
import time

# Configure caching to optimize API calls (optional but recommended)
requests_cache.install_cache('crossref_cache', expire_after=86400)  # Cache expires after 1 day

# Configure logging to monitor the process
logging.basicConfig(
    filename='crossref_update.log',  # Log to a file
    filemode='a',  # Append mode
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

def get_crossref_data_updated(doi: str) -> dict:
    """
    Queries the CrossRef API to extract selected information from a DOI, with affiliations and ORCIDs formatted as per user requirements.

    Parameters:
    ----------
    doi : str
        The DOI of the work to query.

    Returns:
    -------
    dict
        A dictionary containing extracted information with the following fields:
        - 'AF' (Author): Uppercased string of authors in 'FAMILY, GIVEN' format separated by semicolons.
        - 'SO' (Journal): Uppercased journal name.
        - 'PY' (Publication Year): Formatted as 'YYYY'.
        - 'DI' (DOI): DOI as-is.
        - 'AB' (Abstract): Uppercased and cleaned abstract.
        - 'TI' (Title): Uppercased title of the work.
        - 'VL' (Volume): Volume number.
        - 'IS' (Issue): Issue number.
        - 'PG' (Page): Page or article number.
        - 'OI' (ORCID): Author ORCIDs formatted as 'LASTNAME, FIRSTNAME/ORCID; ...'
        - 'C1' (Affiliations): Author affiliations formatted as '[LASTNAME, FIRSTNAME] AFFILIATION1.; [LASTNAME, FIRSTNAME] AFFILIATION2.; ...'

        If an error occurs, returns a dictionary with an 'error' key.
    """
    url = f'https://api.crossref.org/works/{doi}'
    try:
        # Make the API request with a timeout to prevent hanging
        response = requests.get(url, timeout=10)

        # Check if the response status is OK (200)
        if response.status_code == 200:
            data = response.json()
            message = data.get('message', {})

            # -----------------------------
            # 1. Extract and Format Authors
            # -----------------------------
            authors = message.get('author', [])
            author_names = []
            orcid_formatted = []
            affiliations_formatted = []

            for author in authors:
                given = author.get('given', '').strip()
                family = author.get('family', '').strip()

                # Format author name for 'AF' field
                if given and family:
                    formatted_name = f"{family.upper()}, {given.upper()}"
                elif family:
                    formatted_name = f"{family.upper()}, NO GIVEN NAME"
                elif given:
                    formatted_name = f"NO FAMILY NAME, {given.upper()}"
                else:
                    formatted_name = "NO NAME"
                author_names.append(formatted_name)

                # Format ORCID for 'OI' field
                orcid_url = author.get('ORCID', '').strip()
                if orcid_url:
                    # Extract the ORCID ID from the URL
                    orcid_match = re.search(r'(\d{4}-\d{4}-\d{4}-\d{3}[0-9Xx])', orcid_url)
                    if orcid_match:
                        orcid_id = orcid_match.group(1).upper()
                        orcid_entry = f"{formatted_name}/{orcid_id}"
                    else:
                        orcid_entry = f"{formatted_name}/INVALID ORCID FORMAT"
                else:
                    orcid_entry = f"{formatted_name}/NO ORCID"
                orcid_formatted.append(orcid_entry)

                # Format Affiliations for 'C1' field
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
                    # No affiliations for this author
                    affil_entry = f"[{formatted_name}] NO AFFILIATION."
                    affiliations_formatted.append(affil_entry)

            # Compile 'AF' field
            af = '; '.join(author_names) if author_names else 'NO AUTHOR'

            # Compile 'OI' field
            oi = '; '.join(orcid_formatted) if orcid_formatted else 'NO ORCID'

            # Compile 'C1' field
            c1 = ' '.join(affiliations_formatted) if affiliations_formatted else 'NO AFFILIATION'

            # -------------------------
            # 2. Extract Publication Info
            # -------------------------
            # Journal Name
            journal_list = message.get('container-title', ['NO JOURNAL'])
            so = journal_list[0].upper() if journal_list else 'NO JOURNAL'

            # Publication Year
            issued = message.get('issued', {}).get('date-parts', [[None]])
            if issued and issued[0] and issued[0][0]:
                py = f"{issued[0][0]}"
            else:
                py = 'NO DATE'

            # DOI
            di = message.get('DOI', 'NO DOI')

            # Only proceed if 'DI' is valid
            if di == 'NO DOI':
                # Skip this entry as it doesn't have a valid DOI
                return {"error": f"DOI {doi} does not exist in CrossRef."}

            # Abstract
            abstract = message.get('abstract', 'NO ABSTRACT')
            if abstract != 'NO ABSTRACT':
                # Remove HTML tags and convert to uppercase
                abstract = re.sub(r'<[^>]+>', '', abstract).strip().upper()
            else:
                abstract = 'NO ABSTRACT'

            # Title
            title_list = message.get('title', ['NO TITLE'])
            ti = title_list[0].upper() if title_list else 'NO TITLE'

            # Volume, Issue, Page
            vl = message.get('volume', 'NO VOLUME')
            is_ = message.get('issue', 'NO ISSUE')
            pg = message.get('page', 'NO PAGE')

            # -------------------------
            # 3. Compile Extracted Data
            # -------------------------
            info = {
                'AF': af,                           # Author list
                'SO': so,                           # Journal name
                'PY': py,                           # Publication Year
                'DI': di,                           # DOI
                'AB': abstract,                     # Abstract
                'TI': ti,                           # Title
                'VL': vl,                           # Volume
                'IS': is_,                          # Issue
                'PG': pg,                           # Page or Article Number
                'OI': oi,                           # Author ORCIDs in specified format
                'C1': c1,                           # Formatted Affiliations
            }
            return info
    except requests.exceptions.RequestException as e:
        # Handle any request-related errors (e.g., connection issues, timeouts)
        return {"error": f"Request exception occurred: {e}"}
    except ValueError as e:
        # Handle JSON decoding errors or unexpected data formats
        return {"error": f"Value error: {e}"}
    except Exception as e:
        # Catch-all for any other exceptions
        return {"error": f"An unexpected error occurred: {e}"}

def update_wos_ref_with_crossref(wos_ref: pd.DataFrame, doi_column: str = 'DI') -> pd.DataFrame:
        """
        Updates the wos_ref dataframe with additional information from CrossRef based on unique DOIs in the specified DOI column.

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

        # Step 3: Filter out DOIs that are '-' and ensure uniqueness
        filtered_wos_ref = wos_ref[wos_ref[doi_column] != '-']
        unique_dois = filtered_wos_ref[doi_column].dropna().unique()
        logging.info(f"Found {len(unique_dois)} unique DOIs to process after filtering out '-'.")

        # Step 4: Fetch CrossRef Data for Each DOI
        crossref_data_list = []

        # Define the maximum number of threads to prevent overwhelming the API
        max_workers = 10

        # Optionally, implement a retry mechanism for transient failures
        def fetch_data_with_retry(doi, retries=3, backoff_factor=2):
            for attempt in range(retries):
                data = get_crossref_data_updated(doi)
                if 'error' not in data:
                    return data
                else:
                    logging.warning(f"Attempt {attempt+1}: {data['error']}")
                    time.sleep(backoff_factor ** attempt)  # Exponential backoff
            # After retries, return the last error
            return {"error": f"Failed to fetch DOI {doi} after {retries} attempts."}

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all DOI requests
            future_to_doi = {executor.submit(fetch_data_with_retry, doi): doi for doi in unique_dois}

            for future in as_completed(future_to_doi):
                doi = future_to_doi[future]
                try:
                    data = future.result()
                    if 'error' not in data:
                        crossref_data_list.append(data)
                        logging.info(f"Successfully fetched data for DOI: {doi}")
                    else:
                        logging.warning(f"Error fetching DOI {doi}: {data['error']}")
                except Exception as exc:
                    logging.error(f"Generated an exception for DOI {doi}: {exc}")

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
            wos_ref_updated = filtered_wos_ref.merge(
                crossref_df,
                on='DI',
                how='left',
                suffixes=('', '_crossref'),
                indicator=True
            )
            logging.info("Merged CrossRef data into wos_ref dataframe.")
        else:
            wos_ref_updated = filtered_wos_ref.copy()
            logging.info("No CrossRef data to merge. Returning filtered wos_ref dataframe.")

        # Step 7: Fill Missing 'PY' and 'J9' Values
        # Fill 'PY' from CrossRef if missing
        if 'PY_crossref' in wos_ref_updated.columns:
            wos_ref_updated['PY'] = wos_ref_updated['PY'].fillna(wos_ref_updated['PY_crossref'])
            logging.info("Filled missing 'PY' values from CrossRef data.")
        else:
            logging.warning("'PY_crossref' column not found in merged dataframe.")

        # Handle 'J9' - CrossRef does not provide 'J9', so we need a mapping from 'SO' to 'J9'
        # Replace with your actual mapping source
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
        if 'SO' in wos_ref_updated.columns:
            wos_ref_updated['J9_crossref'] = wos_ref_updated['SO'].map(journal_abbreviation_mapping)
            logging.info("Mapped 'SO' to 'J9_crossref' using journal_abbreviation_mapping.")
        else:
            wos_ref_updated['J9_crossref'] = None
            logging.warning("'SO' column not found in merged dataframe. 'J9_crossref' set to None.")

        # Fill 'J9' from 'J9_crossref' where 'J9' is missing
        if 'J9_crossref' in wos_ref_updated.columns:
            wos_ref_updated['J9'] = wos_ref_updated['J9'].fillna(wos_ref_updated['J9_crossref']).fillna('UNKNOWN')
            logging.info("Filled missing 'J9' values from CrossRef data (using 'SO' mapping).")
        else:
            logging.warning("'J9_crossref' column not found in merged dataframe.")

        # Step 8: Drop Temporary Columns from CrossRef Merge
        columns_to_drop = ['PY_crossref', 'J9_crossref', '_merge']
        existing_columns_to_drop = [col for col in columns_to_drop if col in wos_ref_updated.columns]
        wos_ref_updated.drop(columns=existing_columns_to_drop, inplace=True, errors='ignore')
        logging.info("Dropped temporary CrossRef columns from the dataframe.")

        return wos_ref_updated
