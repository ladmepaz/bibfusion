import requests
import re

def get_crossref_data_updated(doi: str) -> dict:
    """
    Queries the CrossRef API to extract selected information from a DOI.
    
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
        - 'abstract': Uppercased and cleaned abstract.
        - 'TI' (Title): Uppercased title of the work.
        - 'volume': Volume number.
        - 'issue': Issue number.
        - 'page': Page or article number.
        - 'journal_issue_number': Specific journal issue number.
        - 'ORCID': Author ORCID identifiers.
        - 'affiliations': Author affiliations concatenated with semicolons.
    
        If an error occurs, returns a dictionary with an 'error' key.
    """
    url = f'https://api.crossref.org/works/{doi}'
    try:
        # Make the query with a timeout to prevent hanging
        response = requests.get(url, timeout=10)

        # Check if the response status is OK
        if response.status_code == 200:
            data = response.json()
            message = data.get('message', {})

            # Extract and format authors
            authors = message.get('author', [])
            author_names = []
            author_orcids = []
            author_affiliations = []
            for a in authors:
                given = a.get('given', '').strip()
                family = a.get('family', '').strip()
                if given and family:
                    # Format as 'FAMILY, GIVEN'
                    author_names.append(f"{family}, {given}")
                elif family:
                    author_names.append(family)
                elif given:
                    author_names.append(given)
                
                # Extract ORCID
                orcid = a.get('ORCID', 'NO ORCID').upper()
                author_orcids.append(orcid)
                
                # Extract affiliations
                affiliations = a.get('affiliation', [])
                affil_names = [aff.get('name', '').strip() for aff in affiliations]
                affil_str = '; '.join(affil_names) if affil_names else 'NO AFFILIATION'
                author_affiliations.append(affil_str)
            
            # Join authors with semicolons without spaces and convert to uppercase
            af = ';'.join(author_names).upper() if author_names else 'NO AUTHOR'
            # Join ORCIDs with semicolons
            orcids = '; '.join(author_orcids) if author_orcids else 'NO ORCID'
            # Join affiliations with semicolons
            affiliations_str = '; '.join(author_affiliations) if author_affiliations else 'NO AFFILIATION'

            # Extract and format journal name
            journal_list = message.get('container-title', ['NO JOURNAL'])
            so = journal_list[0].upper() if journal_list else 'NO JOURNAL'

            # Extract and format publication year
            issued = message.get('issued', {}).get('date-parts', [[None]])
            if issued and issued[0] and issued[0][0]:
                year = issued[0][0]
                py = f"{year}"
            else:
                py = 'NO DATE'

            # Extract DOI
            di = message.get('DOI', 'NO DOI')

            # Extract and clean abstract
            abstract = message.get('abstract', 'NO ABSTRACT')
            if abstract != 'NO ABSTRACT':
                # Remove HTML tags and convert to uppercase
                abstract = re.sub(r'<[^>]+>', '', abstract).strip().upper()
            else:
                abstract = 'NO ABSTRACT'

            # Extract and format title (TI)
            title_list = message.get('title', ['NO TITLE'])
            ti = title_list[0].upper() if title_list else 'NO TITLE'

            # Extract additional variables
            volume = message.get('volume', 'NO VOLUME')
            issue = message.get('issue', 'NO ISSUE')
            page = message.get('page', 'NO PAGE')
            
            # Extract journal_issue_number from 'journal-issue'
            journal_issue = message.get('journal-issue', {})
            journal_issue_number = journal_issue.get('issue', 'NO ISSUE NUMBER')

            # Extract ORCIDs and affiliations are already processed above

            # Construct the info dictionary with the required fields
            info = {
                'AF': af,                           # Author
                'SO': so,                           # Journal
                'PY': py,                           # Publication Year
                'DI': di,                           # DOI
                'abstract': abstract,               # Abstract
                'TI': ti,                           # Title
                'volume': volume,                   # Volume
                'issue': issue,                     # Issue
                'page': page,                       # Page or Article Number
                'journal_issue_number': journal_issue_number, # Journal Issue Number
                'ORCID': orcids,                    # Author ORCIDs
                'affiliations': affiliations_str,    # Author Affiliations
            }
            return info
        else:
            # Handle non-200 HTTP responses
            return {"error": f"Error {response.status_code}: Troubleshooting detected."}

    except requests.exceptions.RequestException as e:
        # Handle any request-related errors (e.g., connection issues, timeouts)
        return {"error": f"Request exception occurred: {e}"}
    except ValueError as e:
        # Handle JSON decoding errors or unexpected data formats
        return {"error": f"Value error: {e}"}
    except Exception as e:
        # Catch-all for any other exceptions
        return {"error": f"An unexpected error occurred: {e}"}
