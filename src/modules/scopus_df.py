import pandas as pd
import bibtexparser
from collections import defaultdict
import re

"""
    Converts a BibTeX file into a structured pandas DataFrame.

    This function reads a BibTeX file, extracts its entries, and organizes them into a DataFrame with columns based on a predefined mapping. 
    It processes the file by converting all data to uppercase, handling multiple entries for fields like 'funding_details', and extracting relevant information 
    such as the affiliation country and universities. It also changes the author format from "AND" to ";" and constructs new columns such as 'SR_FULL' and 'SR' 
    that concatenate key bibliographic details.

    Parameters:
    ----------
    file_path : str
        Path to the BibTeX file to be read.

    Returns:
    -------
    pd.DataFrame
        A pandas DataFrame containing the entries from the BibTeX file, organized into columns defined by `column_mapping` and `column_order`.
        If an error occurs during reading or parsing, it returns None.
"""


def bib_to_df(file_path):
     
    column_mapping = {
        'author': 'AU',
        'author_keywords': 'DE',
        'keywords': 'ID',
        'affiliation': 'C1',
        'references': 'CR',
        'abbrev_source_title': 'JI',
        'abstract': 'AB',
        'art_number': 'AR',
        'chemicals_cas': 'CHEMICAL_CAS',
        'coden': 'coden',
        'correspondence_address1': 'RP',
        'document_type': 'DT',
        'doi': 'DI',
        'editor': 'BE',
        'funding_details': 'FU',
        'isbn': 'BN',
        'issn': 'SN',
        'journal': 'SO',
        'language': 'LA',
        'note': 'TC',
        'number': 'PN',
        'page_count': 'page_count',
        'pages': 'PP',
        'publisher': 'PU',
        'pubmed_id': 'PM',
        'source': 'DB',
        'sponsors': 'sponsors',
        'title': 'TI',
        'url': 'url',
        'volume': 'VL',
        'year': 'PY',
        'funding_text_1': 'FX',
        'abbrev_source_title': 'J9',
        '':'AU_UN_NR', # Not found in the file
        'ID': 'USERS_ID'
    }
    
    # Only to preview the columns in a more specific order
    column_order = [
    'USERS_ID', 'AU', 'DE', 'ID', 
    'C1', 'CR', 'JI', 'AB', 
    'AR', 'chemicals_cas', 'coden', 'RP', 
    'DT', 'DI', 'BE', 'FU', 
    'BN', 'SN', 'SO', 'LA', 
    'TC', 'PN', 'page_count', 'PP', 
    'PU', 'PM', 'DB', 'sponsors', 
    'TI', 'url', 'VL', 'PY', 
    'FX', 'J9','AU_UN', 'AU1_UN', 
    'AU_UN_NR', 'SR_FULL', 'SR', 'AU_CO'
    ]



    try:
        abbreviations_file = 'tests/files/country.csv'
        abbreviations_df = pd.read_csv(abbreviations_file, sep='; ', header=None, encoding='utf-8', engine='python')
        abbreviations_dict = dict(zip(abbreviations_df[0], abbreviations_df[1]))
        with open(file_path, 'r', encoding='utf-8') as bibfile:
            bibtex_str = bibfile.read()
        
        library = bibtexparser.loads(bibtex_str)

        entries_data = []

        for entry in library.entries:
            entry_data = {}
            for i in entry:
                entry_data[i] = entry.get(i, '').upper()
                
                if i == 'url':
                    entry_data[i] = entry.get(i, '')
                # Concatenate multiple 'funding_details' entries
                if i == 'funding_details':
                    current_funding = entry.get(i, '').strip()

                    if 'funding_details' in entry_data:
                        # Concatenate the new funding detail with the existing one
                        entry_data['funding_details'] = entry_data['funding_details'] + '; ' + current_funding
                    else:
                        entry_data['funding_details'] = current_funding
                # Change 'AND' to ';' in the author list
                elif i == 'author':
                    entry_data[i] = entry_data[i].replace(' AND ', ';')
                    first_author = entry_data[i].split(';')[0].strip()
                # Extract the affiliation country
                elif i == 'note':
                    note = entry_data[i]
                    match = re.search(r'CITED BY (\d+)', note)
                    entry_data[i] = match.group(1) if match else ''
                elif i == 'affiliation':
                    affiliation = entry_data[i]
                    
                    # Extract universities
                    university_pattern = r'([A-Z][A-Za-z\s]+(?:University|College|School|Institute|Academy|Faculty|Center|Department)[A-Za-z\s]*)'
                    universities = re.findall(university_pattern, affiliation, re.IGNORECASE)
                    universities = [univ.strip() for univ in universities]  # Clean up the university names
                    entry_data['AU_UN'] = '; '.join(universities).upper() 
                    
                    
                    universities = entry_data['AU_UN'].split(';')
                    
                    entry_data['AU1_UN'] = universities[0].strip() if universities else ''

                    # Regular expression to capture affiliation countries
                    match = r'\b([A-Z\s]+(?:[ -][A-Z\s]+)*)\b(?=\s*(?:;|$))'
                    countries = re.findall(match, affiliation)
                    formatted_countries = [abbreviations_dict.get(country, country) for country in countries]

                    entry_data['AU_CO'] = '; '.join(formatted_countries).upper()
            entry_data['SR_FULL'] = f"{first_author}; {entry_data['year']}; {entry_data['journal']}"
            entry_data['SR'] = f"{first_author}; {entry_data['year']}; {entry_data['abbrev_source_title']}"

            entries_data.append(entry_data)

        df = pd.DataFrame(entries_data)
        
        df.rename(columns=column_mapping, inplace=True)
        
        existing_columns = [col for col in column_order if col in df.columns]

        # Reorder only the existing columns
        df = df[existing_columns]

        df = df.reindex(columns=column_order)

        return df
    
    except FileNotFoundError:
        print(f"El archivo {file_path} o {abbreviations_file} no se encuentra.")
    except UnicodeDecodeError:
        print(f"Error de codificación al intentar leer el archivo {file_path} o {abbreviations_file}.")
    except KeyError as e:
        print(f"Clave no encontrada: {e}")
    except ValueError as e:
        print(f"Valor no válido: {e}")
    except TypeError as e:
        print(f"Error de tipo: {e}")

    return None