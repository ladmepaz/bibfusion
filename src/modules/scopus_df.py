import pandas as pd
import bibtexparser
import re

"""
    Converts a BibTeX file into a pandas DataFrame.

    This function reads a BibTeX file, extracts its entries, and converts
    them into a DataFrame, organizing the columns according to a predefined mapping.
    It extracts the affiliation country from the entries whenever available and
    changes the authors' format from "AND" to ";".

    Parameters:
    ----------
    file_path : str
        Path to the BibTeX file to be read.

    Returns:
    -------
    pd.DataFrame
        A pandas DataFrame containing the entries from the BibTeX file.
        If an error occurs during reading or parsing, it returns None.
    
    Exceptions:
    -----------
    - FileNotFoundError: If the file is not found at the specified path.
    - UnicodeDecodeError: If there is an encoding error while attempting to read the file.
    - bibtexparser.BibTexParserError: If an error occurs while parsing the BibTeX file.
    - KeyError: If trying to access a non-existent key in a dictionary.
    - ValueError: If an invalid value is encountered.
    - TypeError: If an argument of an inappropriate type is passed.

"""
def bib_to_df(file_path):
     
    column_mapping = {
        'source': 'SRC',
        'document_type': 'DT',
        'abbrev_source_title': 'ABR',
        'language': 'LA',
        'issn': 'SN',
        'correspondence_address1': 'C1',
        'references': 'CR',
        'author_keywords': 'DE',
        'abstract': 'AB',
        'affiliation': 'AFF',
        'url': 'URL',
        'note': 'NOTE',
        'doi': 'DOI',
        'pages': 'PAGES',
        'number': 'NUM',
        'volume': 'VL',
        'year': 'PR',
        'journal': 'JNL',
        'title': 'TI',
        'author': 'AU',
        'ENTRYTYPE': 'ENTRY_TYPE',
        'ID': 'USERS',
        'publisher': 'PU',
        'funding_text_1': 'FU',
        'funding_details': 'F_DETAILS',
        'keywords': 'ID',
        'art_number': 'ART NUMBER',
        'isbn': 'BN',
        'coden': 'CODEN',
        'editor': 'PU_EDITOR',
        'pubmed_id': 'PMID',
        'sponsors': 'SP',
        'page_count': 'PG',
        'chemicals_cas': 'CHEMICAL_CAS'
    }
    
    # Only to preview the columns in a more specific order
    column_order = [
    'AU', 'DE', 'ID', 'C1',
    'CR', 'PG', 'BP', 'EP','AB', 'SN',
    'TI', 'ART NUMBER', 'SP', 'CODEN',
    'PU', 'FU', 'DT', 'ENTRY_TYPE', 'PMID',
    'CHEMICAL_CAS', 'USERS', 'NUM', 'BN',
    'VL', 'DOI', 'LA', 'URL',
    'PR', 'JNL', 'ABR', 'AFF',
    'COUNTRY_AFILIATION', 'NOTE', 'CHEMICAL_CAS', 
    'SRC', 'F_DETAILS'
    ]


    try:
        with open(file_path, 'r', encoding='utf-8') as bibfile:
            bibtex_str = bibfile.read()
        
        library = bibtexparser.loads(bibtex_str)

        entries_data = []

        for entry in library.entries:
            entry_data = {}
            for i in entry:
                entry_data[i] = entry.get(i, '').upper()
                for i in list(entry_data):
                    # Change 'AND' to ';' in the author list
                    if i == 'author':
                        entry_data[i] = entry_data[i].replace(' AND ', ';')
                    # Extract the affiliation country
                    elif i == 'affiliation':
                        affiliation = entry_data[i]
                        match = re.search(r',\s*([A-Z ]+)$', affiliation)
                        entry_data['COUNTRY_AFILIATION'] = match.group(1) if match else ''
            entries_data.append(entry_data)


        df = pd.DataFrame(entries_data)
        
        df.rename(columns=column_mapping, inplace=True)
        
        df[['BP', 'EP']] = df['PAGES'].str.split('-', expand=True)
        df.drop(columns=['PAGES'], inplace=True)
        df = df[column_order]

        return df
    
    except FileNotFoundError:
        print(f"El archivo {file_path} no se encuentra.")
    except UnicodeDecodeError:
        print(f"Error de codificación al intentar leer el archivo {file_path}.")
    except KeyError as e:
        print(f"Clave no encontrada: {e}")
    except ValueError as e:
        print(f"Valor no válido: {e}")
    except TypeError as e:
        print(f"Error de tipo: {e}")

    return None