import pandas as pd
import bibtexparser
import re
import unicodedata

def scopus_bib_to_df(file_path):
    """
    Converts a Scopus BibTeX file into a structured pandas DataFrame.

    This function reads a BibTeX file, sanitizes field names to handle non-standard characters
    or spaces (e.g., 'funding_text 1'), and organizes the entries into a DataFrame.
    It processes the file by converting all data to uppercase, handling multiple entries
    for fields like 'funding_details', and extracting relevant information such as the affiliation universities.
    It also changes the author format from "Last, F." to "LAST F" and constructs new columns
    such as 'SR_FULL' and 'SR' that concatenate key bibliographic details.

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
    try:
        # Define the mapping from BibTeX fields to desired DataFrame columns
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
            'coden': 'CODEN',
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
            'page_count': 'PAGE_COUNT',
            'pages': 'PP',
            'publisher': 'PU',
            'pubmed_id': 'PM',
            'source': 'DB',
            'sponsors': 'SP',
            'title': 'TI',
            'url': 'UR',
            'volume': 'VL',
            'year': 'PY',
            'funding_text_1': 'FX',
            'id': 'USERS_ID',
            # Map 'au_un' and 'au1_un' if needed
            'au_un': 'AU_UN',
            'au1_un': 'AU1_UN',
            # Add any additional mappings as needed
        }

        # Define the desired column order
        column_order = [
            'USERS_ID', 'AU', 'DE', 'ID', 'C1', 'CR', 'JI', 'J9', 'AB',
            'AR', 'CHEMICAL_CAS', 'CODEN', 'RP', 'DT', 'DI', 'BE', 'FU',
            'BN', 'SN', 'SO', 'LA', 'TC', 'PN', 'PAGE_COUNT', 'PP',
            'PU', 'PM', 'DB', 'SP', 'TI', 'UR', 'VL', 'PY',
            'FX', 'AU_UN', 'AU1_UN',
            'SR_FULL', 'SR',
            # Additional columns can be added here
        ]

        # Read the BibTeX file
        with open(file_path, 'r', encoding='utf-8') as bibfile:
            bibtex_str = bibfile.read()

        # Preprocess the BibTeX string to sanitize field names
        def sanitize_field_names(bibtex_str):
            sanitized_lines = []
            for line in bibtex_str.splitlines():
                # Skip comment lines
                if line.strip().startswith('%'):
                    sanitized_lines.append(line)
                    continue
                # Find lines that look like field assignments
                if '=' in line:
                    parts = line.split('=', 1)
                    field_name = parts[0].strip()
                    field_value = parts[1]
                    # Normalize the field name to remove non-standard characters
                    field_name = unicodedata.normalize('NFKD', field_name)
                    field_name = field_name.encode('ascii', 'ignore').decode('ascii')
                    field_name = re.sub(r'\s+', '_', field_name)  # Replace whitespace with underscores
                    field_name = re.sub(r'[^a-zA-Z0-9_]', '', field_name)  # Remove any remaining non-alphanumeric characters
                    field_name = field_name.lower()
                    sanitized_line = f'{field_name} = {field_value}'
                    sanitized_lines.append(sanitized_line)
                else:
                    sanitized_lines.append(line)
            return '\n'.join(sanitized_lines)

        bibtex_str = sanitize_field_names(bibtex_str)

        # Parse the sanitized BibTeX entries
        bib_database = bibtexparser.loads(bibtex_str)
        entries = bib_database.entries

        entries_data = []

        for entry in entries:
            entry_data = {}
            try:
                for key in entry:
                    value = entry.get(key, '')
                    key_lower = key.lower()
                    # Keep the 'url' field as is
                    if key_lower == 'url':
                        entry_data[key_lower] = value
                    else:
                        entry_data[key_lower] = value.upper()

                    # Process 'author' field
                    if key_lower == 'author':
                        authors_raw = value.replace('\n', ' ')
                        # Split authors by ' and '
                        authors_list = re.split(r'\s+and\s+', authors_raw, flags=re.IGNORECASE)
                        processed_authors = []
                        for author in authors_list:
                            author = author.strip()
                            if ',' in author:
                                # Format: 'Last, F.'
                                last, first = [part.strip() for part in author.split(',', 1)]
                                # Remove periods from initials
                                first_initial = ''.join([name_part[0] for name_part in first.replace('.', '').split() if name_part])
                            else:
                                # Format: 'First Last' or single name
                                parts = author.split()
                                if len(parts) >= 2:
                                    first_initial = parts[0][0]
                                    last = ' '.join(parts[1:])
                                else:
                                    # Single name, take as last name
                                    first_initial = ''
                                    last = parts[0]
                            processed_author = f"{last.upper()} {first_initial.upper()}".strip()
                            processed_authors.append(processed_author)
                        entry_data[key_lower] = ';'.join(processed_authors)
                        # Extract the first author
                        # Stored as the first in the 'AU' field after mapping

                    # Process 'note' field to extract citation count
                    if key_lower == 'note':
                        note = value.upper()
                        match = re.search(r'CITED BY (\d+)', note)
                        citation_count = match.group(1) if match else ''
                        entry_data[key_lower] = citation_count

                    # Process 'affiliation' field to extract universities
                    if key_lower == 'affiliation':
                        affiliation = value.upper()
                        entry_data[key_lower] = affiliation

                        # Extract universities
                        university_pattern = r'([A-Z][A-Za-z\s]+(?:UNIVERSITY|COLLEGE|SCHOOL|INSTITUTE|ACADEMY|FACULTY|CENTER|DEPARTMENT)[A-Za-z\s]*)'
                        universities = re.findall(university_pattern, affiliation, re.IGNORECASE)
                        universities = [univ.strip().upper() for univ in universities]  # Clean up and uppercase university names
                        entry_data['au_un'] = '; '.join(universities)

                        # Get the first university
                        entry_data['au1_un'] = universities[0] if universities else ''

                entries_data.append(entry_data)

            except Exception as e:
                entry_id = entry.get('id', 'Unknown ID')
                print(f"Warning: Failed to process entry ID '{entry_id}': {e}")
                continue  # Skip to the next entry

        # Create the DataFrame
        df = pd.DataFrame(entries_data)

        # Rename columns according to the mapping
        df.rename(columns=column_mapping, inplace=True)

        # Ensure all desired columns are present
        for col in column_order:
            if col not in df.columns:
                df[col] = ''

        # Reorder the DataFrame columns
        df = df[column_order]

        # Create 'J9' by removing periods from 'JI'
        df['J9'] = df['JI'].str.replace('.', '', regex=False).fillna('')

        # Construct 'SR_FULL' and 'SR' columns using the renamed DataFrame columns
        def construct_sr_full(row):
            au = row['AU']
            py = row['PY']
            so = row['SO']
            if au and py and so:
                first_author = au.split(';')[0]
                return f"{first_author}, {py}, {so}"
            else:
                return ''

        def construct_sr(row):
            au = row['AU']
            py = row['PY']
            j9 = row['J9']
            if au and py and j9:
                first_author = au.split(';')[0]
                return f"{first_author}, {py}, {j9}"
            else:
                return ''

        df['SR_FULL'] = df.apply(construct_sr_full, axis=1)
        df['SR'] = df.apply(construct_sr, axis=1)

        return df

    except FileNotFoundError:
        print(f"The file '{file_path}' was not found.")
        return None
    except UnicodeDecodeError:
        print(f"Encoding error while trying to read the file '{file_path}'.")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None
