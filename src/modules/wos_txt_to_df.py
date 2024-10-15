# wos_txt_to_df.py

import pandas as pd
import re
import os

def wos_txt_to_df(file_path):
    """
    Converts a Web of Science (WoS) text file into a pandas DataFrame with specified formatting and columns.

    Parameters:
        file_path (str): The path to the WoS text file to be processed.

    Returns:
        pandas.DataFrame: A DataFrame containing the processed data with specified columns and formatting.

    Raises:
        FileNotFoundError: If the specified file does not exist.
        ValueError: If the file is not a valid .txt file or does not conform to the expected WoS format.
    """
    try:
        # Check if the file exists
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"The file '{file_path}' does not exist.")

        # Check if the file has a .txt extension
        _, file_extension = os.path.splitext(file_path)
        if file_extension.lower() != '.txt':
            raise ValueError(f"The file '{file_path}' is not a .txt file.")

        # Open the file and read the first few lines to validate content
        with open(file_path, 'r', encoding='utf-8') as f:
            first_lines = [next(f).strip() for _ in range(5)]
            # Check for expected WoS markers
            if not any(line.startswith('FN ') for line in first_lines) or not any(line.startswith('VR ') for line in first_lines):
                raise ValueError(f"The file '{file_path}' does not appear to be a valid Web of Science (WoS) text file.")

        # Re-open the file to start processing from the beginning
        with open(file_path, 'r', encoding='utf-8') as f:
            # Initialize variables
            records = []
            current_record = {}
            current_field = None

            # Define fields that can have multiple entries
            multiple_fields = {'AU', 'AF', 'CR', 'ID', 'DE', 'C1', 'C3', 'EM', 'RI', 'OI', 'FX', 'RP'}

            # Fields to ignore
            ignore_fields = {'FN', 'VR'}

            # Fields to convert to uppercase
            uppercase_fields = {'AB', 'AF', 'AU', 'C1', 'CR', 'DE', 'DT', 'EM', 'FU', 'FX', 'JI', 'J9', 'LA', 'OA',
                                'OI', 'PT', 'RI', 'RP', 'SC', 'SP', 'TI', 'WC', 'WE'}

            for line in f:
                line = line.rstrip('\n')
                if not line.strip():
                    continue  # Skip empty lines

                if line == 'ER':
                    # End of record
                    records.append(current_record)
                    current_record = {}
                    current_field = None
                    continue  # Move to the next line

                if line[:2] in ignore_fields and line[2] == ' ':
                    # Ignore these fields
                    current_field = None
                    continue

                # Check for new field
                match = re.match(r'^([A-Z0-9]{2}) (.*)', line)
                if match:
                    current_field, value = match.groups()
                    value = value.strip()

                    if current_field in ignore_fields:
                        current_field = None
                        continue

                    if current_field in multiple_fields:
                        # Fields that can have multiple entries
                        if current_record.get(current_field):
                            current_record[current_field].append(value)
                        else:
                            current_record[current_field] = [value]
                    else:
                        current_record[current_field] = value
                else:
                    # Continuation of the previous field
                    value = line.strip()
                    if current_field:
                        if current_field in multiple_fields:
                            # Append as a new entry
                            current_record[current_field].append(value)
                        else:
                            current_record[current_field] += ' ' + value

        # Handle the last record if the file does not end with 'ER'
        if current_record:
            records.append(current_record)

        # Process records
        for record in records:
            # Process 'AU' field to desired format
            if 'AU' in record:
                processed_authors = []
                for name in record['AU']:
                    # Reformat the name 'Last, F' -> 'LAST F'
                    parts = name.split(', ')
                    if len(parts) == 2:
                        lastname, firstname = parts
                        processed_name = f"{lastname.upper()} {firstname}"
                    else:
                        processed_name = name.upper()
                    processed_authors.append(processed_name)
                # Assign back to 'AU' field
                record['AU'] = ';'.join(processed_authors)

            # Process 'AF' field to desired format
            if 'AF' in record:
                processed_full_names = []
                for name in record['AF']:
                    # Reformat the name 'Lastname, Firstname' -> 'LASTNAME, FIRSTNAME'
                    processed_name = name.upper()
                    processed_full_names.append(processed_name)
                # Assign back to 'AF' field
                record['AF'] = ';'.join(processed_full_names)

            # Process 'CR' field to desired format
            if 'CR' in record:
                processed_references = []
                for ref in record['CR']:
                    processed_ref = ref.upper()
                    processed_references.append(processed_ref)
                # Assign back to 'CR' field
                record['CR'] = '; '.join(processed_references)

            # Process 'C1' field to uppercase
            if 'C1' in record:
                processed_addresses = []
                for address in record['C1']:
                    processed_address = address.upper()
                    processed_addresses.append(processed_address)
                # Assign back to 'C1' field
                record['C1'] = '; '.join(processed_addresses)

            # Process 'C3' field to uppercase
            if 'C3' in record:
                processed_addresses = []
                for address in record['C3']:
                    processed_address = address.upper()
                    processed_addresses.append(processed_address)
                # Assign back to 'C3' field
                record['C3'] = '; '.join(processed_addresses)

            # Process 'DE' field to uppercase
            if 'DE' in record:
                processed_descriptors = []
                for descriptor in record['DE']:
                    processed_descriptor = descriptor.upper()
                    processed_descriptors.append(processed_descriptor)
                # Assign back to 'DE' field
                record['DE'] = '; '.join(processed_descriptors)

            # Process additional fields to uppercase
            for field in uppercase_fields:
                if field in record and field not in {'AU', 'AF', 'CR', 'C1', 'C3', 'DE'}:
                    if field in multiple_fields and isinstance(record[field], list):
                        # Multiple-entry field
                        processed_values = [value.upper() for value in record[field]]
                        record[field] = '; '.join(processed_values)
                    else:
                        # Single-entry field
                        record[field] = record[field].upper()

            # Flatten other fields
            for key, value in record.items():
                if key not in {'AU', 'AF', 'CR', 'C1', 'C3', 'DE'} | uppercase_fields and isinstance(value, list):
                    record[key] = '; '.join(value)

            # Add 'DB' field with value 'WOS'
            record['DB'] = 'WOS'

            # Create 'SR' field
            if 'AU' in record and 'PY' in record and 'J9' in record:
                first_author = record['AU'].split(';')[0]
                sr_value = f"{first_author}, {record['PY']}, {record['J9']}"
                record['SR'] = sr_value
            else:
                record['SR'] = ''

        # Create DataFrame from records
        df = pd.DataFrame(records)

        # Desired columns in the specified order
        desired_columns = ['AU', 'AF', 'CR', 'AB', 'AR', 'BP', 'C1', 'C3', 'CL', 'CT', 'CY', 'DA', 'DE', 'DI', 'DT',
                           'EA', 'EF', 'EI', 'EM', 'EP', 'ER', 'FU', 'FX', 'GA', 'HC', 'HO', 'HP', 'ID', 'IS', 'J9', 'JI',
                           'LA', 'MA', 'NR', 'OA', 'OI', 'PA', 'PD', 'PG', 'PI', 'PM', 'PN', 'PT', 'PU', 'PY', 'RI', 'RP',
                           'SC', 'SI', 'SN', 'SO', 'SP', 'SU', 'TC', 'TI', 'U1', 'U2', 'UT', 'VL', 'WC', 'WE', 'Z9', 'DB', 'SR']

        # Include only the desired columns, adding empty columns if they are missing
        for col in desired_columns:
            if col not in df.columns:
                df[col] = ''

        df = df[desired_columns]

        return df

    except FileNotFoundError as e:
        print(f"Error: {e}")
        return None
    except ValueError as e:
        print(f"Error: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None
