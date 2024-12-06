import pandas as pd
import re

def scopus_csv_to_df(file_path):
    """
    Reads a Scopus CSV file and transforms the column titles by:
    - Converting them to lowercase.
    - Removing parentheses '(' and ')'.
    - Replacing blank spaces with underscores '_'.
    Then, it makes the contents of specified columns uppercase.

    Parameters:
    ----------
    file_path : str
        The path to the Scopus CSV file.

    Returns:
    -------
    pd.DataFrame
        DataFrame containing the data from the CSV file with transformed column names
        and uppercase content in specified columns.
    """
    try:
        # Read the CSV file
        df = pd.read_csv(file_path)

        # Transform column labels
        def transform_column_labels(columns):
            transformed_columns = []
            for col in columns:
                # Convert to lowercase
                col = col.lower()
                # Remove parentheses
                col = col.replace('(', '').replace(')', '')
                # Replace spaces with underscores
                col = col.replace(' ', '_')
                # Replace any remaining non-alphanumeric characters with underscores
                col = re.sub(r'\W+', '_', col)
                # Remove leading or trailing underscores
                col = col.strip('_')
                transformed_columns.append(col)
            return transformed_columns

        # Apply the transformation to the column names
        df.columns = transform_column_labels(df.columns)

        # List of columns to make uppercase
        columns_to_uppercase = [
            'authors', 'author_full_names', 'title', 'source_title', 'affiliations',
            'authors_with_affiliations', 'abstract', 'author_keywords', 'index_keywords',
            'funding_details', 'references', 'correspondence_address', 'editors',
            'publisher', 'sponsors', 'conference_name', 'conference_date', 'conference_location',
            'language_of_original_document', 'abbreviated_source_title', 'document_type',
            'publication_stage', 'open_access', 'source'
        ]

        # For 'funding_texts', include all columns that start with 'funding_text'
        funding_text_columns = [col for col in df.columns if col.startswith('funding_text')]
        columns_to_uppercase.extend(funding_text_columns)

        # Correct potential typos in column names
        # For example, 'correspondece_address' should be 'correspondence_address'
        corrected_columns = {
            'correspondece_address': 'correspondence_address',
            # Add other corrections if needed
        }
        columns_to_uppercase = [
            corrected_columns.get(col, col) for col in columns_to_uppercase
        ]

        # Make the contents of these columns uppercase
        for col in columns_to_uppercase:
            if col in df.columns:
                df[col] = df[col].astype(str).str.upper()
            else:
                print(f"Warning: Column '{col}' not found in DataFrame.")

        return df

    except FileNotFoundError:
        print(f"The file '{file_path}' was not found.")
        return None
    except pd.errors.EmptyDataError:
        print(f"The file '{file_path}' is empty or not in the correct format.")
        return None
    except Exception as e:
        print(f"An error occurred while processing the file: {e}")
        return None
