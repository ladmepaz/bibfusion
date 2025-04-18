import pandas as pd
import re

def scopus_csv_to_df(file_path):
    """
    Reads a Scopus CSV file and transforms the column titles, normalizes text, and adds a column 'SR':
    - Column titles are made lowercase, parentheses removed, and spaces replaced with underscores.
    - Specified columns are converted to uppercase.
    - Adds a 'SR' column combining: First author (dots removed), Year, Abbreviated Journal (without dots).
    """
    try:
        df = pd.read_csv(file_path)

        def transform_column_labels(columns):
            transformed_columns = []
            for col in columns:
                col = col.lower()
                col = col.replace('(', '').replace(')', '')
                col = col.replace(' ', '_')
                col = re.sub(r'\W+', '_', col)
                col = col.strip('_')
                transformed_columns.append(col)
            return transformed_columns

        df.columns = transform_column_labels(df.columns)

        columns_to_uppercase = [
            'authors', 'author_full_names', 'title', 'source_title', 'affiliations',
            'authors_with_affiliations', 'abstract', 'author_keywords', 'index_keywords',
            'funding_details', 'references', 'correspondence_address', 'editors',
            'publisher', 'sponsors', 'conference_name', 'conference_date', 'conference_location',
            'language_of_original_document', 'abbreviated_source_title', 'document_type',
            'publication_stage', 'open_access', 'source'
        ]
        funding_text_columns = [col for col in df.columns if col.startswith('funding_text')]
        columns_to_uppercase.extend(funding_text_columns)

        corrected_columns = {
            'correspondece_address': 'correspondence_address',
        }
        columns_to_uppercase = [corrected_columns.get(col, col) for col in columns_to_uppercase]

        for col in columns_to_uppercase:
            if col in df.columns:
                df[col] = df[col].astype(str).str.upper()
            else:
                print(f"Warning: Column '{col}' not found in DataFrame.")

        rename_columns = {
            'authors': 'author',
            'art_no': 'article_number',
            'language_of_original_document': 'language',
            'open_access': 'open_access_indicator',
            'source_title': 'journal'
        }
        df.rename(columns=rename_columns, inplace=True)

        # Crear columna SR (sin puntos en las iniciales del autor)
        if all(col in df.columns for col in ['author', 'year', 'abbreviated_source_title']):
            df['SR'] = df.apply(
                lambda row: f"{row['author'].split(';')[0].replace('.', '').strip()}, "
                            f"{row['year']}, "
                            f"{row['abbreviated_source_title'].replace('.', '')}",
                axis=1
            )
        else:
            print("Warning: One or more columns required for 'SR' creation are missing.")

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
