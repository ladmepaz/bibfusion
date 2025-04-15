import pandas as pd
import glob
import os

def process_scimago_data(scimago_files_path_pattern, abbr_file_path):
    """
    Reads and processes Scimago journal CSV files, splits multiple ISSNs, and enriches with journal abbreviations.
    
    Parameters:
    - scimago_files_path_pattern (str): Glob pattern to match all Scimago CSV files (e.g., '.../scimagojr *.csv')
    - abbr_file_path (str): Path to CSV with journal abbreviations
    
    Returns:
    - DataFrame: Cleaned and enriched Scimago DataFrame
    """
    # Step 1: Read and combine all Scimago files
    csv_files = glob.glob(scimago_files_path_pattern)
    all_data = []

    for file in csv_files:
        year = os.path.splitext(os.path.basename(file))[0].split()[-1]
        df = pd.read_csv(file, sep=';', quotechar='"', dtype=str)
        df['year'] = year
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        if 'Issn' in df.columns:
            df['Issn'] = df['Issn'].fillna('')
            df = df.assign(Issn=df['Issn'].str.split(',')).explode('Issn')
            df['Issn'] = df['Issn'].str.strip()

        all_data.append(df)

    final_df = pd.concat(all_data, ignore_index=True)

    columns_to_keep = [
        'Rank', 'Sourceid', 'Title', 'Type', 'Issn', 'SJR', 'SJR Best Quartile',
        'H index', 'Total Docs. (1999)', 'Total Docs. (3years)', 'Total Refs.',
        'Total Citations (3years)', 'Citable Docs. (3years)',
        'Citations / Doc. (2years)', 'Ref. / Doc.', '%Female', 'Overton', 'SDG',
        'Country', 'Region', 'Publisher', 'Coverage', 'Categories', 'Areas',
        'year'
    ]
    df_cleaned = final_df.filter(columns_to_keep)

    # Step 2: Read journal abbreviation file
    abbr_df = pd.read_csv(abbr_file_path, sep=',', dtype=str)
    abbr_df['journal'] = abbr_df['journal'].astype(str).str.strip()
    abbr_df['ISSN'] = abbr_df['ISSN'].astype(str).str.strip()
    abbr_df['journal_abbr'] = abbr_df['journal_abbr'].astype(str).str.strip()
    abbr_df = abbr_df[['journal', 'ISSN', 'journal_abbr']].drop_duplicates()

    # Step 3: Create mapping and enrich
    abbr_lookup = abbr_df[['ISSN', 'journal_abbr']].drop_duplicates(subset='ISSN').copy()
    abbr_lookup['ISSN'] = abbr_lookup['ISSN'].astype(str).str.strip()
    df_cleaned['Issn'] = df_cleaned['Issn'].astype(str).str.strip()
    issn_to_abbr = dict(zip(abbr_lookup['ISSN'], abbr_lookup['journal_abbr']))
    df_cleaned['journal_abbr'] = df_cleaned['Issn'].map(issn_to_abbr)

    # Step 4: Postprocessing
    df_cleaned['journal_abbr'] = df_cleaned['journal_abbr'].str.replace('.', '', regex=False)
    df_cleaned['Title'] = df_cleaned['Title'].str.upper()

    print("✅ Processing complete. Sample:")
    print(df_cleaned[['Title', 'Issn', 'journal_abbr']].sample(5))

    return df_cleaned

# df_result = process_scimago_data(
#     r"C:\Users\User\OneDrive\Documentos\Preprocessing\preprocessing_3\preprocessing\tests\files\scimagojr *.csv",
#     r"C:\Users\User\OneDrive\Documentos\Preprocessing\preprocessing_3\preprocessing\tests\files\scimago_2024_combined.csv"
# )
