import pandas as pd
import glob
import os

def process_scimago_data(scimago_files_path_pattern, abbr_file_path):
    """
    Reads and processes Scimago journal CSV files, splits multiple ISSNs 
    into print (Issn) and eISSN (eIssn), and enriches with journal abbreviations.
    
    Parameters:
    - scimago_files_path_pattern (str): Glob pattern to match all Scimago CSV files 
      (e.g., '.../scimagojr *.csv')
    - abbr_file_path (str): Path to CSV with journal abbreviations
    
    Returns:
    - DataFrame: Cleaned and enriched Scimago DataFrame
    """
    # Step 1: Read and combine all Scimago files
    csv_files = glob.glob(scimago_files_path_pattern)
    all_data = []

    for file in csv_files:
        # Extract the year from the filename
        year = os.path.splitext(os.path.basename(file))[0].split()[-1]
        
        # Read the CSV with semicolon as sep, double quotes as quote char
        df = pd.read_csv(file, sep=';', quotechar='"', dtype=str)
        
        # Add the 'year' column
        df['year'] = year
        
        # Strip leading/trailing spaces for all string columns
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        # Split the 'Issn' into 'Issn' (print) and 'eIssn'
        if 'Issn' in df.columns:
            df['Issn'] = df['Issn'].fillna('')
            splitted = df['Issn'].str.split(',', expand=True)
            
            # The first column (splitted[0]) is the print ISSN
            df['Issn'] = splitted[0].str.strip()
            
            # The second column (splitted[1]) is the eISSN (if present)
            # splitted.get(1) avoids errors if there's only one ISSN
            df['eIssn'] = splitted.get(1).str.strip() if 1 in splitted.columns else None

        all_data.append(df)

    # Combine into a single DataFrame
    final_df = pd.concat(all_data, ignore_index=True)

    # Keep only the specified columns (adding 'eIssn')
    columns_to_keep = [
        'Rank', 'Sourceid', 'Title', 'Type', 'Issn', 'eIssn', 'SJR', 'SJR Best Quartile',
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

    # Step 3: Create mapping (print ISSN -> journal_abbr) and enrich final dataset
    abbr_lookup = abbr_df[['ISSN', 'journal_abbr']].drop_duplicates(subset='ISSN').copy()
    abbr_lookup['ISSN'] = abbr_lookup['ISSN'].astype(str).str.strip()

    # Ensure Issn is properly stripped
    df_cleaned['Issn'] = df_cleaned['Issn'].astype(str).str.strip()

    # Build the dictionary
    issn_to_abbr = dict(zip(abbr_lookup['ISSN'], abbr_lookup['journal_abbr']))
    df_cleaned['journal_abbr'] = df_cleaned['Issn'].map(issn_to_abbr)

    # Step 4: Final postprocessing
    # Remove dots from journal_abbr
    df_cleaned['journal_abbr'] = df_cleaned['journal_abbr'].str.replace('.', '', regex=False)
    # Convert Title to uppercase
    df_cleaned['Title'] = df_cleaned['Title'].str.upper()

    print("✅ Processing complete. Here's a sample:")
    print(df_cleaned[['Title', 'Issn', 'eIssn', 'journal_abbr']].sample(5))

    return df_cleaned


# df_result = process_scimago_data(
#     r"C:\Users\User\OneDrive\Documentos\Preprocessing\preprocessing_3\preprocessing\tests\files\scimagojr *.csv",
#     r"C:\Users\User\OneDrive\Documentos\Preprocessing\preprocessing_3\preprocessing\tests\files\scimago_2024_combined.csv"
# )

import pandas as pd
import re

def parse_medline_journals(txt_path: str) -> pd.DataFrame:
    """
    Parse a PubMed J_Medline.txt file into a DataFrame.

    Each journal block (separated by lines of dashes) becomes one row,
    with columns:
      - JrId
      - JournalTitle
      - MedAbbr
      - PrintISSN
      - OnlineISSN
      - IsoAbbr
      - NlmId

    Parameters
    ----------
    txt_path : str
        Path to the J_Medline.txt file.

    Returns
    -------
    pd.DataFrame
    """
    # Read the entire file
    with open(txt_path, 'r', encoding='utf-8') as f:
        text = f.read()

    # Split into raw records on lines of 50+ dashes
    raw_records = re.split(r'-{5,}', text)

    records = []
    for raw in raw_records:
        lines = [ln.strip() for ln in raw.strip().splitlines() if ln.strip()]
        if not lines:
            continue

        rec = {}
        for line in lines:
            if ':' not in line:
                continue
            key, val = line.split(':', 1)
            key = key.strip()
            val = val.strip()

            # map PubMed field names -> our column names
            if key == 'JrId':
                rec['JrId'] = val
            elif key == 'JournalTitle':
                rec['JournalTitle'] = val
            elif key == 'MedAbbr':
                rec['MedAbbr'] = val
            elif key == 'ISSN (Print)':
                rec['PrintISSN'] = val
            elif key == 'ISSN (Online)':
                rec['OnlineISSN'] = val
            elif key == 'IsoAbbr':
                rec['IsoAbbr'] = val
            elif key == 'NlmId':
                rec['NlmId'] = val

        records.append(rec)

    # Build DataFrame, ensure all expected columns exist
    df = pd.DataFrame(records, columns=[
        'JrId', 'JournalTitle', 'MedAbbr',
        'PrintISSN', 'OnlineISSN',
        'IsoAbbr', 'NlmId'
    ])

    # Clean up empty strings into NaN
    df.replace({'': pd.NA}, inplace=True)

    return df

