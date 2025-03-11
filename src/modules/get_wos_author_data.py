import pandas as pd
import re
def get_wos_author_data(wos_df_3: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts and aligns author, affiliation, corresponding author, ORCID, ResearcherID, and email information from the wos_df_3 dataframe
    by selecting 'SR', 'AU', 'AF', 'C1', 'RP', 'OI', 'RI', and 'EM' columns, processing author names, cleaning data,
    parsing affiliations from 'C1', identifying the corresponding author, extracting ORCID IDs, ResearcherIDs, and emails,
    and combining all information into a single dataframe.

    Special handling:
    - If AuthorFullName or AuthorName is in special cases (e.g., 'NO, NAME', 'NO NAME', 'NO N', 'NO, AUTHOR', 'NO A'), set AuthorName to 'ANONYMOUS'.
    - Trim leading and trailing whitespace from AuthorName to ensure consistency.
    - If 'AU' is 'ANONYMOUS' but 'AF' contains author names, replace 'ANONYMOUS' in 'AU' with names from 'AF'.

    Parameters:
    -----------
    wos_df_3 : pd.DataFrame
        The dataframe containing article data, including the 'SR', 'AU', 'AF', 'C1', 'RP', 'OI', 'RI', and 'EM' columns.

    Returns:
    --------
    authors_df : pd.DataFrame
        A dataframe with one row per author per article, containing 'SR', 'AuthorOrder', 'AuthorName',
        'AuthorFullName', 'Affiliation', 'CorrespondingAuthor', 'Orcid', 'ResearcherID', and 'Email' columns.
    """

    # Step 0: Define the list of invalid 'SR' values to remove
    invalid_sr_values = [
        '1999, 1999',
        '2013, 2013, V24',
        '2013, 2013',
        '2010, 2010',
        '2008, 2008, V73',
        '2006, 2006',
        '2005, 2005, P7',
        '2000, 2000',
        '1990, 1990, V55',
        '1981, 1981, V46',
        '1918, 1918'
    ]

    # Step 1: Remove rows with invalid 'SR' values
    wos_df_cleaned = wos_df_3[~wos_df_3['SR'].isin(invalid_sr_values)].copy()

    # Step 2: Create 'wos_authors' dataframe with required columns
    required_columns = ['SR', 'AU', 'AF', 'C1', 'RP', 'OI', 'RI', 'EM']
    for col in required_columns:
        if col not in wos_df_cleaned.columns:
            wos_df_cleaned[col] = ''
    wos_authors = wos_df_cleaned[required_columns].copy()
    wos_authors['AU'] = wos_authors['AU'].fillna('')
    wos_authors['AF'] = wos_authors['AF'].fillna('')
    wos_authors['C1'] = wos_authors['C1'].fillna('')
    wos_authors['RP'] = wos_authors['RP'].fillna('')
    wos_authors['OI'] = wos_authors['OI'].fillna('')
    wos_authors['RI'] = wos_authors['RI'].fillna('')
    wos_authors['EM'] = wos_authors['EM'].fillna('')

    # Step 3: Split 'AU' and 'AF' by ';'
    wos_authors['AU'] = wos_authors['AU'].apply(lambda x: [a.strip() for a in x.split(';') if a.strip()])
    wos_authors['AF'] = wos_authors['AF'].apply(lambda x: [a.strip() for a in x.split(';') if a.strip()])

    # Step 4: Assign 'AuthorOrder' and handle missing 'AU' entries
    def generate_abbreviated_name(full_name):
        if not full_name or full_name.strip() == '':
            return ''
        # Remove unwanted characters from full_name before processing
        full_name = full_name.replace('“', '').replace('”', '').strip()
        # Handle special cases
        special_cases = ['NO, NAME', 'NO NAME', 'NO N', 'NO, AUTHOR', 'NO A']
        if full_name.upper() in special_cases:
            return 'ANONYMOUS'
        # Handle both 'LastName, FirstName(s)' and 'LastName FirstName(s)' formats
        if ',' in full_name:
            # Format: 'LastName, FirstName(s)'
            parts = full_name.split(',', 1)
            last_name = parts[0].strip()
            first_names = parts[1].strip()
        else:
            # Format: 'LastName FirstName(s)'
            parts = full_name.strip().split()
            last_name = parts[0]
            first_names = ' '.join(parts[1:])
        # Take the initials of the first names
        initials = ''.join([name_part.strip()[0] for name_part in first_names.split() if name_part.strip()])
        abbreviated_name = f"{last_name} {initials}"
        return abbreviated_name

    def assign_author_order(row):
        au_list = row['AU']
        af_list = row['AF']

        # Handle case where 'AU' is 'ANONYMOUS' but 'AF' has author names
        if len(au_list) == 1 and au_list[0].upper() == 'ANONYMOUS' and len(af_list) > 0 and af_list[0].upper() != 'ANONYMOUS':
            # Replace 'AU' with 'AF' names
            au_list = [generate_abbreviated_name(af_name) for af_name in af_list]

        max_length = max(len(au_list), len(af_list))
        author_order = list(range(1, max_length + 1))

        # Extend 'AU' and 'AF' lists to the same length
        au_list += [''] * (max_length - len(au_list))
        af_list += [''] * (max_length - len(af_list))

        # Fill missing 'AU' entries using 'AF'
        for i in range(max_length):
            if au_list[i] == '':
                # Generate abbreviated name from 'AF' entry
                af_name = af_list[i]
                au_list[i] = generate_abbreviated_name(af_name)
        return pd.DataFrame({
            'SR': row['SR'],
            'AuthorOrder': author_order,
            'AuthorName': au_list,
            'AuthorFullName': af_list,
            'C1': row['C1'],
            'RP': row['RP'],
            'OI': row['OI'],
            'RI': row['RI'],
            'EM': row['EM']
        })

    # Apply the function
    author_rows = wos_authors.apply(assign_author_order, axis=1)
    authors_df = pd.concat(author_rows.tolist(), ignore_index=True)

    # Step 5: Fill missing 'AuthorFullName' with 'AuthorName' if needed
    authors_df['AuthorFullName'] = authors_df['AuthorFullName'].where(
        authors_df['AuthorFullName'] != '', authors_df['AuthorName']
    )

    # Step 6: Clean 'AuthorName' and 'AuthorFullName' by removing unwanted characters
    unwanted_chars = ['“', '”']
    for char in unwanted_chars:
        authors_df['AuthorName'] = authors_df['AuthorName'].str.replace(char, '', regex=False)
        authors_df['AuthorFullName'] = authors_df['AuthorFullName'].str.replace(char, '', regex=False)

    # Remove leading and trailing whitespace from 'AuthorName' and 'AuthorFullName'
    authors_df['AuthorName'] = authors_df['AuthorName'].str.strip()
    authors_df['AuthorFullName'] = authors_df['AuthorFullName'].str.strip()

    # Step 7: Remove rows where 'AuthorName' contains numbers
    pattern_digits = re.compile(r'\d')
    authors_df = authors_df[~authors_df['AuthorName'].apply(lambda x: bool(pattern_digits.search(x)))].reset_index(drop=True)

    # Step 8: Remove rows where 'AuthorName' starts with *, ., (, ", or ?
    unwanted_start_chars = ('*', '.', '(', '"', '?')
    authors_df['AuthorName'] = authors_df['AuthorName'].str.lstrip()
    authors_df = authors_df[~authors_df['AuthorName'].str.startswith(unwanted_start_chars)].reset_index(drop=True)

    # Step 9: Standardize 'AuthorFullName' to handle missing commas
    def standardize_author_fullname(name):
        name = name.replace('“', '').replace('”', '').strip()
        if ',' not in name and name.upper() not in ['NO NAME', 'NO N', 'NO, AUTHOR', 'NO A']:
            # Convert 'LastName FirstName(s)' to 'LastName, FirstName(s)'
            parts = name.split()
            if len(parts) > 1:
                last_name = parts[0]
                first_names = ' '.join(parts[1:])
                name = f"{last_name}, {first_names}"
            else:
                # If only one part, keep it as is
                name = name
        return name

    authors_df['AuthorFullName'] = authors_df['AuthorFullName'].apply(standardize_author_fullname)

    # Step 10: Regenerate 'AuthorName' from 'AuthorFullName' to ensure correct format
    authors_df['AuthorName'] = authors_df['AuthorFullName'].apply(generate_abbreviated_name)

    # Step 10b: Handle cases where 'AuthorName' is in special cases
    special_cases = ['NO N', 'NO NAME', 'NO, NAME', 'NO, AUTHOR', 'NO A']
    authors_df['AuthorName'] = authors_df['AuthorName'].apply(
        lambda x: 'ANONYMOUS' if x.upper() in special_cases else x
    )

    # Remove leading and trailing whitespace from 'AuthorName' again
    authors_df['AuthorName'] = authors_df['AuthorName'].str.strip()

    # Step 11: Parse 'C1' to extract affiliations
    def parse_affiliations(c1_entry):
        """
        Parses the 'C1' entry to extract a list of dictionaries with 'AuthorFullName' and 'Affiliation'.
        """
        affiliations = []
        if not c1_entry:
            return affiliations

        # Use regex to find all occurrences of [Author Name] Affiliation
        pattern = r'\[([^\]]+)\]\s*([^\[]*)'
        matches = re.findall(pattern, c1_entry)

        for author_names, affiliation in matches:
            # Split multiple author names inside the brackets by ';'
            author_names_list = [name.strip() for name in author_names.split(';') if name.strip()]
            for author_name in author_names_list:
                # Standardize author name to match with 'AuthorFullName'
                author_name_std = standardize_author_fullname(author_name)
                affiliations.append({
                    'AuthorFullName': author_name_std,
                    'Affiliation': affiliation.strip()
                })
        return affiliations

    # Apply 'parse_affiliations' to each row to get a list of affiliations per article
    authors_df['AffiliationsList'] = authors_df['C1'].apply(parse_affiliations)

    # Step 12: Map affiliations to authors (with standardization)
    def standardize_name_for_matching(name):
        return re.sub(r'\s+', ' ', name.replace(',', '').lower().strip())

    def get_author_affiliation(row):
        # Standardize the author's full name
        author_full_name_std = standardize_name_for_matching(row['AuthorFullName'])
        # Match the author's full name with the affiliations
        for affiliation in row['AffiliationsList']:
            affiliation_name_std = standardize_name_for_matching(affiliation['AuthorFullName'])
            if author_full_name_std == affiliation_name_std:
                return affiliation['Affiliation']
        return ''

    authors_df['Affiliation'] = authors_df.apply(get_author_affiliation, axis=1)

    # Step 13: Identify the corresponding author based on 'RP' column
    def parse_corresponding_author(rp_entry):
        """
        Parses the 'RP' entry to extract the corresponding author's full name.
        """
        if not rp_entry:
            return None
        # Extract the author name before the first parenthesis
        match = re.match(r'^(.+?)\s*\(', rp_entry.strip())
        if match:
            author_name = match.group(1).strip()
            # Standardize the author name
            author_name = standardize_author_fullname(author_name)
            return author_name
        else:
            # If no parenthesis, extract up to the first comma
            match = re.match(r'^([^,]+)', rp_entry.strip())
            if match:
                author_name = match.group(1).strip()
                author_name = standardize_author_fullname(author_name)
                return author_name
        return None

    # Extract corresponding author name
    authors_df['CorrespondingAuthorName'] = authors_df['RP'].apply(parse_corresponding_author)

    # Initialize 'CorrespondingAuthor' column
    authors_df['CorrespondingAuthor'] = False

    # Function to get last name and first initial for matching
    def get_lastname_firstinitial(name):
        if not name:
            return ''
        name = name.replace('“', '').replace('”', '').strip()
        if ',' in name:
            # Format: 'LastName, FirstName(s)'
            parts = name.split(',', 1)
            last_name = parts[0].strip().lower()
            first_names = parts[1].strip()
        else:
            # Format: 'LastName FirstName(s)'
            parts = name.strip().split()
            last_name = parts[0].lower()
            first_names = ' '.join(parts[1:])
        if first_names:
            first_initial = first_names[0].lower()
        else:
            first_initial = ''
        return f"{last_name} {first_initial}"

    # Assign True to the corresponding author
    for idx, row in authors_df.iterrows():
        # Standardize names for matching
        author_name_key = get_lastname_firstinitial(row['AuthorFullName'])
        corresponding_author_key = get_lastname_firstinitial(row['CorrespondingAuthorName']) if row['CorrespondingAuthorName'] else ''
        if author_name_key == corresponding_author_key:
            authors_df.at[idx, 'CorrespondingAuthor'] = True

    # Step 14: Parse 'OI' to extract ORCID IDs and map to authors
    def parse_orcid_ids(oi_entry):
        """
        Parses the 'OI' entry to extract a list of dictionaries with 'AuthorFullName' and 'Orcid'.
        """
        orcid_list = []
        if not oi_entry:
            return orcid_list

        # Split entries by semicolon
        entries = [entry.strip() for entry in oi_entry.split(';') if entry.strip()]
        for entry in entries:
            # Split into name and ORCID ID
            if '/' in entry:
                name_part, orcid_part = entry.rsplit('/', 1)
                orcid = orcid_part.strip()
                author_name = name_part.strip()
                # Standardize author name to match with 'AuthorFullName'
                author_name_std = standardize_author_fullname(author_name)
                orcid_list.append({
                    'AuthorFullName': author_name_std,
                    'Orcid': orcid
                })
        return orcid_list

    # Apply 'parse_orcid_ids' to each row to get a list of ORCID IDs per article
    authors_df['OrcidList'] = authors_df['OI'].apply(parse_orcid_ids)

    # Assign ORCID IDs to authors
    def get_author_orcid(row):
        # Standardize the author's full name
        author_full_name_std = standardize_name_for_matching(row['AuthorFullName'])
        # Match the author's full name with the ORCID list
        for orcid_entry in row['OrcidList']:
            orcid_name_std = standardize_name_for_matching(orcid_entry['AuthorFullName'])
            if author_full_name_std == orcid_name_std:
                return orcid_entry['Orcid']
        return ''

    authors_df['Orcid'] = authors_df.apply(get_author_orcid, axis=1)

    # Step 15: Parse 'RI' to extract ResearcherIDs and map to authors
    def parse_researcher_ids(ri_entry):
        """
        Parses the 'RI' entry to extract a list of dictionaries with 'AuthorFullName' and 'ResearcherID'.
        """
        researcher_id_list = []
        if not ri_entry:
            return researcher_id_list

        # Split entries by semicolon
        entries = [entry.strip() for entry in ri_entry.split(';') if entry.strip()]
        for entry in entries:
            # Split into name and ResearcherID
            if '/' in entry:
                name_part, researcher_id_part = entry.rsplit('/', 1)
                researcher_id = researcher_id_part.strip()
                author_name = name_part.strip()
                # Standardize author name to match with 'AuthorFullName'
                author_name_std = standardize_author_fullname(author_name)
                researcher_id_list.append({
                    'AuthorFullName': author_name_std,
                    'ResearcherID': researcher_id
                })
        return researcher_id_list

    # Apply 'parse_researcher_ids' to each row to get a list of ResearcherIDs per article
    authors_df['ResearcherIDList'] = authors_df['RI'].apply(parse_researcher_ids)

    # Assign ResearcherIDs to authors
    def get_author_researcher_id(row):
        # Standardize the author's full name
        author_full_name_std = standardize_name_for_matching(row['AuthorFullName'])
        # Match the author's full name with the ResearcherID list
        for rid_entry in row['ResearcherIDList']:
            rid_name_std = standardize_name_for_matching(rid_entry['AuthorFullName'])
            if author_full_name_std == rid_name_std:
                return rid_entry['ResearcherID']
        return ''

    authors_df['ResearcherID'] = authors_df.apply(get_author_researcher_id, axis=1)

    # Step 16: Extract emails and assign to authors based on name matching
    def parse_emails(em_entry):
        """
        Parses the 'EM' entry to extract a list of email addresses.
        """
        if not em_entry:
            return []
        # Replace double semicolons with single semicolons
        em_entry = em_entry.replace(';;', ';')
        # Split emails by semicolon
        emails = [email.strip() for email in em_entry.split(';') if email.strip()]
        return emails

    # Apply 'parse_emails' to get the list of emails
    authors_df['EmailList'] = authors_df['EM'].apply(parse_emails)

    # Assign emails to authors
    def assign_emails(row):
        author_full_name_std = standardize_name_for_matching(row['AuthorFullName'])
        emails = row['EmailList']
        if not emails:
            return ''

        # Extract names from emails
        for email in emails:
            email_username = email.split('@')[0]
            email_name_parts = re.split(r'[._]', email_username)
            email_name_parts = [part.lower() for part in email_name_parts if part]

            # Build possible name combinations from email
            email_name_combinations = [
                ' '.join(email_name_parts),
                ' '.join(reversed(email_name_parts))
            ]

            # Check if any combination matches the author's name
            author_name_parts = author_full_name_std.split()
            author_name = ' '.join(author_name_parts)
            for email_name in email_name_combinations:
                if email_name in author_name or author_name in email_name:
                    return email
        return ''

    # Apply 'assign_emails' to each row
    authors_df['Email'] = authors_df.apply(assign_emails, axis=1)

    # Step 17: Drop unnecessary columns and reset index
    authors_df = authors_df.drop(columns=['C1', 'AffiliationsList', 'RP', 'CorrespondingAuthorName', 'OI',
                                          'OrcidList', 'RI', 'ResearcherIDList', 'EM', 'EmailList']).reset_index(drop=True)

    # Step 18: Select relevant columns
    authors_df = authors_df[['SR', 'AuthorOrder', 'AuthorName', 'AuthorFullName', 'Affiliation',
                             'CorrespondingAuthor', 'Orcid', 'ResearcherID', 'Email']]

    return authors_df
