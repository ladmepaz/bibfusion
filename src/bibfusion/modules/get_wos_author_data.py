import pandas as pd
import re
def get_wos_author_data(wos_df_3: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts and aligns author, affiliation, corresponding author, ORCID, ResearcherID, and email information from the wos_df_3 dataframe
    by selecting 'SR', 'author', 'author_full_names', 'affiliations', 'reprint_address', 'orcid', 'researcher_id_number', and 'email_address' columns, processing author names, cleaning data,
    parsing affiliations from 'affiliations', identifying the corresponding author, extracting ORCID IDs, ResearcherIDs, and emails,
    and combining all information into a single dataframe.

    Special handling:
    - If AuthorFullName or AuthorName is in special cases (e.g., 'NO, NAME', 'NO NAME', 'NO N', 'NO, AUTHOR', 'NO A'), set AuthorName to 'ANONYMOUS'.
    - Trim leading and trailing whitespace from AuthorName to ensure consistency.
    - If 'author' is 'ANONYMOUS' but 'author_full_names' contains author names, replace 'ANONYMOUS' in 'author' with names from 'author_full_names'.

    Parameters:
    -----------
    wos_df_3 : pd.DataFrame
        The dataframe containing article data, including the 'SR', 'author', 'author_full_names', 'affiliations', 'reprint_address', 'orcid', 'researcher_id_number', and 'email_address' columns.

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
    # Include OpenAlex fields coming from previous enrichment steps
    required_columns = [
        'SR',
        'author',
        'author_full_names',
        'affiliations',
        'reprint_address',
        'orcid',
        'researcher_id_number',
        'email_address',
        'author_id_openalex',       # semicolon-separated OpenAlex author IDs aligned with authors
        'openalex_work_id',         # OpenAlex work ID for the article
    ]
    for col in required_columns:
        if col not in wos_df_cleaned.columns:
            wos_df_cleaned[col] = ''
    wos_authors = wos_df_cleaned[required_columns].copy()
    wos_authors['author'] = wos_authors['author'].fillna('')
    wos_authors['author_full_names'] = wos_authors['author_full_names'].fillna('')
    wos_authors['affiliations'] = wos_authors['affiliations'].fillna('')
    wos_authors['reprint_address'] = wos_authors['reprint_address'].fillna('')
    wos_authors['orcid'] = wos_authors['orcid'].fillna('')
    wos_authors['author_id_openalex'] = wos_authors['author_id_openalex'].fillna('')
    wos_authors['openalex_work_id'] = wos_authors['openalex_work_id'].fillna('')
    wos_authors['researcher_id_number'] = wos_authors['researcher_id_number'].fillna('')
    wos_authors['email_address'] = wos_authors['email_address'].fillna('')

    # Step 3: Split 'author' and 'author_full_names' by ';'
    wos_authors['author'] = wos_authors['author'].apply(lambda x: [a.strip() for a in x.split(';') if a.strip()])
    wos_authors['author_full_names'] = wos_authors['author_full_names'].apply(lambda x: [a.strip() for a in x.split(';') if a.strip()])

    # Step 4: Assign 'AuthorOrder' and handle missing 'author' entries
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
        au_list = row['author']
        af_list = row['author_full_names']

        # Handle case where 'author' is 'ANONYMOUS' but 'author_full_names' has author names
        if len(au_list) == 1 and au_list[0].upper() == 'ANONYMOUS' and len(af_list) > 0 and af_list[0].upper() != 'ANONYMOUS':
            # Replace 'author' with 'author_full_names' names
            au_list = [generate_abbreviated_name(af_name) for af_name in af_list]

        max_length = max(len(au_list), len(af_list))
        author_order = list(range(1, max_length + 1))

        # Extend 'author' and 'author_full_names' lists to the same length
        au_list += [''] * (max_length - len(au_list))
        af_list += [''] * (max_length - len(af_list))

        # Fill missing 'author' entries using 'author_full_names'
        for i in range(max_length):
            if au_list[i] == '':
                # Generate abbreviated name from 'author_full_names' entry
                af_name = af_list[i]
                au_list[i] = generate_abbreviated_name(af_name)
        return pd.DataFrame({
            'SR': row['SR'],
            'AuthorOrder': author_order,
            'AuthorName': au_list,
            'AuthorFullName': af_list,
            'affiliations': row['affiliations'],
            'reprint_address': row['reprint_address'],
            'orcid': row['orcid'],
            'researcher_id_number': row['researcher_id_number'],
            'email_address': row['email_address']
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

    # Step 11: Parse 'affiliations' to extract affiliations
    def parse_affiliations(c1_entry):
        """
        Parses the 'affiliations' entry to extract a list of dictionaries with 'AuthorFullName' and 'Affiliation'.
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
    authors_df['AffiliationsList'] = authors_df['affiliations'].apply(parse_affiliations)

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

    # Step 13: Identify the corresponding author based on 'reprint_address' column
    def parse_corresponding_author(rp_entry):
        """
        Parses the 'reprint_address' entry to extract the corresponding author's full name.
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
    authors_df['CorrespondingAuthorName'] = authors_df['reprint_address'].apply(parse_corresponding_author)

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

    # Step 14: Parse 'orcid' to extract ORCID IDs and map to authors
    def parse_orcid_ids(oi_entry):
        """
        Parses the 'orcid' entry to extract a list of dictionaries with 'AuthorFullName' and 'Orcid'.
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
    authors_df['OrcidList'] = authors_df['orcid'].apply(parse_orcid_ids)

    # Assign ORCID IDs to authors
    def split_orcid_field(orcid_field):
        # Split by semicolon and remove spaces
        return [entry.strip() for entry in orcid_field.split(';') if entry.strip()]

    def extract_orcid_mapping(orcid_field):
        """
        Returns a list with ORCID per author in order. If an author does not have an ORCID, '' is placed.
        """
        entries = split_orcid_field(orcid_field)
        result = []
        for entry in entries:
            if '/' in entry:
                parts = entry.split('/')
                if len(parts) >= 2:
                    result.append(parts[-1])  # ORCID
                else:
                    result.append('')
            else:
                result.append('')  # No ORCID
        return result
    # Assign ORCID to each individual author
    def assign_orcid(row):
        orcid_list = extract_orcid_mapping(row['orcid'])
        index = row['AuthorOrder'] - 1
        if index < len(orcid_list):
            return orcid_list[int(index)]
        else:
            return ''

    authors_df['Orcid'] = authors_df.apply(assign_orcid, axis=1)

    # Step 15: Map OpenAlex Author IDs per author order and attach work ID
    def split_semicolon_list(val: str):
        if not isinstance(val, str):
            return []
        return [p.strip() for p in val.split(';') if p.strip()]

    # Build SR -> list(OpenAlex Author IDs) mapping and SR -> work_id mapping
    sr_to_author_ids = dict(zip(
        wos_authors['SR'],
        wos_authors['author_id_openalex'].astype(str).apply(split_semicolon_list)
    ))
    sr_to_work_id = dict(zip(
        wos_authors['SR'],
        wos_authors['openalex_work_id'].astype(str)
    ))

    def assign_openalex_author_id(row):
        lst = sr_to_author_ids.get(row['SR'], [])
        try:
            idx = int(row['AuthorOrder']) - 1
        except Exception:
            idx = -1
        if idx is not None and 0 <= idx < len(lst):
            return lst[idx]
        return ''

    authors_df['OpenAlexAuthorID'] = authors_df.apply(assign_openalex_author_id, axis=1)
    authors_df['openalex_work_id'] = authors_df['SR'].map(sr_to_work_id)

    # Step 15b: Reorder columns to place 'openalex_work_id' next to 'SR' if present
    def reorder_columns(df_in: pd.DataFrame) -> pd.DataFrame:
        cols = list(df_in.columns)
        if 'SR' in cols and 'openalex_work_id' in cols:
            cols.remove('openalex_work_id')
            sr_index = cols.index('SR')
            cols.insert(sr_index + 1, 'openalex_work_id')
        # Prefer to show OpenAlexAuthorID before Orcid if both exist
        if 'OpenAlexAuthorID' in cols and 'Orcid' in cols:
            cols.remove('OpenAlexAuthorID')
            orcid_index = cols.index('Orcid')
            cols.insert(orcid_index, 'OpenAlexAuthorID')
        return df_in[cols]

    authors_df = reorder_columns(authors_df)

    # def get_author_orcid(row):
    #     # Standardize the author's full name
    #     author_full_name_std = standardize_name_for_matching(row['AuthorFullName'])
    #     # Match the author's full name with the ORCID list
    #     for orcid_entry in row['OrcidList']:
    #         orcid_name_std = standardize_name_for_matching(orcid_entry['AuthorFullName'])
    #         if author_full_name_std == orcid_name_std:
    #             return orcid_entry['Orcid']
    #     return ''

    # authors_df['Orcid'] = authors_df.apply(get_author_orcid, axis=1)

    # Step 15: Parse 'researcher_id_number' to extract ResearcherIDs and map to authors
    def parse_researcher_ids(ri_entry):
        """
        Parses the 'researcher_id_number' entry to extract a list of dictionaries with 'AuthorFullName' and 'ResearcherID'.
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
    authors_df['ResearcherIDList'] = authors_df['researcher_id_number'].apply(parse_researcher_ids)

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

    # # Step 16: Extract emails and assign to authors based on name matching
    # def parse_emails(em_entry):
    #     """
    #     Parses the 'email_address' entry to extract a list of email addresses.
    #     """
    #     if not em_entry:
    #         return []
    #     # Replace double semicolons with single semicolons
    #     em_entry = em_entry.replace(';;', ';')
    #     # Split emails by semicolon
    #     emails = [email.strip() for email in em_entry.split(';') if email.strip()]
    #     return emails

    # # Apply 'parse_emails' to get the list of emails
    # authors_df['EmailList'] = authors_df['email_address'].apply(parse_emails)

    # # Assign emails to authors
    # def assign_emails(row):
    #     author_full_name_std = standardize_name_for_matching(row['AuthorFullName'])
    #     emails = row['EmailList']
    #     if not emails:
    #         return ''

    #     # Extract names from emails
    #     for email in emails:
    #         email_username = email.split('@')[0]
    #         email_name_parts = re.split(r'[._]', email_username)
    #         email_name_parts = [part.lower() for part in email_name_parts if part]

    #         # Build possible name combinations from email
    #         email_name_combinations = [
    #             ' '.join(email_name_parts),
    #             ' '.join(reversed(email_name_parts))
    #         ]

    #         # Check if any combination matches the author's name
    #         author_name_parts = author_full_name_std.split()
    #         author_name = ' '.join(author_name_parts)
    #         for email_name in email_name_combinations:
    #             if email_name in author_name or author_name in email_name:
    #                 return email
    #     return ''

    # Paso 16 modificado: Extraer emails
    def parse_emails(em_entry):
        """
        Parse the input 'email_address' to extract a list of emails.
        """
        if not isinstance(em_entry, str) or not em_entry.strip():
            return []
        # Replace double semicolons with single semicolon
        em_entry = em_entry.replace(';;', ';')
        # Split emails by semicolon
        emails = [email.strip() for email in em_entry.split(';') if email.strip()]
        return emails

    # Create a dictionary of emails by SR
    sr_emails_dict = {}
    for idx, row in wos_authors.iterrows():
        sr = row['SR']
        emails = parse_emails(row['email_address'])
        sr_emails_dict[sr] = emails

    # Function to assign emails to authors
    def assign_emails_improved(group_df):
        """
        Assign emails to authors of the same article (SR) more effectively.
        """
        sr = group_df['SR'].iloc[0]
        emails = sr_emails_dict.get(sr, [])
        result_emails = []
        
        # If there are no emails, return an empty list for all authors
        if not emails:
            return [''] * len(group_df)
        
        # Case 1: If we have exactly the same number of emails as authors, assign in order
        if len(emails) == len(group_df):
            return emails
        
        # Case 2: Try to match emails with author names
        assigned_emails = [''] * len(group_df)
        available_emails = emails.copy()
        
        for i, (_, author_row) in enumerate(group_df.iterrows()):
            author_full_name = author_row['AuthorFullName'].lower() if isinstance(author_row['AuthorFullName'], str) else ""
            author_name = author_row['AuthorName'].lower() if isinstance(author_row['AuthorName'], str) else ""
            
            # Split author name into parts
            last_name = ""
            first_name = ""
            
            if ',' in author_full_name:
                name_parts = author_full_name.split(',', 1)
                last_name = name_parts[0].strip()
                if len(name_parts) > 1:
                    first_name_parts = name_parts[1].strip().split()
                    first_name = first_name_parts[0] if first_name_parts else ""
            else:
                name_parts = author_full_name.split()
                last_name = name_parts[0] if name_parts else ""
                first_name = name_parts[1] if len(name_parts) > 1 else ""
            
            # Search for matches between the email and the author's name
            best_match = None
            best_score = 0
            
            for email in available_emails:
                email_username = email.split('@')[0].lower()
                
                # Calculate match score
                score = 0
                
                # Check if the last name is in the email username
                if last_name and last_name in email_username:
                    score += 3
                
                # Check if the first name initial is in the email username
                if first_name and first_name[0] in email_username:
                    score += 2
                
                # Check if there are more parts of the name in the email username
                for part in author_full_name.replace(',', ' ').split():
                    if len(part) > 2 and part.lower() in email_username:
                        score += 1
                
                if score > best_score:
                    best_score = score
                    best_match = email
            
            # Assign the best email found
            if best_score > 0 and best_match:
                assigned_emails[i] = best_match
                # Remove the assigned email from the list to avoid duplicates
                if best_match in available_emails:
                    available_emails.remove(best_match)
        
        # If there are remaining emails and authors without email, assign them sequentially
        empty_indices = [i for i, email in enumerate(assigned_emails) if not email]
        for i in empty_indices:
            if available_emails:
                assigned_emails[i] = available_emails.pop(0)
        
        return assigned_emails

    # Apply email assignment by groups
    email_assignments = {}
    for sr, group in authors_df.groupby('SR'):
        emails = assign_emails_improved(group)
        for i, (idx, _) in enumerate(group.iterrows()):
            if i < len(emails):
                email_assignments[idx] = emails[i]
            else:
                email_assignments[idx] = ''

    # Assign emails to the DataFrame
    authors_df['Email'] = authors_df.index.map(lambda idx: email_assignments.get(idx, ''))

        
    
    
    # Original code
    # Apply 'assign_emails' to each row
    #authors_df['Email'] = authors_df.apply(assign_emails_improved)), axis=1)

    # Step 17: Drop unnecessary columns and reset index
    authors_df = authors_df.drop(columns=['affiliations', 'AffiliationsList', 'reprint_address', 'CorrespondingAuthorName', 'orcid',
                                          'OrcidList', 'researcher_id_number', 'ResearcherIDList', 'email_address']).reset_index(drop=True)

    # Step 18: Select relevant columns (include OpenAlex IDs and place work_id next to SR)
    cols_final = [
        'SR', 'openalex_work_id', 'AuthorOrder', 'AuthorName', 'AuthorFullName', 'Affiliation',
        'CorrespondingAuthor', 'OpenAlexAuthorID', 'Orcid', 'ResearcherID', 'Email'
    ]
    # Keep only columns that exist to avoid KeyError if some fields are missing
    cols_existing = [c for c in cols_final if c in authors_df.columns]
    authors_df = authors_df[cols_existing]

    return authors_df
