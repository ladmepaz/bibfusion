import pandas as pd
import re


def get_scopus_author_data(df_original: pd.DataFrame) -> pd.DataFrame:
    """
        Builds Author.csv from the Scopus articles DataFrame (mains + refs).
        Preserves:
            - AuthorID (Scopus), aligned by author order when available.
            - ORCID (normalized when provided as a URL).
            - OpenAlexAuthorID / openalex_work_id if present in df_original (aligned by AuthorOrder).
    """

    datos_autores = []

    for _, row in df_original.iterrows():
        sr = row.get('SR')

        # List of authors (text)
        if pd.notna(row.get('author')):
            autores = str(row['author']).split('; ')
        else:
            continue

        # List of AuthorID (Scopus), separated by ';'
        author_ids = []
        if 'authors_id' in row and pd.notna(row['authors_id']):
            author_ids = [aid.strip() for aid in str(row['authors_id']).split(';') if str(aid).strip()]

        # ORCID: extract only the ID if provided as a URL
        orcids = []
        if pd.notna(row.get('orcid')):
            orcid_matches = re.findall(r'https?://orcid\.org/(\d{4}-\d{4}-\d{4}-\d{3}[\dX])',
                                       str(row['orcid']).lower())
            orcids = [m.upper() for m in orcid_matches]

        # Full names
        autores_full = []
        if pd.notna(row.get('author_full_names')):
            for author in str(row['author_full_names']).split(';'):
                author = author.strip()
                m = re.search(r'([^(]+)\s*\(\d+\)', author)
                autores_full.append(m.group(1).strip() if m else author)

        # Individual and combined affiliations
        affiliations = {}
        authors_with_affiliations_list = []
        if pd.notna(row.get('authors_with_affiliations')):
            authors_afil = str(row['authors_with_affiliations']).split(';')
            for author_afil in authors_afil:
                if ',' in author_afil:
                    parts = author_afil.split(',', 1)
                    name_author = parts[0].strip()
                    affiliation = parts[1].strip() if len(parts) > 1 else 'NO AFFILIATION'
                    affiliations[name_author] = affiliation
        if affiliations:
            for name_author, affiliation in affiliations.items():
                author_name_parts = name_author.split()
                if len(author_name_parts) >= 2:
                    last_name = author_name_parts[0].replace('.', '')
                    initials = ' '.join([p[0] + '.' for p in author_name_parts[1:] if p])
                    formatted_name = f"{last_name} {initials}".upper()
                    authors_with_affiliations_list.append(f"{formatted_name}, {affiliation}")
        combined_authors_affiliations = "; ".join(authors_with_affiliations_list) if authors_with_affiliations_list else "NO AFFILIATIONS"

        # ResearcherID embedded in author_full_names
        researcher_ids = {}
        if pd.notna(row.get('author_full_names')):
            for author_full in str(row['author_full_names']).split(';'):
                m_id = re.search(r'([^(]+)\((\d+)\)', author_full.strip())
                if m_id:
                    name = m_id.group(1).strip()
                    researcher_id = m_id.group(2).strip()
                    researcher_ids[name] = researcher_id

        # Create entry per author
        for i, author in enumerate(autores):
            author_order = i + 1
            author_name = author.strip()
            author_fullname = autores_full[i] if i < len(autores_full) else ''
            # Affiliation
            affiliation = 'NO AFFILIATION'
            for key in affiliations:
                if author_name in key or key in author_name:
                    affiliation = affiliations[key]
                    break
            # ResearcherID
            researcher_id = ''
            for key, value in researcher_ids.items():
                if author_fullname in key or key in author_fullname:
                    researcher_id = value
                    break
            # ORCID
            orcid = "NO ORCID"
            if i < len(orcids):
                orcid = orcids[i]
            # Scopus AuthorID
            author_id = ""
            if i < len(author_ids):
                author_id = author_ids[i]

            datos_autores.append({
                'SR': sr,
                'AuthorOrder': author_order,
                'AuthorName': author_name,
                'AuthorFullName': author_fullname,
                'AuthorID': author_id,
                'Affiliation': affiliation,
                'authors_with_affiliations': combined_authors_affiliations,
                'CorrespondingAuthor': False,
                'Orcid': orcid,
                'ResearcherID': researcher_id,
                'Email': ''
            })

    df_autores = pd.DataFrame(datos_autores)

    # Add OpenAlexAuthorID and openalex_work_id aligned by SR and AuthorOrder if present in df_original
    try:
        rows = []
        for _, r in df_original.iterrows():
            sr0 = r.get('SR')
            work_id = r.get('openalex_work_id') if 'openalex_work_id' in df_original.columns else ''
            oa_list = []
            if 'author_id_openalex' in df_original.columns and pd.notna(r.get('author_id_openalex')):
                oa_list = [a.strip() for a in str(r.get('author_id_openalex')).split(';') if str(a).strip()]
            for i, _ in enumerate(str(r.get('author') or '').split('; ')):
                rows.append({'SR': sr0, 'AuthorOrder': i+1, 'OpenAlexAuthorID': oa_list[i] if i < len(oa_list) else '', 'openalex_work_id': work_id})
        if rows:
            oa_df = pd.DataFrame(rows)
            df_autores = df_autores.merge(oa_df, on=['SR','AuthorOrder'], how='left')
    except Exception:
        pass

    return df_autores
