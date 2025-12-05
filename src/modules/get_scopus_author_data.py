import pandas as pd
import re


def get_scopus_author_data(df_original: pd.DataFrame) -> pd.DataFrame:
    """
    Construye Author.csv a partir del DataFrame de artículos de Scopus (mains + refs).
    Conserva:
      - AuthorID (Scopus) alineado por orden de autor cuando existe.
      - Orcid (normalizado si viene como URL).
      - OpenAlexAuthorID / openalex_work_id si están en df_original (se alinean por AuthorOrder).
    """
    datos_autores = []

    for _, row in df_original.iterrows():
        sr = row.get('SR')

        # Lista de autores (texto)
        if pd.notna(row.get('author')):
            autores = str(row['author']).split('; ')
        else:
            continue

        # Lista de AuthorID (Scopus), separados por ';'
        author_ids = []
        if 'authors_id' in row and pd.notna(row['authors_id']):
            author_ids = [aid.strip() for aid in str(row['authors_id']).split(';') if str(aid).strip()]

        # ORCID: extrae sólo el ID si viene como URL
        orcids = []
        if pd.notna(row.get('orcid')):
            orcid_matches = re.findall(r'https?://orcid\.org/(\d{4}-\d{4}-\d{4}-\d{3}[\dX])',
                                       str(row['orcid']).lower())
            orcids = [m.upper() for m in orcid_matches]

        # Nombres completos
        autores_full = []
        if pd.notna(row.get('author_full_names')):
            for autor in str(row['author_full_names']).split(';'):
                autor = autor.strip()
                m = re.search(r'([^(]+)\s*\(\d+\)', autor)
                autores_full.append(m.group(1).strip() if m else autor)

        # Afiliaciones individuales y combinadas
        afiliaciones = {}
        authors_with_affiliations_list = []
        if pd.notna(row.get('authors_with_affiliations')):
            autores_afil = str(row['authors_with_affiliations']).split(';')
            for autor_afil in autores_afil:
                if ',' in autor_afil:
                    partes = autor_afil.split(',', 1)
                    nombre_autor = partes[0].strip()
                    afiliacion = partes[1].strip() if len(partes) > 1 else 'NO AFFILIATION'
                    afiliaciones[nombre_autor] = afiliacion
        if afiliaciones:
            for nombre_autor, afiliacion in afiliaciones.items():
                nombre_partes = nombre_autor.split()
                if len(nombre_partes) >= 2:
                    apellido = nombre_partes[0].replace('.', '')
                    iniciales = ' '.join([p[0] + '.' for p in nombre_partes[1:] if p])
                    formatted_name = f"{apellido} {iniciales}".upper()
                    authors_with_affiliations_list.append(f"{formatted_name}, {afiliacion}")
        combined_authors_affiliations = "; ".join(authors_with_affiliations_list) if authors_with_affiliations_list else "NO AFFILIATIONS"

        # ResearcherID embebido en author_full_names
        researcher_ids = {}
        if pd.notna(row.get('author_full_names')):
            for autor_full in str(row['author_full_names']).split(';'):
                m_id = re.search(r'([^(]+)\((\d+)\)', autor_full.strip())
                if m_id:
                    nombre = m_id.group(1).strip()
                    researcher_id = m_id.group(2).strip()
                    researcher_ids[nombre] = researcher_id

        # Crear entrada por autor
        for i, autor in enumerate(autores):
            author_order = i + 1
            author_name = autor.strip()
            author_fullname = autores_full[i] if i < len(autores_full) else ''
            # Afiliación
            affiliation = 'NO AFFILIATION'
            for key in afiliaciones:
                if author_name in key or key in author_name:
                    affiliation = afiliaciones[key]
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

    # Añadir OpenAlexAuthorID y openalex_work_id alineados por SR y AuthorOrder si existen en df_original
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
