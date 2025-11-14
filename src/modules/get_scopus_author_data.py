import pandas as pd
import re

def get_scopus_author_data(df_original):
    # Lista para almacenar los datos de cada autor
    datos_autores = []
    
    # Iteramos por cada fila del DataFrame original
    for index, row in df_original.iterrows():
        sr = row['SR']
        
        # Procesamos los autores
        if pd.notna(row['author']):
            autores = row['author'].split('; ')
            
            # Procesamos los ORCIDs (extraemos solo los números después de .org/)
            orcids = []
            if pd.notna(row['orcid']):
                # Extraemos solo la parte numérica del ORCID
                orcid_matches = re.findall(r'https?://orcid\.org/(\d{4}-\d{4}-\d{4}-\d{3}[\dX])', 
                                         row['orcid'].lower())
                orcids = [match.upper() for match in orcid_matches]  # Convertimos a mayúsculas
            
            # Procesamos los nombres completos de los autores
            autores_full = []
            if pd.notna(row['author_full_names']):
                for autor in row['author_full_names'].split(';'):
                    autor = autor.strip()
                    # Intenta extraer nombre con ID
                    match = re.search(r'([^(]+)\s*\(\d+\)', autor)
                    if match:
                        autores_full.append(match.group(1).strip())
                    else:
                        # Si no hay ID, usa el nombre completo tal cual
                        autores_full.append(autor)
            
            # Procesamos las afiliaciones para la columna individual
            afiliaciones = {}
            # Procesamos las afiliaciones para la columna combinada
            authors_with_affiliations_list = []
            
            if pd.notna(row['authors_with_affiliations']):
                # Dividimos por punto y coma para separar cada autor con su afiliación
                autores_afil = row['authors_with_affiliations'].split(';')
                for autor_afil in autores_afil:
                    if ',' in autor_afil:
                        # El primer elemento es el nombre del autor
                        partes = autor_afil.split(',', 1)
                        nombre_autor = partes[0].strip()
                        afiliacion = partes[1].strip() if len(partes) > 1 else 'NO AFFILIATION'
                        afiliaciones[nombre_autor] = afiliacion
                        
                        # Construimos el formato para authors_with_affiliations
                        # Extraemos el apellido y las iniciales
                        nombre_partes = nombre_autor.split()
                        if len(nombre_partes) >= 2:
                            apellido = nombre_partes[0].replace('.', '')
                            iniciales = ' '.join([p[0] + '.' for p in nombre_partes[1:] if p])
                            formatted_name = f"{apellido} {iniciales}".upper()
                            authors_with_affiliations_list.append(f"{formatted_name}, {afiliacion}")
            
            # Creamos la cadena combinada de autores con afiliaciones
            combined_authors_affiliations = "; ".join(authors_with_affiliations_list) if authors_with_affiliations_list else "NO AFFILIATIONS"
            
            # Procesamos los IDs de investigador
            researcher_ids = {}
            if pd.notna(row['author_full_names']):
                for autor_full in row['author_full_names'].split(';'):
                    match_id = re.search(r'([^(]+)\((\d+)\)', autor_full.strip())
                    if match_id:
                        nombre = match_id.group(1).strip()
                        researcher_id = match_id.group(2).strip()
                        researcher_ids[nombre] = researcher_id
            
            # Creamos una entrada para cada autor
            for i, autor in enumerate(autores):
                author_order = i + 1
                author_name = autor.strip()
                
                # Obtenemos el nombre completo
                author_fullname = ""
                if i < len(autores_full):
                    author_fullname = autores_full[i]
                
                # Obtenemos la afiliación
                affiliation = 'NO AFFILIATION'
                for key in afiliaciones:
                    if author_name in key or key in author_name:
                        affiliation = afiliaciones[key]
                        break
                
                # Obtenemos el ResearcherID
                researcher_id = ""
                for key, value in researcher_ids.items():
                    if author_fullname in key or key in author_fullname:
                        researcher_id = value
                        break
                
                # Obtenemos el ORCID (solo el número) o 'NO ORCID' si no hay
                orcid = "NO ORCID"
                if i < len(orcids):
                    orcid = orcids[i]
                
                # Agregamos a la lista de datos
                datos_autores.append({
                    'SR': sr,
                    'AuthorOrder': author_order,
                    'AuthorName': author_name,
                    'AuthorFullName': author_fullname,  
                    'Affiliation': affiliation,
                    'authors_with_affiliations': combined_authors_affiliations,  # Nueva columna
                    'CorrespondingAuthor': False,  # Siempre será False sin correspondence_address
                    'Orcid': orcid,
                    'ResearcherID': researcher_id,
                    'Email': ''  # Siempre vacío sin correspondence_address
                })
    
    # Creamos el DataFrame final
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
