import pandas as pd
import numpy as np
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
            
            # Procesamos los nombres completos de los autores
            autores_full = []
            if pd.notna(row['author_full_names']):
                # Extraemos los nombres completos sin el ID
                pattern = r'([^(]+)\s*\(\d+\)'
                autores_full = [re.search(pattern, autor.strip()).group(1).strip() if re.search(pattern, autor.strip()) else "" for autor in row['author_full_names'].split(';')]
            
            # Procesamos las afiliaciones
            afiliaciones = {}
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
            
            # Procesamos el autor correspondiente y email
            autores_correspondencia = []
            emails_por_autor = {}
            
            if pd.notna(row['correspondence_address']):
                # Dividimos por punto y coma para separar cada parte
                partes = row['correspondence_address'].split(';')
                
                # Extraemos información de correspondencia
                autor_actual = None
                for parte in partes:
                    parte = parte.strip()
                    
                    # Comprobamos si es un autor (generalmente inicia con inicial y punto)
                    autor_match = re.match(r'^([A-Z]\.\s+[A-Z]+)', parte)
                    if autor_match:
                        autor_actual = autor_match.group(1).strip()
                        autores_correspondencia.append(autor_actual)
                    
                    # Comprobamos si es un email
                    email_match = re.search(r'EMAIL:\s*([^;\s]+)', parte, re.IGNORECASE)
                    if email_match and autor_actual:
                        emails_por_autor[autor_actual] = email_match.group(1).strip()
            
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
                
                # Creamos versiones inversa del nombre (APELLIDO I. -> I. APELLIDO) para comparar con autores_correspondencia
                partes_nombre = author_name.split(' ')
                if len(partes_nombre) >= 2:
                    apellido = partes_nombre[0].replace('.', '')
                    iniciales = ' '.join(partes_nombre[1:])
                    nombre_invertido = f"{iniciales} {apellido}"
                    
                    # Verificamos todas las posibles formas del nombre
                    is_corresponding = False
                    email = ""
                    
                    for autor_corr in autores_correspondencia:
                        # Normalizamos para comparación
                        autor_norm = autor_corr.replace(' ', '').replace('.', '').upper()
                        author_name_norm = author_name.replace(' ', '').replace('.', '').upper()
                        nombre_invertido_norm = nombre_invertido.replace(' ', '').replace('.', '').upper()
                        
                        # Comparamos de varias formas
                        if (autor_norm in author_name_norm or 
                            author_name_norm in autor_norm or 
                            autor_norm in nombre_invertido_norm or 
                            nombre_invertido_norm in autor_norm):
                            is_corresponding = True
                            if autor_corr in emails_por_autor:
                                email = emails_por_autor[autor_corr]
                            break
                else:
                    is_corresponding = False
                    email = ""
                
                # Obtenemos el ResearcherID
                researcher_id = ""
                for key, value in researcher_ids.items():
                    if author_fullname in key or key in author_fullname:
                        researcher_id = value
                        break
                
                # Agregamos a la lista de datos
                datos_autores.append({
                    'SR': sr,
                    'AuthorOrder': author_order,
                    'AuthorName': author_name,
                    'AuthorFullName': author_fullname,  
                    'Affiliation': affiliation,
                    'CorrespondingAuthor': is_corresponding,
                    'Orcid': "",  # No hay información de ORCID
                    'ResearcherID': researcher_id,
                    'Email': email
                })
    
    # Creamos el DataFrame final
    df_autores = pd.DataFrame(datos_autores)
    return df_autores

# Para usar la función:
# df_autores = get_scopus_author_data(df_original)