import pandas as pd
import re
import urllib.parse

def process_scopus_references(df):
    """
    Procesa un DataFrame de Scopus para extraer y estructurar sus referencias bibliográficas.

    Parameters:
    -----------
    df : pandas.DataFrame
        DataFrame que debe contener al menos las columnas:
        - 'references': Referencias bibliográficas en formato texto
        - 'abbreviated_source_title': (opcional) Nombre abreviado de la fuente
        - 'author': (opcional) Autores del documento fuente
        - 'year': (opcional) Año de publicación del documento fuente

    Returns:
    --------
    pandas.DataFrame
        DataFrame con las referencias procesadas y estructuradas contiendo las columnas:
        - 'SR': Identificador del documento fuente
        - 'SR_ref': Identificador de la referencia (Primer_Autor, año, revista)
        - 'title': Título de la referencia
        - 'authors': Autores de la referencia (separados por ';')
        - 'journal': Revista de la referencia
        - 'year': Año de publicación de la referencia
        - 'volume': Volumen de la revista
        - 'pages': Páginas del artículo
        - 'doi': DOI del artículo
        - 'CR_ref': Texto original de la referencia
    """
    # Verificar que exista la columna 'references'
    if 'references' not in df.columns:
        raise ValueError("El DataFrame debe contener la columna 'references'")
    
    # Crear copia del DataFrame para no modificar el original
    scopus_df = df.copy()
    
    # Preparar columna 'source_title'
    if 'abbreviated_source_title' in scopus_df.columns:
        scopus_df['source_title'] = scopus_df['abbreviated_source_title'].str.replace('.', '', regex=False)
    else:
        scopus_df['source_title'] = '-'
    
    # Crear columna 'SR' si no existe pero tenemos 'author' y 'year'
    if 'SR' not in scopus_df.columns and 'author' in scopus_df.columns and 'year' in scopus_df.columns:
        scopus_df = _create_sr_column(scopus_df)
    elif 'SR' not in scopus_df.columns:
        # Crear un ID único si no hay SR ni se puede crear
        scopus_df['SR'] = [f"DOC_{i+1}" for i in range(len(scopus_df))]
    
    # Extraer y procesar las referencias
    references_list = []
    
    for _, row in scopus_df.iterrows():
        sr_value = row['SR']
        references_text = str(row['references'])
        
        # Separar múltiples referencias (por punto y coma)
        individual_refs = [ref.strip() for ref in re.split(r';\s*', references_text) if ref.strip()]
        
        # Procesar cada referencia individual
        for ref_text in individual_refs:
            reference_data = _parse_reference(ref_text)
            if reference_data:  # Si se pudo procesar correctamente
                reference_data['SR'] = sr_value
                reference_data['CR_ref'] = ref_text
                references_list.append(reference_data)
    
    # Si no hay referencias procesadas, devolver DataFrame vacío
    if not references_list:
        return pd.DataFrame()
    
    # Crear DataFrame con las referencias procesadas
    references_df = pd.DataFrame(references_list)
    
    # Crear columna 'SR_ref'
    references_df['SR_ref'] = references_df.apply(
        lambda row: f"{row['authors'].split(';')[0] if isinstance(row['authors'], str) and ';' in row['authors'] else row['authors']}, {row['year']}, {row['source_title']}",
        axis=1
    )
    
    # Mapear 'source_title' desde scopus_df
    references_df['source_title_mainarticle'] = references_df['SR'].map(
        scopus_df.set_index('SR')['source_title'].to_dict()
    )
    
    # Definir el orden de las columnas
    column_order = ['SR', 'SR_ref', 'title', 'authors', 'source_title', 'source_title_mainarticle', 
                   'year', 'volume', 'pages', 'doi', 'CR_ref']
    
    # Asegurarse de que todas las columnas existen
    for col in column_order:
        if col not in references_df.columns:
            references_df[col] = '-'
    
    # Devolver el DataFrame ordenado
    return references_df[column_order]

def _create_sr_column(df):
    """
    Crea la columna SR (Source Reference) con formato "Primer_Autor, año, revista"
    """
    def make_sr(row):
        author = row.get('author', '')
        year = row.get('year', '')
        journal = row.get('source_title', '')
        
        # Extraer primer autor
        first_author = author.split(';')[0] if isinstance(author, str) and ';' in author else author
        
        return f"{first_author}, {year}, {journal}"
    
    df['SR'] = df.apply(make_sr, axis=1)
    return df

def _parse_reference(reference):
    """
    Analiza una referencia individual y extrae todos sus componentes
    
    Parameters:
    -----------
    reference : str
        Texto de una referencia bibliográfica
        
    Returns:
    --------
    dict
        Diccionario con los componentes extraídos o None si no se pudo procesar
    """
    # Extraer autores
    authors, remaining = _extract_authors(reference)
    if not authors:
        return None
    
    # Extraer DOI si existe
    doi, remaining = _extract_doi(remaining)
    
    # Extraer el año
    year, remaining = _extract_year(remaining)
    
    # Extraer título, revista, volumen y páginas
    title, journal, volume, pages = _extract_title_journal_volume_pages(remaining)
    if not title:
        return None
    
    return {
        'authors': authors,
        'title': title,
        'source_title': journal,
        'year': year,
        'volume': volume,
        'pages': pages,
        'doi': doi
    }

def _extract_authors(reference):
    """
    Extrae los autores de una referencia bibliográfica.
    
    Parameters:
    -----------
    reference : str
        Texto de referencia con formato: "AUTOR1, AUTOR2, ..., TÍTULO, REVISTA, ..."
        
    Returns:
    --------
    tuple
        - authors_str: String con autores separados por ';'
        - remaining: Texto restante después de los autores
    """
    # Dividir por comas
    parts = [p.strip() for p in reference.split(',')]
    if not parts:
        return '', reference
    
    authors = []
    title_index = None
    
    # Patrón para detectar un autor: APELLIDO seguido de INICIALES
    author_pattern = r'^[A-Z][A-Za-z\'\-]+\s+(?:[A-Z]\.?)+$'
    
    # Examinar cada parte para determinar si es un autor
    for i, part in enumerate(parts):
        if re.match(author_pattern, part):
            authors.append(part)
        else:
            # Si no parece un autor, hemos llegado al título
            title_index = i
            break
    
    # Si no se detectaron autores, usar el primer elemento como autor por defecto
    if not authors and parts:
        authors = [parts[0]]
        title_index = 1
    
    # Si todos parecen autores (poco probable), usar el último como título
    if title_index is None:
        title_index = len(authors)
        authors = authors[:-1]
    
    # Unir autores con punto y coma
    authors_str = ';'.join(authors)
    
    # Reconstruir el texto restante
    remaining = ', '.join(parts[title_index:])
    
    return authors_str, remaining

def _extract_doi(text):
    """
    Extrae el DOI del texto si está presente
    
    Parameters:
    -----------
    text : str
        Texto de la referencia
        
    Returns:
    --------
    tuple
        - doi: DOI extraído o cadena vacía
        - text: Texto sin el DOI
    """
    # Buscar "DOI:" seguido por "10." y caracteres
    doi_match = re.search(r'DOI:\s*(10\.\S+)', text, flags=re.IGNORECASE)
    if doi_match:
        doi = doi_match.group(1)
        text = text.replace(doi_match.group(0), '')
    else:
        # Alternativamente, buscar "10." directamente
        doi_match = re.search(r'(10\.\S+)', text)
        if doi_match:
            doi = doi_match.group(1)
            text = text.replace(doi, '')
        else:
            doi = ''
    
    # Decodificar caracteres URL-encoded
    doi = urllib.parse.unquote(doi)
    # Eliminar comas o puntos al final
    doi = doi.rstrip('.,')
    text = text.strip(' ,.')
    
    return doi, text

def _extract_year(text):
    """
    Extrae el año de publicación, asumiendo que está entre paréntesis
    
    Parameters:
    -----------
    text : str
        Texto de la referencia
        
    Returns:
    --------
    tuple
        - year: Año extraído o cadena vacía
        - text: Texto sin el año
    """
    # Buscar año en paréntesis, típicamente al final
    year_match = re.search(r'\((\d{4})\)', text)
    if year_match:
        year = year_match.group(1)
        # Eliminar el año con paréntesis del texto
        text = text.replace(year_match.group(0), '').strip(' ,.')
    else:
        # Si no hay paréntesis, buscar un año de 4 dígitos
        year_match = re.search(r'\b(\d{4})\b', text)
        if year_match:
            year = year_match.group(1)
            # Confirmar que es un año válido (entre 1800 y 2030)
            if 1800 <= int(year) <= 2030:
                text = text.replace(year, '').strip(' ,.')
            else:
                year = ''
        else:
            year = ''
    
    return year, text

def _extract_title_journal_volume_pages(text):
    """
    Extrae título, revista, volumen y páginas de una referencia bibliográfica.
    Excluye nombres de países del campo journal.
    """
    # 1. Eliminar año
    text = re.sub(r'\(\d{4}\)', '', text).strip().rstrip(',')

    # 2. Extraer páginas
    pages = ""
    match_pages = re.search(r'PP\.\s*([\d\-–]+)', text)
    if match_pages:
        pages = match_pages.group(1).strip()
        text = text.replace(match_pages.group(0), '').strip().rstrip(',')

    # 3. Separar por comas
    components = [comp.strip() for comp in text.split(',') if comp.strip()]

    # 4. Buscar volumen/número desde el final
    
    # Carga del CSV con separador ;
    paises_df = pd.read_csv(r"tests\files\country.csv", sep=';', header=None, names=["codigo", "pais"])

    # Convertimos los nombres de países a mayúsculas para facilitar la comparación
    lista_paises = set(paises_df["pais"].str.upper().str.strip())

    volume = ""
    i = len(components) - 1
    while i >= 0:
        comp = components[i]
        if re.fullmatch(r'\d+([\-–]\d+)?', comp):
            if not volume:
                volume = comp
            i -= 1
        elif comp.upper() in lista_paises:
            i -= 1  # saltar países
        else:
            break

    # 5. Journal
    journal = ""
    if i >= 0:
        journal = components[i]
        i -= 1

    # 6. Título (lo que queda antes del journal)
    title_parts = components[:i+1]
    title_parts = [part for part in title_parts if part.isupper() or ':' in part]
    title = ', '.join(title_parts).strip()

    return title, journal.strip(), volume.strip(), pages.strip()




def _clean_text(text):
    """
    Limpia el texto eliminando caracteres innecesarios
    """
    # Eliminar múltiples espacios
    text = re.sub(r'\s+', ' ', text)
    # Eliminar puntuación al inicio y final
    text = text.strip(' ,.:;-')
    return text

# Ejemplo de uso
# if __name__ == "__main__":
#     # Ejemplo con una lista de referencias
#     data = {
#         'author': ['Smith J', 'Johnson K'],
#         'year': [2020, 2019],
#         'abbreviated_source_title': ['J. Sci.', 'Nature'],
#         'references': [
#             "ARTHUR T.M., BONO J.L., KALCHAYANAND N., CHARACTERIZATION OF ESCHERICHIA COLI O157:H7 STRAINS FROM CONTAMINATED RAW BEEF TRIM DURING HIGH EVENT PERIODS, APPL ENVIRON MICROBIOL, 80, PP. 506-514, (2014)",
#             "CALLAWAY T.R., ANDERSON R.C., EDRINGTON T.S., GENOVESE K.J., BISCHOFF K.M., POOLE T.L., JUNG Y.S., HARVEY R.B., NISBET D.J., WHAT ARE WE DOING ABOUT ESCHERICHIA COLI O157: H7 IN CATTLE, J ANIM SCI, 82, E, PP. E93-E99, (2004)"
#         ]
#     }
    
#     test_df = pd.DataFrame(data)
    
#     # Procesar referencias
#     references_df = process_scopus_references(test_df)
#     print(references_df.head())