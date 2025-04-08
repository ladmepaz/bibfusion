import pandas as pd
import re
import os

def fill_missing_affiliations(df):
    """
    Rellena los valores vacíos o espacios en blanco en la columna 'Affiliation' con 'NO AFFILIATION'.
        
    Parámetros:
        df (pd.DataFrame): DataFrame con la columna 'Affiliation'.
        
    Retorna:
        pd.DataFrame: DataFrame con los valores corregidos en 'Affiliation'.
    """
    df["Affiliation"] = df["Affiliation"].fillna("NO AFFILIATION.").replace(r"^\s*$", "NO AFFILIATION.", regex=True)
    return df

def extract_countries(df, country_codes_file):
    """
    Extrae países de la columna 'Affiliation' y crea una nueva columna 'Country'.
    Procesa cada afiliación separada por punto y coma individualmente.
    Coloca "NO COUNTRY" donde no se detecte ningún país.
    
    Args:
        df (pandas.DataFrame): DataFrame con la columna 'Affiliation'
        country_codes_file (str): Ruta al archivo CSV con códigos de países
        
    Returns:
        pandas.DataFrame: DataFrame con la nueva columna 'Country'
    """
    # Cargar el archivo de códigos de países
    if os.path.exists(country_codes_file):
        country_codes = pd.read_csv(country_codes_file, delimiter=';', header=None, names=['code', 'name'])
        # Limpiar espacios en blanco y convertir a mayúsculas
        country_codes['code'] = country_codes['code'].str.strip().str.upper()
        country_codes['name'] = country_codes['name'].str.strip().str.upper()
        
        # Crear un diccionario para mapear códigos a nombres de países
        code_to_name = dict(zip(country_codes['code'], country_codes['name']))
        
        # Crear una lista de todos los nombres de países para búsqueda
        all_country_names = country_codes['name'].tolist()
        all_country_codes = country_codes['code'].tolist()
        
        # Ordenar países por longitud (descendente) para evitar coincidencias parciales
        all_country_names = sorted(all_country_names, key=len, reverse=True)
        all_country_codes = sorted(all_country_codes, key=len, reverse=True)
        
        # Diccionario para mapear nombres a sí mismos (para normalización)
        name_to_name = {name: name for name in all_country_names}
        
        # Combinar ambos diccionarios para tener un único mapeo
        country_map = {**code_to_name, **name_to_name}
    else:
        print(f"El archivo {country_codes_file} no existe. No se realizará la extracción de países.")
        return df
    
    # Función para extraer países de una única afiliación
    def extract_countries_from_single_affiliation(single_affiliation):
        if pd.isna(single_affiliation) or not single_affiliation.strip():
            return "NO COUNTRY"
        
        if single_affiliation.strip().upper() == 'NO AFFILIATION':
            return "NO COUNTRY"
        
        # Convertir a mayúsculas
        single_affiliation = single_affiliation.upper().strip()
        
        # Conjunto para almacenar países encontrados (evita duplicados dentro de una misma afiliación)
        found_countries = set()
        
        # Buscar códigos de países en la afiliación
        for code in all_country_codes:
            pattern = r'\b' + re.escape(code) + r'\b'
            if re.search(pattern, single_affiliation):
                # Usar el nombre completo del país en mayúsculas
                found_countries.add(country_map[code])
        
        # Buscar nombres de países en la afiliación
        for name in all_country_names:
            pattern = r'\b' + re.escape(name) + r'\b'
            if re.search(pattern, single_affiliation):
                found_countries.add(name)
        
        # Si no se encontró ningún país, devolver "NO COUNTRY"
        if not found_countries:
            return "NO COUNTRY"
        
        # Convertir conjunto a lista y unir con punto y coma
        return "; ".join(sorted(list(found_countries)))
    
    # Función para procesar múltiples afiliaciones separadas por punto y coma
    def process_multiple_affiliations(affiliations_text):
        if pd.isna(affiliations_text):
            return "NO COUNTRY"
        
        # Dividir por punto y coma para procesar cada afiliación individualmente
        affiliations_list = affiliations_text.split(';')
        
        # Procesar cada afiliación individualmente
        country_results = []
        for affiliation in affiliations_list:
            affiliation = affiliation.strip()
            # Incluso si la afiliación está vacía, procesarla para que se agregue "NO COUNTRY"
            country_result = extract_countries_from_single_affiliation(affiliation)
            country_results.append(country_result)
        
        # Unir los resultados con punto y coma, manteniendo la estructura original
        return "; ".join(country_results)
    
    # Aplicar la función a cada fila del DataFrame
    df['Country'] = df['Affiliation'].apply(process_multiple_affiliations)
    
    return df

# Ejemplo de uso
# def main():
#     # Ruta a los archivos (ajustar según sea necesario)
#     data_file = r"tests\files\wos_author_affiliation.csv"
#     country_codes_file = r"tests\files\country.csv"
    
#     # Cargar datos
#     try:
#         df = pd.read_csv(data_file)
#         df = fill_missing_affiliations(df)
#         # Verificar si existe la columna 'Affiliation'
#         if 'Affiliation' not in df.columns:
#             print(f"Error: El archivo {data_file} no contiene la columna 'Affiliation'")
#             return
        
#         # Procesar datos
#         result_df = extract_countries(df, country_codes_file)
        
#         # Guardar resultado
#         result_df.to_csv("affiliations_with_countries.csv", index=False)
#         print("Procesamiento completado. Resultado guardado en 'affiliations_with_countries.csv'")
        
#         # Mostrar algunas filas para verificar
#         print("\nPrimeras filas del resultado:")
#         print(result_df[['Affiliation', 'Country']].head())
        
#     except Exception as e:
#         print(f"Error al procesar el archivo: {e}")

# if __name__ == "__main__":
#     main()