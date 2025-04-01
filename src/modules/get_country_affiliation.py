import pandas as pd
import re
import os

def extract_countries(df, country_codes_file):
    """
    Extrae países de la columna 'Affiliation' y crea una nueva columna 'Country' 
    con los países en mayúsculas separados por punto y coma.
    Mantiene los países duplicados si aparecen varias veces.
    
    Args:
        df (pandas.DataFrame): DataFrame con la columna 'Affiliation'
        country_codes_file (str): Ruta al archivo CSV con códigos de países
        
    Returns:
        pandas.DataFrame: DataFrame con la nueva columna 'Country'
    """
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
    fill_missing_affiliations(df)
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
        
        # Diccionario para mapear nombres a sí mismos (para normalización)
        name_to_name = {name: name for name in all_country_names}
        
        # Combinar ambos diccionarios para tener un único mapeo
        country_map = {**code_to_name, **name_to_name}
    else:
        print(f"El archivo {country_codes_file} no existe. No se realizará la extracción de países.")
        return df
    
    # Función para extraer países de una afiliación
    def extract_countries_from_affiliation(affiliation):
        if pd.isna(affiliation) or affiliation.strip().upper() == 'NO AFFILIATION':
            return ""
        
        # Convertir a mayúsculas
        affiliation = affiliation.upper()
        
        # Lista para almacenar países encontrados (manteniendo duplicados)
        found_countries = []
        
        # Buscar códigos de países en la afiliación
        for code in all_country_codes:
            pattern = r'\b' + re.escape(code) + r'\b'
            matches = re.findall(pattern, affiliation)
            # Agregar el país por cada coincidencia encontrada
            for _ in range(len(matches)):
                found_countries.append(country_map[code])
        
        # Buscar nombres de países en la afiliación
        for name in all_country_names:
            pattern = r'\b' + re.escape(name) + r'\b'
            matches = re.findall(pattern, affiliation)
            # Agregar el país por cada coincidencia encontrada
            for _ in range(len(matches)):
                found_countries.append(name)
        
        # Unir con punto y coma, manteniendo todas las ocurrencias
        return "; ".join(found_countries) if found_countries else ""
    
    # Aplicar la función a cada fila del DataFrame
    df['Country'] = df['Affiliation'].apply(extract_countries_from_affiliation)
    
    return df

# Ejemplo de uso
def main():
    # Ruta a los archivos (ajustar según sea necesario)
    data_file = r"tests\files\wos_author_affiliation.csv"
    country_codes_file = r"tests\files\country.csv"
    
    # Cargar datos
    try:
        df = pd.read_csv(data_file)
        
        # Verificar si existe la columna 'Affiliation'
        if 'Affiliation' not in df.columns:
            print(f"Error: El archivo {data_file} no contiene la columna 'Affiliation'")
            return
        
        # Procesar datos
        result_df = extract_countries(df, country_codes_file)
        
        # Guardar resultado
        result_df.to_csv("affiliations_with_countries.csv", index=False)
        print("Procesamiento completado. Resultado guardado en 'affiliations_with_countries.csv'")
        
        # Mostrar algunas filas para verificar
        print("\nPrimeras filas del resultado:")
        print(result_df[['Affiliation', 'Country']].head())
        
    except Exception as e:
        print(f"Error al procesar el archivo: {e}")

if __name__ == "__main__":
    main()