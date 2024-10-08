import pandas as pd
import re

# Ruta del archivo txt y la ruta de salida para el archivo Excel
ruta_txt = r'C:\Users\Steban Ibarra\Downloads\EM_1_500.txt'
ruta_excel = r'C:\Users\Steban Ibarra\Downloads\output_separated_columns_correct.xlsx'

# Inicializar un diccionario para almacenar los datos
data = {
    
    "AU": [], "AF": [], "CR": [], "AB": [], "AR": [], "BP": [], "C1": [], "C3": [], "CL": [], "CT": [], 
    "CY": [], "DA": [], "DE": [], "DI": [], "DT": [], "EA": [], "EF": [], "EI": [], "EM": [], "EP": [], 
    "ER": [], "FU": [], "FX": [], "GA": [], "HC": [], "HO": [], "HP": [], "ID": [], "IS": [], "J9": [], 
    "JI": [], "LA": [], "MA": [], "NR": [], "OA": [], "OI": [], "PA": [], "PD": [], "PG": [], "PI": [], 
    "PM": [], "PN": [], "PT": [], "PU": [], "PY": [], "RI": [], "RP": [], "SC": [], "SI": [], "SN": [], 
    "SO": [], "SP": [], "SU": [], "TC": [], "TI": [], "U1": [], "U2": [], "UT": [], "VL": [], "WC": [], 
    "WE": [], "Z9": [], "DB": [], "AU_UN": [], "AU1_UN": [], "AU_UN_NR": [], "SR_FULL_SR": [], "AU_CO": []

}

# Leer el archivo de texto
with open(ruta_txt, 'r', encoding='utf-8') as file:
    content = file.read()

# Separar los registros por 'ER' (fin de cada registro)
records = content.split('ER\n')

# Procesar cada registro y llenar los datos
for record in records:
    if record.strip():
        # Diccionario temporal para almacenar los valores actuales
        current_data = {key: '' for key in data.keys()}
        
        # Utilizar expresiones regulares para detectar las etiquetas de dos caracteres
        lines = record.split('\n')
        current_label = None
        for line in lines:
            # Si se encuentra una etiqueta nueva, actualizar la etiqueta actual
            match = re.match(r'^([A-Z]{2})\s+(.*)', line)
            if match:
                current_label, value = match.groups()
                if current_label in current_data:
                    current_data[current_label] += value
            elif current_label == 'CR':
                # Si es una línea que continúa en el campo de referencias, añadir la línea
                current_data[current_label] += f"; {line.strip()}"
        
        # Añadir el registro al diccionario de datos
        for key in data.keys():
            data[key].append(current_data[key])

# Crear el DataFrame con los datos
df = pd.DataFrame(data)

# Guardar el DataFrame en un archivo Excel
df.to_excel(ruta_excel, index=False)

print(f"Archivo Excel creado exitosamente en: {ruta_excel}")
