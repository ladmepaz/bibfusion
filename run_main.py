import sys

# Añadimos la carpeta 'src' al path para poder importar main
sys.path.append('src')

import main

main.preprocesing_df(
    path_wos=[r'C:\Users\User\OneDrive\Documentos\Preprocessing\preprocessing_4\preprocessing\WoS_90_22NOV25.txt'],
    path_scopus=r'C:\Users\User\OneDrive\Documentos\Preprocessing\preprocessing_4\preprocessing\scopus_294_22NOV25.csv'
)
