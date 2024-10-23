import pandas as pd

def remove_duplicates(file_path: str, search: int = 1) -> pd.DataFrame:
    """
    Remove duplicate rows based on DOI ('DI') and Title ('TI') columns from an Excel file,
    while preserving rows where either DOI or Title cells are empty.

    Parameters
    ----------
    file_path : str
        The file path of the Excel file to read and process.
    search : int (default value = 1)
        Select the method for search the file. The default value is 1 (it means that the file,
        is storaged locally.) But if search = 2, the search is going to be through web-scrapping.

    Returns
    -------
    pd.DataFrame
        A DataFrame with duplicates removed based on non-empty DOI and Title columns.
        Rows with empty DOI and Title cells are preserved.

    Raises
    ------
    ValueError
        If the Excel file does not contain the required 'DI' or 'TI' columns.

    Notes
    -----
    The function first removes duplicates only where DOI ('DI') or Title ('TI') are not empty,
    then combines these rows with rows where both DOI and Title are empty to preserve them.
    The cleaned DataFrame is saved as a new Excel file named 'cleaned_EM_407_DF.xlsx'.

    Example
    -------
    >>> file_path = '/path/to/your/excel_file.xlsx'
    >>> cleaned_df = remove_duplicates(file_path)
    >>> print(cleaned_df)
    """

    if (search == 1):
        # Leer el excel principal
        df = pd.read_excel(file_path)
    elif (search == 2):
        # Leer excel desde clave
        df = pd.read_csv(f'https://docs.google.com/spreadsheets/d/{file_path}/export?format=csv&usp=sharing')

    # Confirmar que existe la columna DI o TI
    if 'DI' not in df.columns or 'TI' not in df.columns:
        raise ValueError("The file doesn't contain the columns 'DI' or 'TI'.")

    # Filtro (Dataframe sin valores nulos)
    df_cleaned = df[
        df['DI'].notna() | df['TI'].notna()  # Eliminar las celdas sin valores.
    ].drop_duplicates(
        subset=['DI', 'TI'],
        keep='first'
    )

    # Dataframe con valores nulos
    df_empty = df[
        df['DI'].isna() & df['TI'].isna()
    ]

    df_final = pd.concat([df_cleaned, df_empty])

    # Exportar
    return df_final
