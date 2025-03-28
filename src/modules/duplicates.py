import pandas as pd

def remove_duplicates_df(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicate rows based on DOI ('DI') and Title ('TI') columns from a DataFrame,
    while preserving rows where either DOI or Title cells are empty.

    Parameters
    ----------
    dataframe : pd.DataFrame
        The DataFrame to process.

    Returns
    -------
    pd.DataFrame
        A DataFrame with duplicates removed based on non-empty DOI and Title columns.
        Rows with empty DOI and Title cells are preserved.

    Raises
    ------
    ValueError
        If the DataFrame does not contain the required 'DI' or 'TI' columns.

    Notes
    -----
    The function first removes duplicates only where DOI ('DI') or Title ('TI') are not empty,
    then combines these rows with rows where both DOI and Title are empty to preserve them.

    Example
    -------
    >>> df = pd.DataFrame({'DI': ['10.1234', '10.5678', None], 'TI': ['Title1', 'Title2', None]})
    >>> cleaned_df = remove_duplicates_df(df)
    >>> print(cleaned_df)
    """

    # Confirmar que existe la columna DI o title
    if 'doi' not in dataframe.columns or 'title' not in dataframe.columns:
        raise ValueError("The DataFrame doesn't contain the columns 'DI' or 'title'.")

    # Filtro (Dataframe sin valores nulos)
    df_cleaned = dataframe[
        dataframe['doi'].notna() | dataframe['title'].notna()  # Eliminar las celdas sin valores.
    ].drop_duplicates(
        subset=['doi', 'title'],
        keep='first'
    )

    # Dataframe con valores nulos
    df_empty = dataframe[
        dataframe['doi'].isna() & dataframe['title'].isna()
    ]

    df_final = pd.concat([df_cleaned, df_empty])

    # Retornar el DataFrame limpio
    return df_final