import pandas as pd

def remove_duplicates_df(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicate rows from a DataFrame based on the 'doi' and 'title' columns,
    while preserving rows where both values are missing.

    Parameters
    ----------
    dataframe : pd.DataFrame
        The DataFrame to process. It must contain the columns 'doi' and 'title'.

    Returns
    -------
    pd.DataFrame
        A DataFrame with duplicates removed based on non-null combinations of 'doi' and 'title'.
        Rows where both 'doi' and 'title' are missing are preserved.

    Raises
    ------
    ValueError
        If the DataFrame does not contain the required 'doi' or 'title' columns.

    Notes
    -----
    - Rows with at least one of 'doi' or 'title' are considered for duplicate removal.
    - Rows where both 'doi' and 'title' are missing are not deduplicated and are kept as-is.

    Example
    -------
    >>> df = pd.DataFrame({
    ...     'doi': ['10.1234', '10.1234', None],
    ...     'title': ['Title A', 'Title A', None]
    ... })
    >>> clean_df = remove_duplicates_df(df)
    >>> print(clean_df)
    """

    # Confirm that the 'doi' and 'title' columns exist
    if 'doi' not in dataframe.columns or 'title' not in dataframe.columns:
        raise ValueError("The DataFrame doesn't contain the columns 'DI' or 'title'.")

    # Filter (DataFrame without null values)
    df_cleaned = dataframe[
        dataframe['doi'].notna() | dataframe['title'].notna()  # Delete rows without values.
    ].drop_duplicates(
        subset=['doi', 'title'],
        keep='first'
    )

    # DataFrame with null values
    df_empty = dataframe[
        dataframe['doi'].isna() & dataframe['title'].isna()
    ]
    duplicates_removed = len(dataframe) - (len(df_cleaned) + len(df_empty))

    df_final = pd.concat([df_cleaned, df_empty])

    # Return the cleaned DataFrame
    return df_final, duplicates_removed