import pandas as pd

def merge_tos_with_articles(df_tos, df_article):
    """
    Merges df_tos and df_article using the SR column.

    Parameters:
        df_tos (pd.DataFrame): Contains columns ['SR', 'tos']
        df_article (pd.DataFrame): Contains columns ['SR', 'author', 'title', 'year', 'doi']

    Returns:
        pd.DataFrame: Merged DataFrame with columns:
            ['SR', 'tos', 'author', 'title', 'year', 'doi']
    """
    # Perform a merge ensuring it is a left join (keep all SR from df_tos)
    df_result = pd.merge(
        df_tos,
        df_article[['SR', 'author', 'title', 'year', 'doi', 'author_keywords', 'abstract']],
        on='SR',
        how='left'
    )
    # Convert year to integer if not NaN
    if 'year' in df_result.columns:
        df_result['year'] = pd.to_numeric(df_result['year'], errors='coerce')
        df_result['year'] = df_result['year'].astype('Int64')  # Integer but allows NaN
    
    df_result["doi"] = df_result["doi"].astype(str).str.strip()  # remove leading/trailing spaces
    df_result["doi"] = df_result["doi"].str.replace(r"\s+", " ", regex=True)  # normalize internal spaces
    df_result["doi"] = df_result["doi"].str.upper()  # optional: unify to uppercase
    df_result = df_result.drop_duplicates(subset=["doi"], keep="first")
    
    return df_result
