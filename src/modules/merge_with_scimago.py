import pandas as pd 

def merge_with_scimago(scopus_df: pd.DataFrame,
                       scimago_df: pd.DataFrame) -> pd.DataFrame:
    """
    Left-join scopus_df (with a 'journal' column) to scimago_df on
    scopus_df['journal']==scimago_df['Title'], bringing in only the
    'journal_abbr' column.  Ensures no duplicate Titles in scimago.
    """
    # 1) ensure we have the right cols
    if 'journal' not in scopus_df.columns:
        raise ValueError("scopus_df must have a 'journal' column")
    if 'Title' not in scimago_df.columns or 'journal_abbr' not in scimago_df.columns:
        raise ValueError("scimago_df must have 'Title' and 'journal_abbr'")

    # 2) reduce Scimago to exactly one row per Title
    scimago_unique = (
        scimago_df[['Title','journal_abbr']]
        .drop_duplicates(subset='Title', keep='first')
    )

    # 3) merge
    merged = scopus_df.merge(
        scimago_unique,
        how='left',
        left_on='journal',
        right_on='Title'
    )

    # 4) drop the extra Title column
    merged = merged.drop(columns=['Title'])
    return merged
