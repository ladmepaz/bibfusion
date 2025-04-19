import pandas as pd

def enrich_with_scimago(
    scopus_df_3: pd.DataFrame,
    scimago: pd.DataFrame
) -> pd.DataFrame:
    """
    Adds a 'Sourceid' column to scopus_df_3 by looking up in the scimago DataFrame,
    then returns only the columns ['SR','journal','abbreviated_source_title','issn','Sourceid'].

    Priority of lookup:
      1) scopus_df_3['issn'] == scimago['Issn']
      2) scopus_df_3['journal'] == scimago['Title']
    """
    df = scopus_df_3.copy()

    # --- Build unique lookup maps ---
    issn_map = (
        scimago[['Issn', 'Sourceid']]
        .dropna(subset=['Issn'])
        .drop_duplicates(subset=['Issn'])
        .set_index('Issn')['Sourceid']
    )
    title_map = (
        scimago[['Title', 'Sourceid']]
        .dropna(subset=['Title'])
        .drop_duplicates(subset=['Title'])
        .set_index('Title')['Sourceid']
    )

    # Step 1: match by ISSN
    df['Sourceid'] = df['issn'].map(issn_map)

    # Step 2: backfill missing via journal title
    missing = df['Sourceid'].isna()
    df.loc[missing, 'Sourceid'] = df.loc[missing, 'journal'].map(title_map)

    # Step 3: select only the desired columns
    return df[['SR', 'journal', 'abbreviated_source_title', 'issn', 'Sourceid']].copy()
