import pandas as pd

def add_SR_ref(scopus_ref_5: pd.DataFrame) -> pd.DataFrame:
    """
    Given a DataFrame with columns
      ['SR','CR_ref','year','CR_ref_modified','author_first','journal','journal_abbr']
    this returns a copy with:
      - any row with missing/empty 'journal_abbr' removed
      - a new column 'SR_ref' = "author_first, year, journal_abbr"
    """
    df = scopus_ref_5.copy()

    # filter out rows where journal_abbr is NaN or blank
    mask = df['journal_abbr'].notna() & (df['journal_abbr'].astype(str).str.strip() != '')
    df = df.loc[mask].copy()

    # build SR_ref
    df['SR_ref'] = (
        df['author_first'].astype(str).str.strip()
        + ', '
        + df['year'].astype(str)
        + ', '
        + df['journal_abbr'].astype(str).str.strip()
    )

    return df
