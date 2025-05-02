import pandas as pd
import re

def split_and_extract_year(df: pd.DataFrame) -> pd.DataFrame:
    """
    Input: DataFrame with ['SR','CR_ref'].
    Output: DataFrame with ['SR','CR_ref','year','CR_ref_modified']:

      - year: int extracted from the trailing '(YYYY)'
      - CR_ref_modified: CR_ref with trailing '(YYYY)' and any 
                         volume/issue/pages (including PP.xxx) stripped.
    Rows without a valid '(YYYY)' are dropped.
    """
    year_pat = re.compile(r'\s*\((\d{4})\)\s*$')
    # now also catches ", PP. 12-34" or ", 10" or ", 10.5" etc:
    drop_pattern = re.compile(
        r',\s*(?:PP\.\s*\d+(?:-\d+)?|\d+(?:\.\d+)?)(?:.*)$',
        flags=re.IGNORECASE
    )

    records = []
    for _, row in df.iterrows():
        cr = row['CR_ref']
        # 1) pull out the year
        m = year_pat.search(cr)
        if not m:
            continue
        year = int(m.group(1))

        # 2) strip the "(YYYY)"
        without_year = year_pat.sub('', cr).rstrip(', ').strip()

        # 3) drop volume/issue/pages including PP.xx
        modified = drop_pattern.sub('', without_year).rstrip(', ').strip()

        records.append({
            'SR':               row['SR'],
            'CR_ref':           cr,
            'year':             year,
            'CR_ref_modified':  modified
        })

    return pd.DataFrame.from_records(records)
