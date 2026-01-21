import re
import pandas as pd

def split_scopus_references(df: pd.DataFrame) -> pd.DataFrame:
    """
    Input: DataFrame with columns ['SR','references'] where 'references' is a
           semicolon-separated string of CR_refs.
    Output: DataFrame with columns ['SR','CR_ref'], one row per individual reference.
    """
    if 'SR' not in df.columns or 'references' not in df.columns:
        raise ValueError("Must have 'SR' and 'references' columns")
    rows = []
    for _, row in df.iterrows():
        sr = row['SR']
        text = str(row['references'])
        for ref in text.split(';'):
            ref = ref.strip().rstrip(',')
            if ref:
                rows.append({'SR': sr, 'CR_ref': ref})
    return pd.DataFrame(rows)

import re
import pandas as pd

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


# —— Usage example —— 
# scopus_references_1 = split_scopus_references(raw_scopus_df)
# scopus_references_2 = split_and_extract_year(scopus_references_1)

import re
import pandas as pd

def extract_venue(df: pd.DataFrame) -> pd.DataFrame:
    """
    From a DataFrame with a 'CR_ref_modified' column, produce:
      - CR_ref_modified_1: everything up to the *real* last comma
      - venue:             what follows that comma (unless it looks like pages
                          or starts with AND/OR)
    """
    if 'CR_ref_modified' not in df.columns:
        raise ValueError("DataFrame must have a 'CR_ref_modified' column")
    out = df.copy()

    # count commas
    comma_counts = out['CR_ref_modified'].str.count(',')

    # defaults
    out['CR_ref_modified_1'] = out['CR_ref_modified']
    out['venue']             = ''

    # candidate rows have at least two commas
    cand = comma_counts >= 2
    # split off the very last comma
    split_df = out.loc[cand, 'CR_ref_modified'].str.rsplit(',', n=1, expand=True)
    head = split_df[0].str.strip()
    tail = split_df[1].str.strip()

    # only accept tail as venue if
    # 1) it doesn't start with "AND" or "OR"
    # 2) it isn't a pages string (e.g. "PP.")
    ok = (~tail.str.match(r'^(AND|OR)\b', na=False)) & (~tail.str.match(r'^PP\.', na=False))

    # apply to rows that both are candidates and ok
    mask = cand & ok
    out.loc[mask, 'CR_ref_modified_1'] = head[mask]
    out.loc[mask, 'venue']             = tail[mask]

    return out

import pandas as pd

def split_authors_and_venue(df: pd.DataFrame,
                            source_col: str = 'CR_ref_modified_1'
                           ) -> pd.DataFrame:
    """
    Splits each row’s `source_col` into:
      - CR_ref_modified_2: the leading author segments
      - venue_1: whatever comes after the author list

    Strategy:
      1. Split on ', ' into pieces.
      2. Treat pieces with ≤3 words as “author” tokens.
      3. Everything from the first piece with >3 words onward is venue_1.
    """
    out = df.copy()
    authors_list = []
    venue_list = []

    for txt in out[source_col].fillna(''):
        parts = txt.split(', ')
        # accumulate segments until we hit one that looks “too long” (i.e. title)
        auth_seg = []
        i = 0
        for i, seg in enumerate(parts):
            if len(seg.split()) <= 3:
                auth_seg.append(seg)
            else:
                break
        # authors is whatever we collected
        authors = ', '.join(auth_seg).rstrip(', ')
        # venue is the rest
        venue = ', '.join(parts[i:]).lstrip(', ').strip()
        # edge case: if we never broke (all ≤3 words), then
        # authors = first segment, venue = everything after that
        if i == len(parts)-1 and len(parts) > 1:
            # e.g. ["BLUNCH NJ", "INTRODUCTION TO ..."]
            # our loop would take both as authors; fix:
            authors = parts[0]
            venue = ', '.join(parts[1:])
        authors_list.append(authors)
        venue_list.append(venue)

    out['CR_ref_modified_2'] = authors_list
    out['venue_1']            = venue_list
    return out

