import re
import pandas as pd

def extract_title(df: pd.DataFrame) -> pd.DataFrame:
    """
    From a DataFrame with:
      - 'CR_ref_modified_1' (authors + title, comma-separated)
      - 'venue'            (journal or other venue)

    Produce:
      - 'authors'          (comma-separated author list)
      - 'title'            (the remainder after the authors)
      - rename 'venue' -> 'journal_title'

    Now also removes any leading author blocks like
      'DANA L-P., SMALL BUSINESS...' → 'SMALL BUSINESS...'
      'DANA L.-P., RAMADANI V., ...'   → '...'
    """
    required = {'CR_ref_modified_1', 'venue'}
    if not required.issubset(df.columns):
        raise ValueError(f"DataFrame must have columns: {required}")

    # 1) Try to split off a canonical AUTHOR+INITIALS block
    author_title_pat = re.compile(
        r'^((?:'                                # group1 = one or more authors
          r'[A-Z][A-Za-z\'’\-]*'               #   surname
          r'(?:\s+[A-Z][A-Za-z\'’\-]*)*'       #   optional extra surname words
          r'\s+'                               #   space
          r'[A-Z][A-Z\.\-]*'                   #   initials (allow dots & hyphens)
          r',\s*'                              #   comma + spaces
        r')+)'                                 # repeat authors block
        r'(.*)$'                               # group2 = rest → title
    )

    # 2) A fallback stripper that will remove *any* leading AUTHOR+INITIALS,
    #    even if mixed, hyphenated, etc.
    strip_leading_authors = re.compile(
        r'^(?:'
          r'[A-Z][A-Za-z\'’\-]*'               # surname
          r'(?:\s+[A-Z][A-Za-z\'’\-]*)*'       # optional extra surname words
          r'\s+'                               # space
          r'[A-Z][A-Z\.\-]*'                   # initials
          r',\s*'                              # comma + spaces
        r')+'                                  # one or more times
    )

    out = df.copy()
    out['authors'] = ''
    out['title']   = ''

    # First pass: extract authors vs. title
    for idx, row in out.iterrows():
        text = row['CR_ref_modified_1'].strip()
        m = author_title_pat.match(text)
        if m:
            # Group1 ends in comma, so strip and split
            auth_block = m.group(1).rstrip(', ')
            auth_list  = [a.strip() for a in auth_block.split(',') if a.strip()]
            title      = m.group(2).strip()
        else:
            auth_list = []
            title     = text

        out.at[idx, 'authors'] = ', '.join(auth_list)
        out.at[idx, 'title']   = title

    # Second pass: strip *any* leftover leading author blocks from 'title'
    out['title'] = (
        out['title']
          .str.replace(strip_leading_authors, '', regex=True)
          .str.strip()
    )

    # Finally rename 'venue' → 'journal_title'
    return out.rename(columns={'venue': 'journal_title'})
