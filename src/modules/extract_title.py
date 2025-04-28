import re
import pandas as pd

def extract_title(df: pd.DataFrame) -> pd.DataFrame:
    """
    From a DataFrame with:
      - 'CR_ref_modified_1' (authors + title)
      - 'venue'            (journal or other venue)
    Produce:
      - 'authors'          (comma-separated author list)
      - 'title'            (the remainder after the authors)
      - rename 'venue' -> 'journal_title'
    """
    required = {'CR_ref_modified_1', 'venue'}
    if not required.issubset(df.columns):
        raise ValueError(f"DataFrame must have columns: {required}")

    out = df.copy()

    # matches one-or-more ALL-CAP surname words (allowing ' or -),
    # plus a trailing initials block of caps/dots/hyphens/spaces
    author_pat = re.compile(
        r"^[A-Z][A-Z'’\-]*(?:\s+[A-Z][A-Z'’\-]*)*\s+[A-Z][A-Z\.\-\s]*$"
    )

    out['authors'] = ''
    out['title']   = ''

    for idx, row in out.iterrows():
        text  = row['CR_ref_modified_1']
        parts = [p.strip() for p in text.split(',')]
        auths = []
        title = ''

        for i, token in enumerate(parts):
            if author_pat.match(token):
                auths.append(token)
            else:
                title = ', '.join(parts[i:]).strip()
                break

        out.at[idx, 'authors'] = ', '.join(auths)
        out.at[idx, 'title']   = title

    # finally rename venue → journal_title
    out = out.rename(columns={'venue': 'journal_title'})

    return out
