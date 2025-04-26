import pandas as pd

def split_scopus_references(df: pd.DataFrame) -> pd.DataFrame:
    if 'SR' not in df.columns or 'references' not in df.columns:
        raise ValueError("The DataFrame must contain 'SR' and 'references' columns")

    # 1) explode the semicolon‐separated list into one row per CR_ref
    rows = []
    for _, row in df.iterrows():
        sr = row['SR']
        for ref in str(row['references']).split(';'):
            ref = ref.strip()
            if ref:
                rows.append({'SR': sr, 'CR_ref': ref})
    out = pd.DataFrame(rows)

    # 2) keep only those with a (YYYY) year, drop bare “(YYYY)”
    out = out[out['CR_ref'].str.contains(r'\(\d{4}\)')]
    out = out[~out['CR_ref'].str.match(r'^\(\d{4}\)$')]

    # 3) identify articles via multiple regex passes
    article_pat = r'\bJOURNAL\b|PP\.|,\s*\d+,\s*\d+,\s*\(\d{4}\)'
    out['type'] = out['CR_ref'].str.contains(article_pat, regex=True).map(lambda x: 'article' if x else '')

    # 4) catch “, volume, (year)” patterns
    mask_vol_only = (out['type'] == '') & out['CR_ref'].str.contains(r',\s*\d+,\s*\(\d{4}\)$')
    out.loc[mask_vol_only, 'type'] = 'article'

    # 5) catch “, JournalName, (year)” endings
    mask_journal_end = (
        (out['type'] == '') &
        out['CR_ref'].str.contains(r',\s*[^,]+,\s*\(\d{4}\)$')
    )
    out.loc[mask_journal_end, 'type'] = 'article'

    # 6) filter to only articles, then drop the helper column
    out = out[out['type'] == 'article'].drop(columns='type').reset_index(drop=True)

    return out
