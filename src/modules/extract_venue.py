import re
import pandas as pd

def extract_venue(df: pd.DataFrame) -> pd.DataFrame:
    """
    From a DataFrame with a 'CR_ref_modified' column, produce:
      - CR_ref_modified_1: the reference up through the author/title split
      - venue:             the trailing journal or book title (even if it contains commas)
    """
    if 'CR_ref_modified' not in df.columns:
        raise ValueError("DataFrame must have a 'CR_ref_modified' column")
    out = df.copy()

    # count commas in each ref
    comma_counts = out['CR_ref_modified'].str.count(',')

    # initialize new columns
    out['CR_ref_modified_1'] = out['CR_ref_modified']
    out['venue']             = ''

    # split on final comma
    split_last = out['CR_ref_modified'].str.rsplit(',', n=1, expand=True)
    head_last = split_last[0].str.strip()
    tail_last = split_last[1].str.strip()

    # page-only tails look like "PP." or all digits
    page_only = tail_last.str.match(r'^\s*(PP\.|\d+)', na=False)
    ok1 = ~page_only

    # apply when there's at least one comma and the tail isn't just pages
    mask1 = comma_counts >= 1
    out.loc[mask1 & ok1, 'CR_ref_modified_1'] = head_last[mask1 & ok1]
    out.loc[mask1 & ok1, 'venue']             = tail_last[mask1 & ok1]

    # fallback: if still no venue but ≥2 commas, split on last two commas
    need_fb = (out['venue'] == '') & (comma_counts >= 2)
    for idx in out[need_fb].index:
        cr = out.at[idx, 'CR_ref_modified']
        parts = cr.rsplit(',', 2)
        if len(parts) == 3:
            head, seg1, seg2 = parts
            candidate = f"{seg1.strip()}, {seg2.strip()}"
            # sanity check: require at least 3 words in candidate
            if len(candidate.split()) >= 3:
                out.at[idx, 'CR_ref_modified_1'] = head.strip()
                out.at[idx, 'venue']            = candidate

    return out
