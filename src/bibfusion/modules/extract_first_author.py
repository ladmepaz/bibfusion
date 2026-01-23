import pandas as pd
import re

def extract_first_author(df: pd.DataFrame) -> pd.DataFrame:
    """
    From a DataFrame with:
      - 'CR_ref'   (e.g. "AL-KHAWALDAH RA, AL-ZOUBI WK, ...")
    Produce:
      - 'author_first'  (e.g. "AL-KHAWALDAH RA")
    """
    if 'CR_ref' not in df.columns:
        raise ValueError("DataFrame must have a 'CR_ref' column")
        
    out = df.copy()
    def _first(cr: str) -> str:
        if not isinstance(cr, str) or not cr.strip():
            return ""
        return cr.split(',', 1)[0].strip()
        
    out['author_first'] = out['CR_ref'].apply(_first)
    return out
