import pandas as pd
import re

def extract_journal(df: pd.DataFrame) -> pd.DataFrame:
    """
    From a DataFrame with:
      - 'CR_ref_modified'  (authors + title + journal, comma-separated)
    Produce:
      - 'journal'         (the substring after the final comma)
    """
    if 'CR_ref_modified' not in df.columns:
        raise ValueError("DataFrame must have a 'CR_ref_modified' column")
    
    out = df.copy()
    
    def _get_journal(ref: str) -> str:
        if pd.isna(ref):
            return ''
        parts = ref.rsplit(',', 1)
        return parts[-1].strip() if len(parts) > 1 else ''
    
    out['journal'] = out['CR_ref_modified'].apply(_get_journal)
    return out
