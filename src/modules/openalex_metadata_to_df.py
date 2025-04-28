import pandas as pd

def openalex_metadata_to_df(meta: dict) -> pd.DataFrame:
    """
    Given the dict returned by get_openalex_metadata, build a one-row DataFrame
    with exactly these seven columns:
      - openalex_id
      - doi
      - authors        (list of author‐dicts)
      - abstract
      - keywords      (list of strings)
      - journal
      - source        (dict of source‐fields)
    """
    if meta is None:
        # no match
        return pd.DataFrame(columns=[
            "openalex_id","doi","authors","abstract","keywords","journal","source"
        ])
    # wrap in a list so DataFrame has exactly one row
    return pd.DataFrame([{
        "openalex_id": meta.get("openalex_id"),
        "doi":          meta.get("doi"),
        "authors":      meta.get("authors"),
        "abstract":     meta.get("abstract"),
        "keywords":     meta.get("keywords"),
        "journal":      meta.get("journal"),
        "source":       meta.get("source"),
    }])
