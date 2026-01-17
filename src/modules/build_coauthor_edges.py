import os
from itertools import combinations
from typing import Optional

import pandas as pd


def build_coauthor_edges_links(all_dir: str, out_path: Optional[str] = None, id_col: str = 'PersonID') -> pd.DataFrame:
    """
    Build a simple list of coauthor links from consolidated outputs.

    - Nodes are authors keyed by `id_col` (default: PersonID).
    - For each article (SR), generate undirected pairs (from,to).
    - Attach article-level metadata: year, SR, openalex_work_id.
    - No aggregation: one row per pair per SR.

    Inputs (from `all_dir`):
      - All_ArticleAuthor.csv: columns include SR, PersonID (or AuthorID), AuthorOrder
      - All_Articles.csv: columns include SR, year, openalex_work_id

    Output:
      - DataFrame with columns: from, to, year, SR, openalex_work_id
      - If `out_path` is provided (or default), the DataFrame is also written to CSV.
    """

    aa_csv = os.path.join(all_dir, 'All_ArticleAuthor.csv')
    art_csv = os.path.join(all_dir, 'All_Articles.csv')
    if not os.path.exists(aa_csv):
        raise FileNotFoundError(f'All_ArticleAuthor.csv not found in {all_dir}')
    if not os.path.exists(art_csv):
        raise FileNotFoundError(f'All_Articles.csv not found in {all_dir}')

    aa = pd.read_csv(aa_csv)
    art = pd.read_csv(art_csv)

    # Choose identifier column
    if id_col not in aa.columns:
        # fallback to AuthorID
        if 'AuthorID' in aa.columns:
            id_col = 'AuthorID'
        else:
            raise ValueError(f'{id_col} not found and no AuthorID column available')

    # Map SR -> year, openalex_work_id
    art_map = art[['SR'] + [c for c in ['year', 'openalex_work_id'] if c in art.columns]].copy()
    # Ensure types are string-ish for safe merges
    art_map['SR'] = art_map['SR'].astype(str)

    # Prepare output rows
    rows = []

    # Group by article and create pairs
    if 'SR' not in aa.columns:
        raise ValueError('All_ArticleAuthor.csv missing SR column')
    aa['SR'] = aa['SR'].astype(str)
    aa[id_col] = aa[id_col].astype(str)

    # Unique authors per SR (avoid duplicates on same article)
    for sr, grp in aa.groupby('SR'):
        authors = sorted(set(grp[id_col].dropna().astype(str).tolist()))
        if len(authors) < 2:
            continue
        # Get article metadata
        meta = art_map.loc[art_map['SR'] == sr]
        year = int(meta['year'].iloc[0]) if ('year' in meta.columns and not meta['year'].isna().all()) else None
        oaw = meta['openalex_work_id'].iloc[0] if 'openalex_work_id' in meta.columns and not meta['openalex_work_id'].isna().all() else None

        for a, b in combinations(authors, 2):
            f, t = (a, b) if a <= b else (b, a)
            rows.append({'from': f, 'to': t, 'year': year, 'SR': sr, 'openalex_work_id': oaw})

    df = pd.DataFrame(rows, columns=['from', 'to', 'year', 'SR', 'openalex_work_id'])

    if out_path is None:
        out_path = os.path.join(all_dir, 'CoauthorEdges_Links.csv')
    # Write CSV
    df.to_csv(out_path, index=False)
    return df

