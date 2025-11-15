import os
from itertools import combinations
from typing import Optional

import pandas as pd


GEPIH_NODE_COLS = [
    'PersonID',
    'AuthorFullName',
    'AuthorName',
    'Orcid',
    'OpenAlexAuthorID',
    'AuthorID',
    'ResearcherID',
    'Email',
]


def build_coauthor_network_for_gephi(
    all_dir: str,
    edge_out: Optional[str] = None,
    node_out: Optional[str] = None,
    id_col: str = 'PersonID',
    aggregate: bool = True,
    main_only: bool = False,
    min_weight: int = 1,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Build a co-author network from consolidated outputs and export CSVs consumable by Gephi.

    - Edges: undirected pairs (source=from, target=to). If aggregate=True, one row per pair with 'weight'.
      If aggregate=False, one row per article (SR) per pair; includes 'SR' and 'year'.
    - Nodes: authors with basic attributes (PersonID, names, IDs, email) from All_Authors.csv.

    Parameters
    - all_dir: directory containing All_* CSVs.
    - edge_out/node_out: optional explicit output paths. Defaults to CoauthorEdges.csv / CoauthorNodes.csv under all_dir.
    - id_col: identifier column in All_ArticleAuthor (default 'PersonID', fallback to 'AuthorID' if missing).
    - aggregate: whether to aggregate edges by pair (weight=count of shared articles).
    - main_only: if True, restrict to ismainarticle==TRUE articles.
    - min_weight: if aggregate=True, filter edges with weight < min_weight.

    Returns (edges_df, nodes_df)
    """

    aa_csv = os.path.join(all_dir, 'All_ArticleAuthor.csv')
    art_csv = os.path.join(all_dir, 'All_Articles.csv')
    au_csv = os.path.join(all_dir, 'All_Authors.csv')

    if not os.path.exists(aa_csv) or not os.path.exists(art_csv) or not os.path.exists(au_csv):
        raise FileNotFoundError('Missing required All_* CSVs in ' + all_dir)

    aa = pd.read_csv(aa_csv)
    art = pd.read_csv(art_csv)
    au = pd.read_csv(au_csv)

    # Choose identifier column
    if id_col not in aa.columns:
        if 'AuthorID' in aa.columns:
            id_col = 'AuthorID'
        else:
            raise ValueError(f'{id_col} not found and no AuthorID column available')

    # Optionally restrict to main articles only
    if main_only and 'ismainarticle' in art.columns:
        flag = art['ismainarticle']
        is_main = flag if str(flag.dtype) == 'bool' else (flag.astype(str).str.upper().eq('TRUE') | flag.astype(str).eq('1'))
        sr_main = set(art.loc[is_main, 'SR'].astype(str).tolist())
    else:
        sr_main = None

    # Build non-aggregated edges first
    # Map SR -> year
    art_map = art[['SR'] + [c for c in ['year'] if c in art.columns]].copy()
    art_map['SR'] = art_map['SR'].astype(str)

    aa['SR'] = aa['SR'].astype(str)
    aa[id_col] = aa[id_col].astype(str)

    edge_rows = []
    for sr, grp in aa.groupby('SR'):
        if sr_main is not None and sr not in sr_main:
            continue
        ids = sorted(set(grp[id_col].dropna().astype(str).tolist()))
        if len(ids) < 2:
            continue
        row_art = art_map.loc[art_map['SR'] == sr]
        year = int(row_art['year'].iloc[0]) if 'year' in row_art.columns and not row_art['year'].isna().all() else None
        for a, b in combinations(ids, 2):
            s, t = (a, b) if a <= b else (b, a)
            edge_rows.append({'source': s, 'target': t, 'year': year, 'SR': sr})

    edges_df = pd.DataFrame(edge_rows, columns=['source', 'target', 'year', 'SR'])

    # Aggregate if requested
    if aggregate and not edges_df.empty:
        agg = edges_df.groupby(['source', 'target'], as_index=False).size().rename(columns={'size': 'weight'})
        if min_weight > 1:
            agg = agg[agg['weight'] >= min_weight]
        agg['type'] = 'undirected'
        edges_out_df = agg
    else:
        edges_df['type'] = 'undirected'
        edges_out_df = edges_df

    # Nodes: from All_Authors; restrict to those present in edges
    if not edges_out_df.empty:
        node_ids = sorted(set(edges_out_df['source']).union(set(edges_out_df['target'])))
        nodes_df = au.copy()
        nodes_df[id_col] = nodes_df[id_col].astype(str)
        present_cols = [c for c in GEPIH_NODE_COLS if c in nodes_df.columns]
        nodes_df = nodes_df[present_cols]
        nodes_df = nodes_df[nodes_df[id_col].isin(node_ids)].drop_duplicates(subset=[id_col])
        # Gephi expects an id column named 'Id' or uses first column; keep PersonID as key
        # Ensure PersonID exists; if using AuthorID, rename accordingly for clarity
        if id_col != 'PersonID' and 'PersonID' in nodes_df.columns:
            # Reorder to keep id_col first
            cols = [id_col] + [c for c in nodes_df.columns if c != id_col]
            nodes_df = nodes_df[cols]
    else:
        nodes_df = pd.DataFrame(columns=GEPIH_NODE_COLS)

    # Write outputs
    if edge_out is None:
        edge_out = os.path.join(all_dir, 'CoauthorEdges.csv')
    if node_out is None:
        node_out = os.path.join(all_dir, 'CoauthorNodes.csv')
    edges_out_df.to_csv(edge_out, index=False)
    nodes_df.to_csv(node_out, index=False)

    return edges_out_df, nodes_df

