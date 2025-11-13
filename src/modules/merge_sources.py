import re
import unicodedata
from typing import Dict, Tuple, List

import pandas as pd
from rapidfuzz import fuzz, process


def _ascii_upper(text: str) -> str:
    if not isinstance(text, str):
        return ''
    norm = unicodedata.normalize('NFKD', str(text))
    stripped = ''.join(ch for ch in norm if not unicodedata.combining(ch))
    return stripped.upper().strip()


def normalize_doi(doi: str) -> str:
    if not isinstance(doi, str):
        return ''
    s = doi.strip().lower()
    if not s:
        return ''
    if s.startswith('https://doi.org/'):
        s = s.replace('https://doi.org/', '')
    # drop obvious suffixes
    s = re.sub(r"/(full|pdf|html).*$", '', s)
    s = s.strip().strip('.')
    return s


def _best_row(rows: List[pd.Series]) -> pd.Series:
    # choose the row with most non-empty across prioritized fields
    pri = ['abstract', 'orcid', 'openalex_work_id', 'author_full_names', 'journal', 'issn', 'eissn']
    def score(r):
        return sum(1 for c in pri if c in r.index and pd.notna(r[c]) and str(r[c]).strip() != '')
    rows_sorted = sorted(rows, key=score, reverse=True)
    return rows_sorted[0]


def merge_articles(wos_article: pd.DataFrame, scopus_article: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, str]]:
    a = wos_article.copy(); a['__source'] = 'wos'
    b = scopus_article.copy(); b['__source'] = 'scopus'
    for df in (a, b):
        df['__doi_norm'] = df.get('doi', '').apply(normalize_doi)
        df['__title_key'] = df.get('title', '').apply(_ascii_upper)
        df['__year'] = pd.to_numeric(df.get('year', ''), errors='coerce').astype('Int64')

    combined = pd.concat([a, b], ignore_index=True, sort=False)

    # Group with DOI
    with_doi = combined[combined['__doi_norm'] != '']
    groups = []
    doi_to_primary_sr: Dict[str, str] = {}
    for doi, g in with_doi.groupby('__doi_norm'):
        chosen = _best_row([r for _, r in g.iterrows()])
        # sources_merged
        sources = ';'.join(sorted(g['__source'].unique()))
        chosen = chosen.copy()
        chosen['sources_merged'] = sources
        groups.append(chosen)
        # primary SR for this DOI
        doi_to_primary_sr[doi] = chosen.get('SR', '')

    merged_with_doi = pd.DataFrame(groups) if groups else with_doi.copy()

    # Without DOI: dedupe by title+year using fuzzy match within same title_key
    without_doi = combined[combined['__doi_norm'] == '']
    used = set()
    rows_no_doi = []
    for _, grp in without_doi.groupby(['__title_key', '__year']):
        if len(grp) == 1:
            r = grp.iloc[0].copy()
            r['sources_merged'] = r['__source']
            rows_no_doi.append(r)
            continue
        titles = grp['title'].astype(str).tolist()
        # choose the best row
        chosen = _best_row([r for _, r in grp.iterrows()])
        chosen = chosen.copy()
        chosen['sources_merged'] = ';'.join(sorted(grp['__source'].unique()))
        rows_no_doi.append(chosen)

    merged_no_doi = pd.DataFrame(rows_no_doi) if rows_no_doi else without_doi.copy()

    merged = pd.concat([merged_with_doi, merged_no_doi], ignore_index=True, sort=False)

    # Clean helper cols
    merged = merged.drop(columns=['__doi_norm', '__title_key', '__year', '__source'], errors='ignore')

    return merged, doi_to_primary_sr


def merge_authors(
    wos_author: pd.DataFrame,
    scopus_author: pd.DataFrame,
    wos_article: pd.DataFrame,
    scopus_article: pd.DataFrame,
    wos_articleauthor: pd.DataFrame,
    scopus_articleauthor: pd.DataFrame,
    doi_to_primary_sr: Dict[str, str]
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    # Build SR -> doi map for each source
    def sr_to_doi(df: pd.DataFrame) -> Dict[str, str]:
        m = {}
        if 'SR' in df.columns:
            tmp = df[['SR', 'doi']].copy()
            tmp['__doi_norm'] = tmp.get('doi', '').apply(normalize_doi)
            for _, r in tmp.iterrows():
                m[str(r['SR'])] = r['__doi_norm']
        return m
    map_wos = sr_to_doi(wos_article)
    map_scopus = sr_to_doi(scopus_article)

    # Remap SR to primary SR using DOI where available
    def remap_articleauthor(df: pd.DataFrame, sr_map: Dict[str, str]) -> pd.DataFrame:
        df2 = df.copy()
        df2['SR'] = df2['SR'].astype(str)
        def map_sr(sr):
            doi = sr_map.get(sr, '')
            if doi and doi in doi_to_primary_sr:
                return doi_to_primary_sr[doi] or sr
            return sr
        df2['SR'] = df2['SR'].map(map_sr)
        return df2

    aa_w = remap_articleauthor(wos_articleauthor, map_wos)
    aa_s = remap_articleauthor(scopus_articleauthor, map_scopus)
    all_aa = pd.concat([aa_w, aa_s], ignore_index=True, sort=False)

    # Prefer PersonID if present; otherwise keep as-is
    # Deduplicate by SR + PersonID + AuthorOrder
    if 'PersonID' in all_aa.columns:
        subset = ['SR', 'PersonID', 'AuthorOrder']
    else:
        subset = ['SR', 'AuthorID', 'AuthorOrder']
    all_aa = all_aa.drop_duplicates(subset=subset)

    # Merge authors: prefer unique PersonID
    au = pd.concat([wos_author, scopus_author], ignore_index=True, sort=False)
    if 'PersonID' in au.columns:
        au = au.sort_values(by=['PersonID','Orcid'], ascending=[True, False])
        au = au.drop_duplicates(subset=['PersonID'])
    else:
        # fallback
        au = au.drop_duplicates(subset=['AuthorID'])

    return au, all_aa


def _sr_to_doi(df: pd.DataFrame) -> Dict[str, str]:
    m = {}
    if df is None or df.empty:
        return m
    if 'SR' in df.columns:
        tmp = df[['SR', 'doi']].copy()
        tmp['__doi_norm'] = tmp.get('doi', '').apply(normalize_doi)
        for _, r in tmp.iterrows():
            m[str(r['SR'])] = r['__doi_norm']
    return m


def merge_citation(
    wos_citation: pd.DataFrame,
    scopus_citation: pd.DataFrame,
    wos_article: pd.DataFrame,
    scopus_article: pd.DataFrame,
    doi_to_primary_sr: Dict[str, str]
) -> pd.DataFrame:
    def remap_sr(df: pd.DataFrame, map_sr_doi: Dict[str, str]) -> pd.DataFrame:
        if df is None or df.empty:
            return df
        out = df.copy()
        if 'SR' in out.columns:
            out['SR'] = out['SR'].astype(str)
            def map_sr(sr):
                doi = map_sr_doi.get(sr, '')
                if doi and doi in doi_to_primary_sr:
                    return doi_to_primary_sr[doi] or sr
                return sr
            out['SR'] = out['SR'].map(map_sr)
        return out

    map_w = _sr_to_doi(wos_article)
    map_s = _sr_to_doi(scopus_article)
    w = remap_sr(wos_citation, map_w)
    s = remap_sr(scopus_citation, map_s)
    combined = pd.concat([x for x in [w, s] if x is not None], ignore_index=True, sort=False)
    combined = combined.drop_duplicates()
    return combined


def merge_affiliation(
    wos_aff: pd.DataFrame,
    scopus_aff: pd.DataFrame,
    wos_article: pd.DataFrame,
    scopus_article: pd.DataFrame,
    doi_to_primary_sr: Dict[str, str]
) -> pd.DataFrame:
    map_w = _sr_to_doi(wos_article)
    map_s = _sr_to_doi(scopus_article)

    def remap(df: pd.DataFrame, sr_map: Dict[str, str]) -> pd.DataFrame:
        if df is None or df.empty:
            return df
        out = df.copy()
        if 'SR' in out.columns:
            out['SR'] = out['SR'].astype(str)
            def map_sr(sr):
                doi = sr_map.get(sr, '')
                if doi and doi in doi_to_primary_sr:
                    return doi_to_primary_sr[doi] or sr
                return sr
            out['SR'] = out['SR'].map(map_sr)
        return out

    w = remap(wos_aff, map_w)
    s = remap(scopus_aff, map_s)
    combined = pd.concat([x for x in [w, s] if x is not None], ignore_index=True, sort=False)
    # prefer PersonID dedupe if exists
    if 'PersonID' in combined.columns:
        subset = [c for c in ['SR', 'PersonID', 'Affiliation'] if c in combined.columns]
    else:
        subset = [c for c in ['SR', 'AuthorID', 'Affiliation'] if c in combined.columns]
    if subset:
        combined = combined.drop_duplicates(subset=subset)
    else:
        combined = combined.drop_duplicates()
    return combined


def merge_from_outputs(wos_dir: str, scopus_dir: str, out_dir: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Convenience wrapper that reads outputs from WoS_results and Scopus_results directories and writes All_* CSVs.
    Returns (all_articles, all_authors, all_articleauthor)
    """
    # Load Articles
    wos_article = pd.read_csv(f"{wos_dir}/Article.csv")
    scopus_article = pd.read_csv(f"{scopus_dir}/Article.csv")
    all_articles, doi_map = merge_articles(wos_article, scopus_article)
    all_articles.to_csv(f"{out_dir}/All_Articles.csv", index=False)

    # Load Authors + ArticleAuthor
    wos_author = pd.read_csv(f"{wos_dir}/Author.csv")
    scopus_author = pd.read_csv(f"{scopus_dir}/Author.csv")
    wos_articleauthor = pd.read_csv(f"{wos_dir}/ArticleAuthor.csv")
    scopus_articleauthor = pd.read_csv(f"{scopus_dir}/ArticleAuthor.csv")

    all_authors, all_articleauthor = merge_authors(
        wos_author, scopus_author, wos_article, scopus_article, wos_articleauthor, scopus_articleauthor, doi_map
    )
    all_authors.to_csv(f"{out_dir}/All_Authors.csv", index=False)
    all_articleauthor.to_csv(f"{out_dir}/All_ArticleAuthor.csv", index=False)

    return all_articles, all_authors, all_articleauthor


def merge_all_entities(wos_dir: str, scopus_dir: str, out_dir: str) -> Dict[str, pd.DataFrame]:
    """
    Merge Articles, Authors, ArticleAuthor, Citation and Affiliation into a single destination folder.
    Returns a dict of DataFrames keyed by name.
    """
    # Ensure out_dir exists
    import os
    os.makedirs(out_dir, exist_ok=True)

    # Articles first
    wos_article = pd.read_csv(f"{wos_dir}/Article.csv")
    scopus_article = pd.read_csv(f"{scopus_dir}/Article.csv")
    all_articles, doi_map = merge_articles(wos_article, scopus_article)
    all_articles.to_csv(f"{out_dir}/All_Articles.csv", index=False)

    # Authors / ArticleAuthor
    wos_author = pd.read_csv(f"{wos_dir}/Author.csv")
    scopus_author = pd.read_csv(f"{scopus_dir}/Author.csv")
    wos_articleauthor = pd.read_csv(f"{wos_dir}/ArticleAuthor.csv")
    scopus_articleauthor = pd.read_csv(f"{scopus_dir}/ArticleAuthor.csv")
    all_authors, all_articleauthor = merge_authors(
        wos_author, scopus_author, wos_article, scopus_article, wos_articleauthor, scopus_articleauthor, doi_map
    )
    all_authors.to_csv(f"{out_dir}/All_Authors.csv", index=False)
    all_articleauthor.to_csv(f"{out_dir}/All_ArticleAuthor.csv", index=False)

    # Citations
    try:
        wos_citation = pd.read_csv(f"{wos_dir}/Citation.csv")
    except Exception:
        wos_citation = pd.DataFrame()
    try:
        scopus_citation = pd.read_csv(f"{scopus_dir}/Citation.csv")
    except Exception:
        scopus_citation = pd.DataFrame()
    all_citation = merge_citation(wos_citation, scopus_citation, wos_article, scopus_article, doi_map)
    all_citation.to_csv(f"{out_dir}/All_Citation.csv", index=False)

    # Affiliation (edge)
    try:
        wos_aff = pd.read_csv(f"{wos_dir}/Affiliation.csv")
    except Exception:
        # fallback to temp name
        try:
            wos_aff = pd.read_csv(f"{wos_dir}/10_temp_wos_author_affiliation.csv")
        except Exception:
            wos_aff = pd.DataFrame()
    try:
        scopus_aff = pd.read_csv(f"{scopus_dir}/Affiliation.csv")
    except Exception:
        scopus_aff = pd.DataFrame()
    all_aff = merge_affiliation(wos_aff, scopus_aff, wos_article, scopus_article, doi_map)
    all_aff.to_csv(f"{out_dir}/All_Affiliation.csv", index=False)

    return {
        'All_Articles': all_articles,
        'All_Authors': all_authors,
        'All_ArticleAuthor': all_articleauthor,
        'All_Citation': all_citation,
        'All_Affiliation': all_aff,
    }
