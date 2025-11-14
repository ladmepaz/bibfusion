import os
import pandas as pd

from .to_xlsx import remove_illegal_chars_series


def _ascii_upper(text: str) -> str:
    if not isinstance(text, str):
        return ""
    import unicodedata
    norm = unicodedata.normalize('NFKD', str(text))
    stripped = ''.join(ch for ch in norm if not unicodedata.combining(ch))
    return stripped.upper().strip()


def _normalize_issn(value: str) -> str:
    if not isinstance(value, str):
        return ''
    digits = ''.join(ch for ch in value if ch.isdigit())
    if len(digits) != 8:
        return ''
    return f"{digits[:4]}-{digits[4:]}"


def build_user_dataset_from_all(all_dir: str, out_path: str = None, add_quartile: bool = True) -> str:
    """
    Create a user-friendly Excel from consolidated All_* CSVs.

    Phase 1 (initial):
    - Read `All_Articles.csv` from `all_dir` and export to a single-sheet Excel
      named 'wos_scopus'.

    Parameters
    - all_dir: directory containing All_Articles.csv (e.g., 'all_data_wos_scopus')
    - out_path: optional explicit path for output .xlsx. If None, writes
      `UserDataset.xlsx` under `all_dir`.

    Returns the path to the generated Excel file.
    """

    if out_path is None:
        out_path = os.path.join(all_dir, "UserDataset.xlsx")

    articles_csv = os.path.join(all_dir, "All_Articles.csv")
    if not os.path.exists(articles_csv):
        raise FileNotFoundError(f"All_Articles.csv not found in {all_dir}")

    # Load all consolidated articles once
    articles_all = pd.read_csv(articles_csv)

    # Optionally enrich with Scimago quartile (by year)
    if add_quartile:
        scimago_csv = os.path.join(all_dir, "All_Scimagodb.csv")
        if os.path.exists(scimago_csv):
            sci = pd.read_csv(scimago_csv)

            # Normalize keys in Articles
            def norm_from_cols(dataf, cols, normfn):
                for c in cols:
                    if c in dataf.columns:
                        return dataf[c].apply(normfn)
                return pd.Series([''] * len(dataf))

            def title_from_cols(dataf, cols):
                for c in cols:
                    if c in dataf.columns:
                        return dataf[c]
                return pd.Series([''] * len(dataf))

            articles_all['__issn_norm'] = norm_from_cols(articles_all, ['issn', 'ISSN'], _normalize_issn)
            articles_all['__eissn_norm'] = norm_from_cols(articles_all, ['eissn', 'EISSN'], _normalize_issn)
            articles_all['__title_key'] = title_from_cols(articles_all, ['source_title', 'journal', 'source', 'Journal']).apply(_ascii_upper)
            articles_all['__year'] = pd.to_numeric(articles_all.get('year', pd.Series([None]*len(articles_all))), errors='coerce').astype('Int64')

            # Normalize keys in Scimago
            sci['__issn_norm'] = norm_from_cols(sci, ['Issn', 'ISSN', 'issn'], _normalize_issn)
            sci['__eissn_norm'] = norm_from_cols(sci, ['eIssn', 'EISSN', 'eissn'], _normalize_issn)
            sci['__title_key'] = title_from_cols(sci, ['Title', 'title', 'Journal', 'journal']).apply(_ascii_upper)
            sci['__year'] = pd.to_numeric(sci.get('year', pd.Series([None]*len(sci))), errors='coerce').astype('Int64')

            # Pick quartile column name
            quart_col = 'SJR Best Quartile' if 'SJR Best Quartile' in sci.columns else (
                'Quartile' if 'Quartile' in sci.columns else None
            )
            if quart_col is not None:
                sci_q = sci[['__issn_norm', '__eissn_norm', '__title_key', '__year', quart_col]].copy()
                sci_q = sci_q.rename(columns={quart_col: 'Quartile'})

                # Merge by ISSN+year
                merged = articles_all.merge(
                    sci_q[['__issn_norm', '__year', 'Quartile']].drop_duplicates(),
                    how='left', on=['__issn_norm', '__year'], suffixes=('', '_q1')
                )
                # Fill by eISSN+year where missing
                aux = articles_all.merge(
                    sci_q[['__eissn_norm', '__year', 'Quartile']].drop_duplicates(),
                    how='left', left_on=['__eissn_norm', '__year'], right_on=['__eissn_norm', '__year']
                )
                merged['Quartile'] = merged['Quartile'].fillna(aux['Quartile'])

                # Fill by title+year where still missing
                aux2 = articles_all.merge(
                    sci_q[['__title_key', '__year', 'Quartile']].drop_duplicates(),
                    how='left', on=['__title_key', '__year']
                )
                merged['Quartile'] = merged['Quartile'].fillna(aux2['Quartile'])

                # If we created a separate merged frame, sync back to df
                articles_all = merged

                # Position Quartile next to 'journal' if present
                if 'Quartile' in articles_all.columns:
                    cols = list(articles_all.columns)
                    # Remove duplicates of Quartile if any accidental merges created them
                    # Ensure single 'Quartile'
                    # Rebuild with first occurrence only
                    seen = set()
                    cols_unique = []
                    for c in cols:
                        if c not in seen:
                            cols_unique.append(c); seen.add(c)
                    articles_all = articles_all[cols_unique]

                    try:
                        if 'journal' in articles_all.columns:
                            cols = list(articles_all.columns)
                            qpos = cols.index('Quartile')
                            jpos = cols.index('journal')
                            if qpos != jpos + 1:
                                cols.pop(qpos)
                                cols.insert(jpos + 1, 'Quartile')
                                articles_all = articles_all[cols]
                        elif 'source_title' in articles_all.columns:
                            cols = list(articles_all.columns)
                            qpos = cols.index('Quartile')
                            spos = cols.index('source_title')
                            if qpos != spos + 1:
                                cols.pop(qpos)
                                cols.insert(spos + 1, 'Quartile')
                                articles_all = articles_all[cols]
                    except Exception:
                        # If any positioning fails, keep as-is
                        pass

    # Clean illegal characters per column to avoid Excel write errors
    for col in articles_all.columns:
        articles_all[col] = remove_illegal_chars_series(articles_all[col])

    # Write Excel with sheet 'wos_scopus' and (if available) raw WoS sheet 'wos'
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        # Build main-only view for the user and drop ismainarticle/help columns
        if 'ismainarticle' in articles_all.columns:
            col = articles_all['ismainarticle']
            is_main = col if pd.api.types.is_bool_dtype(col) else (col.astype(str).str.upper().eq('TRUE') | col.astype(str).eq('1'))
        else:
            is_main = pd.Series([False] * len(articles_all))

        helper_cols = ['__issn_norm', '__eissn_norm', '__title_key', '__year']
        sheet_cols_drop = [c for c in helper_cols + ['ismainarticle'] if c in articles_all.columns]
        sheet_main = articles_all[is_main].drop(columns=sheet_cols_drop, errors='ignore')
        # Ensure unique articles in user-facing sheet
        if 'SR' in sheet_main.columns:
            sheet_main = sheet_main.drop_duplicates(subset=['SR'])
        # Add normalized helper columns for display (uppercased ASCII)
        try:
            if 'title' in sheet_main.columns:
                sheet_main['title_norm'] = sheet_main['title'].apply(_ascii_upper)
            # Journal normalization: prefer 'journal', else 'source_title'
            jcol = 'journal' if 'journal' in sheet_main.columns else ('source_title' if 'source_title' in sheet_main.columns else None)
            if jcol is not None:
                sheet_main['journal_norm'] = sheet_main[jcol].apply(_ascii_upper)
        except Exception:
            pass
        sheet_main.to_excel(writer, sheet_name="wos_scopus", index=False)

        # Try to locate WoS_results raw first-load CSV to provide a 1:1 count reference
        # Typical layout: <base>/WoS_results/1_temp_wos_df.csv and <base>/all_data_wos_scopus/
        base_dir = os.path.abspath(os.path.join(all_dir, os.pardir))
        wos_dir = os.path.join(base_dir, "WoS_results")
        wos_raw_candidates = [
            os.path.join(wos_dir, "1_temp_wos_df.csv"),
            os.path.join(wos_dir, "1_temp_wos_df.csv"),  # same, kept for clarity/future variants
        ]
        for p in wos_raw_candidates:
            if os.path.exists(p):
                wos_raw = pd.read_csv(p)
                # Clean illegal characters
                for col in wos_raw.columns:
                    wos_raw[col] = remove_illegal_chars_series(wos_raw[col])
                # Sheet name 'wos' as requested
                wos_raw.to_excel(writer, sheet_name="wos", index=False)
                break

        # Add Scopus raw first-load CSV if present
        scopus_dir = os.path.join(base_dir, "Scopus_results")
        scopus_raw_candidates = [
            os.path.join(scopus_dir, "1_temp_scopus_df.csv"),
        ]
        for p in scopus_raw_candidates:
            if os.path.exists(p):
                scopus_raw = pd.read_csv(p)
                for col in scopus_raw.columns:
                    scopus_raw[col] = remove_illegal_chars_series(scopus_raw[col])
                scopus_raw.to_excel(writer, sheet_name="scopus", index=False)
                break

        # Build reference_df: All_Citation with metadata of referenced items (from All_Articles)
        citation_csv = os.path.join(all_dir, "All_Citation.csv")
        if os.path.exists(citation_csv):
            try:
                citations = pd.read_csv(citation_csv)
                # Keep only SR and SR_ref from citations
                edges = citations[['SR', 'SR_ref']].copy()

                # Join reference metadata by matching SR_ref -> Articles.SR
                # Use the already loaded (possibly enriched) articles df
                articles_meta = articles_all.copy()
                joined = edges.merge(articles_meta, how='left', left_on='SR_ref', right_on='SR')

                # Drop duplicate SR column from the right side; keep left SR and SR_ref
                if 'SR_y' in joined.columns:
                    joined = joined.drop(columns=['SR_y'])
                    joined = joined.rename(columns={'SR_x': 'SR'})
                else:
                    # If merge didn't suffix, simply drop the right SR
                    cols = list(joined.columns)
                    # Remove the last occurrence of 'SR' if there are two
                    if cols.count('SR') > 1:
                        # Drop the column named 'SR' that corresponds to articles_meta
                        # Identify by index: keep the first occurrence
                        first = True
                        keep = []
                        for c in cols:
                            if c == 'SR':
                                if first:
                                    keep.append(True); first = False
                                else:
                                    keep.append(False)
                            else:
                                keep.append(True)
                        joined = joined.loc[:, keep]

                # Add normalized columns for display
                try:
                    if 'title' in joined.columns:
                        joined['title_norm'] = joined['title'].apply(_ascii_upper)
                    jcol = 'journal' if 'journal' in joined.columns else ('source_title' if 'source_title' in joined.columns else None)
                    if jcol is not None:
                        joined['journal_norm'] = joined[jcol].apply(_ascii_upper)
                except Exception:
                    pass

                # Clean illegal characters
                for c in joined.columns:
                    joined[c] = remove_illegal_chars_series(joined[c])

                joined.to_excel(writer, sheet_name="reference_df", index=False)
            except Exception:
                # If anything goes wrong, skip creating this sheet silently
                pass

        # Build journal_df: SR, JournalMain, YearMain, JournalRef, YearRef
        if os.path.exists(citation_csv):
            try:
                citations = pd.read_csv(citation_csv)
                edges = citations[['SR', 'SR_ref']].copy()

                articles_meta = articles_all.copy()
                # Identify journal column in articles
                art_journal_col = 'journal' if 'journal' in articles_meta.columns else (
                    'source_title' if 'source_title' in articles_meta.columns else None
                )
                # Merge citing (Main)
                main = edges.merge(articles_meta[['SR', art_journal_col, 'year']], how='left', on='SR')
                main = main.rename(columns={art_journal_col: 'JournalMain', 'year': 'YearMain'})

                # Merge referenced (Ref)
                ref = main.merge(articles_meta[['SR', art_journal_col, 'year']], how='left', left_on='SR_ref', right_on='SR', suffixes=('', '_ref'))
                # Columns after merge: SR (citing), SR_ref, SR_ref's SR as SR_ref merge col
                # Rename ref columns
                ref = ref.rename(columns={f'{art_journal_col}_ref': 'JournalRef', 'year_ref': 'YearRef'})
                if 'SR_ref1' in ref.columns:
                    # In case pandas created SR_ref1 when both had SR_ref
                    pass
                # Keep only requested columns
                journal_df = ref[['SR', 'JournalMain', 'YearMain', 'JournalRef', 'YearRef']].copy()

                # Clean illegal characters
                for c in journal_df.columns:
                    journal_df[c] = remove_illegal_chars_series(journal_df[c])

                journal_df.to_excel(writer, sheet_name="journal_df", index=False)
            except Exception:
                pass

        # Sheet 6: authors_df from All_Authors.csv (ordered columns)
        authors_csv = os.path.join(all_dir, "All_Authors.csv")
        if os.path.exists(authors_csv):
            try:
                authors = pd.read_csv(authors_csv)
                desired_cols = [
                    'PersonID',
                    'AuthorFullName',
                    'AuthorName',
                    'Orcid',
                    'OpenAlexAuthorID',
                    'AuthorID',
                    'ResearcherID',
                    'Email',
                ]
                present = [c for c in desired_cols if c in authors.columns]
                authors = authors[present]
                for c in authors.columns:
                    authors[c] = remove_illegal_chars_series(authors[c])
                authors.to_excel(writer, sheet_name="authors_df", index=False)
            except Exception:
                pass

        # Sheet 11: author_clusters (if available)
        clusters_csv = os.path.join(all_dir, 'All_AuthorClusters.csv')
        if os.path.exists(clusters_csv):
            try:
                clusters = pd.read_csv(clusters_csv)
                for c in clusters.columns:
                    clusters[c] = remove_illegal_chars_series(clusters[c])
                clusters.to_excel(writer, sheet_name='author_clusters', index=False)
            except Exception:
                pass

        # Sheet 12: author_conflicts_review (if available)
        conflicts_review_csv = os.path.join(all_dir, 'All_AuthorConflictsReview.csv')
        if os.path.exists(conflicts_review_csv):
            try:
                conflicts_rev = pd.read_csv(conflicts_review_csv)
                for c in conflicts_rev.columns:
                    conflicts_rev[c] = remove_illegal_chars_series(conflicts_rev[c])
                conflicts_rev.to_excel(writer, sheet_name='author_conflicts_review', index=False)
            except Exception:
                pass

        # Sheet 7: figure_1_data — yearly totals for main articles (ismainarticle == TRUE)
        try:
            # Robust boolean filter for ismainarticle
            is_main = False
            if 'ismainarticle' in df.columns:
                col = df['ismainarticle']
                if pd.api.types.is_bool_dtype(col):
                    is_main = col
                else:
                    is_main = col.astype(str).str.upper().eq('TRUE') | col.astype(str).eq('1')
            else:
                is_main = pd.Series([False] * len(df))

            years = pd.to_numeric(df.get('year', pd.Series([None] * len(df))), errors='coerce').astype('Int64')
            main_years = years[is_main]
            if main_years.notna().any():
                ymin = int(main_years.min())
                ymax = int(main_years.max())
                full_range = list(range(ymin, ymax + 1))
                counts = main_years.value_counts().reindex(full_range, fill_value=0).sort_index(ascending=False)
                fig_df = pd.DataFrame({'year': counts.index, 'total': counts.values})
                # Clean and write
                for c in fig_df.columns:
                    fig_df[c] = remove_illegal_chars_series(fig_df[c])
                fig_df.to_excel(writer, sheet_name="figure_1_data", index=False)
        except Exception:
            pass

        # Sheet 8: figure_2_country — country co-occurrence network for main articles
        try:
            if 'SR' in articles_all.columns:
                # Filter main articles again
                if 'ismainarticle' in articles_all.columns:
                    col = articles_all['ismainarticle']
                    is_main = col if pd.api.types.is_bool_dtype(col) else (col.astype(str).str.upper().eq('TRUE') | col.astype(str).eq('1'))
                else:
                    is_main = pd.Series([False] * len(df))

                df_main = articles_all[is_main].copy()
                # Country column (support alternative casing)
                country_col = 'country' if 'country' in df_main.columns else ('Country' if 'Country' in df_main.columns else None)
                if country_col is not None:
                    rows = []
                    import re
                    from itertools import combinations
                    for _, r in df_main.iterrows():
                        sr = r.get('SR', None)
                        raw = r.get(country_col, '')
                        if pd.isna(raw) or str(raw).strip() == '':
                            continue
                        # Split by comma or semicolon
                        parts = re.split(r"[;,]", str(raw))
                        # Normalize and deduplicate within the article
                        countries = sorted({p.strip().upper() for p in parts if p and p.strip()})
                        if len(countries) < 2:
                            continue
                        for a, b in combinations(countries, 2):
                            rows.append({'from': a, 'to': b, 'SR': sr})
                    if rows:
                        net_df = pd.DataFrame(rows)
                        # Clean
                        for c in net_df.columns:
                            net_df[c] = remove_illegal_chars_series(net_df[c])
                        net_df.to_excel(writer, sheet_name='figure_2_country', index=False)
        except Exception:
            pass

        # Sheet 9: figure_3_coauthor — collaboration network for top-10 productive authors (union of ego networks)
        try:
            aa_csv = os.path.join(all_dir, "All_ArticleAuthor.csv")
            authors_csv = os.path.join(all_dir, "All_Authors.csv")
            if os.path.exists(aa_csv):
                aa = pd.read_csv(aa_csv)
                # Prefer PersonID over AuthorID
                id_col = 'PersonID' if 'PersonID' in aa.columns else ('AuthorID' if 'AuthorID' in aa.columns else None)
                if id_col is None:
                    raise ValueError("All_ArticleAuthor.csv lacks PersonID/AuthorID")

                # Map SR -> ismainarticle from the articles DF already loaded
                if 'ismainarticle' in articles_all.columns and 'SR' in articles_all.columns:
                    art_flag = articles_all[['SR', 'ismainarticle']].copy()
                    # Normalize flag to boolean
                    col = art_flag['ismainarticle']
                    if pd.api.types.is_bool_dtype(col):
                        art_flag['__main'] = col
                    else:
                        art_flag['__main'] = col.astype(str).str.upper().eq('TRUE') | col.astype(str).eq('1')
                    aa_main = aa.merge(art_flag[['SR', '__main']], how='left', on='SR')
                    aa_main = aa_main[aa_main['__main'] == True]
                else:
                    aa_main = aa.iloc[0:0]

                # Top-10 authors by distinct main articles
                top_series = (
                    aa_main[[id_col, 'SR']]
                    .drop_duplicates()
                    .groupby(id_col)['SR']
                    .nunique()
                    .sort_values(ascending=False)
                    .head(10)
                )
                top_ids = set(top_series.index.tolist())

                # Sheet 10: figure_3_top10 — table of top-10 authors with IDs and counts
                try:
                    top_df = top_series.reset_index()
                    top_df.columns = [id_col, 'total_main_articles']
                    # Attach names and identifiers if available
                    if os.path.exists(authors_csv):
                        au = pd.read_csv(authors_csv)
                        cols_want = [
                            id_col,
                            'AuthorFullName',
                            'AuthorName',
                            'Orcid',
                            'OpenAlexAuthorID',
                            'Email',
                            'ResearcherID',
                        ]
                        cols_present = [c for c in cols_want if c in au.columns]
                        au2 = au[cols_present].drop_duplicates(subset=[id_col]) if id_col in cols_present else au[cols_present]
                        au2[id_col] = au2[id_col].astype(str)
                        top_df[id_col] = top_df[id_col].astype(str)
                        top_df = top_df.merge(au2, how='left', on=id_col)
                    # Rank column
                    top_df = top_df.sort_values(by='total_main_articles', ascending=False).reset_index(drop=True)
                    top_df.insert(0, 'rank', top_df.index + 1)
                    # Clean and write
                    for c in top_df.columns:
                        top_df[c] = remove_illegal_chars_series(top_df[c])
                    top_df.to_excel(writer, sheet_name='figure_3_top10', index=False)
                except Exception:
                    pass

                # Build full coauthor network (all articles, main+refs)
                from itertools import combinations
                pair_counts = {}
                for sr, grp in aa.groupby('SR'):
                    ids = sorted(set(grp[id_col].dropna().astype(str).tolist()))
                    if len(ids) < 2:
                        continue
                    for a, b in combinations(ids, 2):
                        # undirected key (min, max)
                        key = (a, b)
                        pair_counts[key] = pair_counts.get(key, 0) + 1

                if pair_counts:
                    import itertools
                    edges_full = pd.DataFrame(
                        [(k[0], k[1], w) for k, w in pair_counts.items()],
                        columns=['from', 'to', 'weight']
                    )

                    # Ego networks union: induced subgraph on (top_ids ∪ neighbors of top_ids)
                    adj = {}
                    for a, b, w in edges_full[['from', 'to', 'weight']].itertuples(index=False):
                        adj.setdefault(a, set()).add(b)
                        adj.setdefault(b, set()).add(a)
                    neighbors = set()
                    for u in top_ids:
                        if u in adj:
                            neighbors.update(adj[u])
                    node_set = top_ids.union(neighbors)
                    edges_sub = edges_full[edges_full['from'].isin(node_set) & edges_full['to'].isin(node_set)].copy()

                    # Attach names if available
                    if os.path.exists(authors_csv):
                        au = pd.read_csv(authors_csv)
                        # Preferred display name
                        disp = 'AuthorFullName' if 'AuthorFullName' in au.columns else (
                            'AuthorName' if 'AuthorName' in au.columns else None
                        )
                        if disp is not None:
                            # Build map id -> name
                            id_map = au[[
                                c for c in [id_col, disp] if c in au.columns
                            ]].drop_duplicates()
                            id_map[id_col] = id_map[id_col].astype(str)
                            name_map = dict(zip(id_map[id_col], id_map[disp]))
                            edges_sub['from_name'] = edges_sub['from'].map(name_map)
                            edges_sub['to_name'] = edges_sub['to'].map(name_map)

                    # Clean and write
                    for c in edges_sub.columns:
                        edges_sub[c] = remove_illegal_chars_series(edges_sub[c])
                    edges_sub.to_excel(writer, sheet_name='figure_3_coauthor', index=False)
        except Exception:
            pass

    return out_path
