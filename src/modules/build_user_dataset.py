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

    df = pd.read_csv(articles_csv)

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

            df['__issn_norm'] = norm_from_cols(df, ['issn', 'ISSN'], _normalize_issn)
            df['__eissn_norm'] = norm_from_cols(df, ['eissn', 'EISSN'], _normalize_issn)
            df['__title_key'] = title_from_cols(df, ['source_title', 'journal', 'source', 'Journal']).apply(_ascii_upper)
            df['__year'] = pd.to_numeric(df.get('year', pd.Series([None]*len(df))), errors='coerce').astype('Int64')

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
                merged = df.merge(
                    sci_q[['__issn_norm', '__year', 'Quartile']].drop_duplicates(),
                    how='left', on=['__issn_norm', '__year'], suffixes=('', '_q1')
                )
                # Fill by eISSN+year where missing
                aux = df.merge(
                    sci_q[['__eissn_norm', '__year', 'Quartile']].drop_duplicates(),
                    how='left', left_on=['__eissn_norm', '__year'], right_on=['__eissn_norm', '__year']
                )
                merged['Quartile'] = merged['Quartile'].fillna(aux['Quartile'])

                # Fill by title+year where still missing
                aux2 = df.merge(
                    sci_q[['__title_key', '__year', 'Quartile']].drop_duplicates(),
                    how='left', on=['__title_key', '__year']
                )
                merged['Quartile'] = merged['Quartile'].fillna(aux2['Quartile'])

                # If we created a separate merged frame, sync back to df
                df = merged

                # Position Quartile next to 'journal' if present
                if 'Quartile' in df.columns:
                    cols = list(df.columns)
                    # Remove duplicates of Quartile if any accidental merges created them
                    # Ensure single 'Quartile'
                    # Rebuild with first occurrence only
                    seen = set()
                    cols_unique = []
                    for c in cols:
                        if c not in seen:
                            cols_unique.append(c); seen.add(c)
                    df = df[cols_unique]

                    try:
                        if 'journal' in df.columns:
                            cols = list(df.columns)
                            qpos = cols.index('Quartile')
                            jpos = cols.index('journal')
                            if qpos != jpos + 1:
                                cols.pop(qpos)
                                cols.insert(jpos + 1, 'Quartile')
                                df = df[cols]
                        elif 'source_title' in df.columns:
                            cols = list(df.columns)
                            qpos = cols.index('Quartile')
                            spos = cols.index('source_title')
                            if qpos != spos + 1:
                                cols.pop(qpos)
                                cols.insert(spos + 1, 'Quartile')
                                df = df[cols]
                    except Exception:
                        # If any positioning fails, keep as-is
                        pass

    # Clean illegal characters per column to avoid Excel write errors
    for col in df.columns:
        df[col] = remove_illegal_chars_series(df[col])

    # Write Excel with sheet 'wos_scopus' and (if available) raw WoS sheet 'wos'
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="wos_scopus", index=False)

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

    return out_path
