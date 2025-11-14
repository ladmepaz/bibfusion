import os
import re
import unicodedata
from typing import Dict, List, Tuple

import pandas as pd


def _ascii_upper(text: str) -> str:
    if not isinstance(text, str):
        return ''
    norm = unicodedata.normalize('NFKD', str(text))
    stripped = ''.join(ch for ch in norm if not unicodedata.combining(ch))
    return stripped.upper().strip()


_ORCID_RE = re.compile(r"(\d{4}-\d{4}-\d{4}-\d{3}[0-9Xx])")


def _norm_orcid(value: str) -> str:
    if not isinstance(value, str):
        return ''
    s = value.strip()
    s = s.replace('ORCID:', '').replace('orcid:', '')
    m = _ORCID_RE.search(s)
    if not m:
        return ''
    return m.group(1).upper()


def _norm_openalex_author(value: str) -> str:
    if not isinstance(value, str):
        return ''
    s = value.strip().upper()
    s = s.replace('OA:', '')
    # Extract canonical pattern A<digits> from any representation
    m = re.search(r"A\d+", s)
    return m.group(0) if m else ''


def _best_row(rows: List[pd.Series]) -> pd.Series:
    # Prefer row with ORCID, then OpenAlex, then with longer AuthorFullName
    def score(r):
        sc = 0
        if str(r.get('Orcid', '')).strip() not in ('', 'NO ORCID', 'NO_ORCID'):
            sc += 10
        if str(r.get('OpenAlexAuthorID', '')).strip() not in ('', 'NO', 'NO OPENALEX'):
            sc += 5
        name = str(r.get('AuthorFullName', ''))
        sc += min(len(name), 50) / 50.0
        return sc
    return sorted(rows, key=score, reverse=True)[0]


def cross_consolidate_all_authors(all_dir: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Consolidate identities in All_Authors.csv across ORCID / OpenAlex / NAME into a single global PersonID.
    - Global PersonID priority: ORCID:XXXX -> OA:A... -> NAME:...
    - Collapse rows sharing same normalized ORCID; attach all OpenAlex IDs as aliases.
    - Map NAME-only rows to an existing identity if the normalized name matches uniquely.
    - Remap All_ArticleAuthor.PersonID to the global PersonID (or create it if missing via AuthorID join).

    Writes the updated CSVs in-place and returns (authors_df, articleauthor_df).
    """
    authors_csv = os.path.join(all_dir, 'All_Authors.csv')
    aa_csv = os.path.join(all_dir, 'All_ArticleAuthor.csv')

    if not os.path.exists(authors_csv) or not os.path.exists(aa_csv):
        raise FileNotFoundError('All_Authors.csv or All_ArticleAuthor.csv not found in ' + all_dir)

    au = pd.read_csv(authors_csv)
    aa = pd.read_csv(aa_csv)

    # Normalize fields
    au = au.copy()
    au['__orig_pid'] = au.get('PersonID', '').astype(str)
    au['__orcid_norm'] = au.get('Orcid', '').apply(_norm_orcid)
    # Also extract from PersonID if needed
    mask_pid_orcid = au['__orcid_norm'] == ''
    au.loc[mask_pid_orcid, '__orcid_norm'] = au.loc[mask_pid_orcid, '__orig_pid'].apply(_norm_orcid)

    au['__oa_norm'] = au.get('OpenAlexAuthorID', '').apply(_norm_openalex_author)
    # Also from PersonID if an OA id is encoded there
    mask_pid_oa = au['__oa_norm'] == ''
    au.loc[mask_pid_oa, '__oa_norm'] = au.loc[mask_pid_oa, '__orig_pid'].apply(_norm_openalex_author)

    au['__name_key'] = au.get('AuthorFullName', au.get('AuthorName', '')).apply(_ascii_upper)

    # 1) ORCID groups -> ORCID:XXXX
    groups: Dict[str, List[int]] = {}
    for i, v in au['__orcid_norm'].items():
        if v:
            groups.setdefault(f'ORCID:{v}', []).append(i)

    # Map OA -> ORCID PID when any row shares the same OA and has ORCID
    oa_to_orcid_pid: Dict[str, str] = {}
    for i, row in au.iterrows():
        if row.get('__oa_norm') and row.get('__orcid_norm'):
            oa_to_orcid_pid[row['__oa_norm']] = f"ORCID:{row['__orcid_norm']}"

    # 2) OA groups (without orcid): link to ORCID group if shared OpenAlexID; else OA:A...
    for i, v in au['__oa_norm'].items():
        if v and not au.at[i, '__orcid_norm']:
            pid = oa_to_orcid_pid.get(v, f'OA:{v}')
            groups.setdefault(pid, []).append(i)

    # 3) NAME-only groups (without orcid and oa)
    # Try to map to existing groups via unique name_key; otherwise keep as-is
    # Build name index for groups created so far
    pid_to_name = {}
    for pid, idxs in groups.items():
        # choose best row to get a representative name
        best = _best_row([au.loc[j] for j in idxs])
        pid_to_name[pid] = best.get('AuthorFullName', '') or best.get('AuthorName', '')
    name_to_pid = {}
    for pid, nm in pid_to_name.items():
        key = _ascii_upper(nm)
        if not key:
            continue
        name_to_pid.setdefault(key, set()).add(pid)

    conflicts: List[Dict[str, str]] = []
    for i in range(len(au)):
        if au.at[i, '__orcid_norm'] or au.at[i, '__oa_norm']:
            continue
        key = au.at[i, '__name_key']
        if key and key in name_to_pid and len(name_to_pid[key]) == 1:
            # map to the only pid for that name
            pid = list(name_to_pid[key])[0]
            groups.setdefault(pid, []).append(i)
        else:
            # keep as independent NAME:... identity
            orig = au.at[i, '__orig_pid']
            pid = orig if isinstance(orig, str) and orig.startswith('NAME:') else f"NAME:{key}" if key else orig
            groups.setdefault(pid, []).append(i)
            # If multiple candidates existed, log conflict for review
            if key and key in name_to_pid and len(name_to_pid[key]) > 1:
                conflicts.append({
                    'OriginalPersonID': str(au.at[i, '__orig_pid']),
                    'NameKey': key,
                    'CandidatePIDs': '; '.join(sorted(name_to_pid[key])),
                })

    # Build consolidated authors
    out_rows = []
    pid_map: Dict[str, str] = {}  # orig_pid -> global_pid
    alias_rows: List[Dict[str, str]] = []
    for pid, idxs in groups.items():
        rows = [au.loc[j] for j in idxs]
        best = _best_row(rows)

        # Global PID already determined by group key (ORCID:/OA:/NAME:)
        global_pid = pid

        # Aggregate fields
        # Orcid: prefer normalized
        orcid_norms = sorted({r.get('__orcid_norm', '') for r in rows if r.get('__orcid_norm', '')})
        orcid_url = f"https://orcid.org/{orcid_norms[0]}" if orcid_norms else ''

        # OpenAlex IDs: collect as URL form
        oa_ids = sorted({ _norm_openalex_author(r.get('OpenAlexAuthorID', '')) for r in rows if _norm_openalex_author(r.get('OpenAlexAuthorID', '')) })
        oa_urls = ['https://openalex.org/' + oid for oid in oa_ids]

        # Names
        fullname = best.get('AuthorFullName', '') or best.get('AuthorName', '')
        fullname = _ascii_upper(fullname)
        display_name = best.get('AuthorName', '') or best.get('AuthorFullName', '')

        # Other fields
        email = ''
        rid = ''
        authorid = ''
        for r in rows:
            if not email:
                email = str(r.get('Email', '') or '')
            if not rid:
                rid = str(r.get('ResearcherID', '') or '')
            if not authorid:
                authorid = str(r.get('AuthorID', '') or '')

        out_rows.append({
            'PersonID': global_pid,
            'AuthorFullName': fullname,
            'AuthorName': display_name,
            'Orcid': orcid_url,
            'OpenAlexAuthorID': '; '.join(oa_urls) if oa_urls else '',
            'AuthorID': authorid,
            'ResearcherID': rid,
            'Email': email,
        })

        for j in idxs:
            orig = au.at[j, '__orig_pid']
            pid_map[str(orig)] = global_pid
            alias_rows.append({
                'OriginalPersonID': str(orig),
                'GlobalPersonID': global_pid,
                'AuthorFullName': fullname,
                'Orcid': orcid_url,
                'OpenAlexAuthorIDs': '; '.join(oa_urls) if oa_urls else ''
            })

    authors_out = pd.DataFrame(out_rows)
    # Sort by PersonID for stability
    authors_out = authors_out.drop_duplicates(subset=['PersonID']).sort_values('PersonID')

    # Remap All_ArticleAuthor
    aa_out = aa.copy()
    if 'PersonID' in aa_out.columns:
        aa_out['PersonID'] = aa_out['PersonID'].astype(str).map(lambda x: pid_map.get(x, x))
        # Deduplicate after mapping
        subset = [c for c in ['SR', 'PersonID', 'AuthorOrder'] if c in aa_out.columns]
        if subset:
            aa_out = aa_out.drop_duplicates(subset=subset)
    elif 'AuthorID' in aa_out.columns:
        # Merge to attach PersonID from authors_out
        aa_out = aa_out.merge(authors_out[['AuthorID', 'PersonID']].drop_duplicates(), how='left', on='AuthorID')
        # Prefer PersonID column moving forward
        subset = [c for c in ['SR', 'PersonID', 'AuthorOrder'] if c in aa_out.columns]
        if subset:
            aa_out = aa_out.drop_duplicates(subset=subset)

    # Write back (and emit alias/conflicts for traceability)
    authors_out.to_csv(authors_csv, index=False)
    aa_out.to_csv(aa_csv, index=False)
    try:
        if alias_rows:
            pd.DataFrame(alias_rows).to_csv(os.path.join(all_dir, 'All_AuthorAlias.csv'), index=False)
        if conflicts:
            pd.DataFrame(conflicts).to_csv(os.path.join(all_dir, 'All_AuthorConflicts.csv'), index=False)
    except Exception:
        pass

    return authors_out, aa_out
