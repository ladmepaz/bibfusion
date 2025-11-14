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


def _name_signature(text: str) -> str:
    """Robust name key: ASCII upper, split on non-letters, sort tokens."""
    s = _ascii_upper(text)
    # Keep A-Z and spaces
    import re as _re
    s = _re.sub(r"[^A-Z]+", " ", s)
    tokens = [t for t in s.split() if t]
    tokens.sort()
    return ' '.join(tokens)


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


def cross_consolidate_all_authors(all_dir: str, aggressive_name_merge: bool = False) -> Tuple[pd.DataFrame, pd.DataFrame]:
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
    aff_csv = os.path.join(all_dir, 'All_Affiliation.csv')

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
    au['__name_sig'] = au.get('AuthorFullName', au.get('AuthorName', '')).apply(_name_signature)

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

    # Aggressive NAME -> rich identity merge (optional)
    if aggressive_name_merge:
        # Representative name key/signature per existing group
        pid_rep_name_key: Dict[str, str] = {}
        pid_rep_name_sig: Dict[str, str] = {}
        for pid, idxs in groups.items():
            best = _best_row([au.loc[j] for j in idxs])
            rep = best.get('AuthorFullName', '') or best.get('AuthorName', '')
            pid_rep_name_key[p] = _ascii_upper(rep) if (p := pid) else ''
            pid_rep_name_sig[p] = _name_signature(rep)

        # Article counts per original PersonID from AA (before remap)
        if 'PersonID' in aa.columns:
            aa_counts = aa['PersonID'].astype(str).value_counts().to_dict()
        else:
            aa_counts = {}

        def has_orcid(pid: str) -> bool:
            return pid.startswith('ORCID:')

        def has_oa(pid: str) -> bool:
            return pid.startswith('OA:') or any(_norm_openalex_author(str(au.loc[i].get('OpenAlexAuthorID',''))) for i in groups.get(pid, []))

        def any_email(pid: str) -> bool:
            for i in groups.get(pid, []):
                if str(au.loc[i].get('Email','')).strip():
                    return True
            return False

        def article_count(pid: str) -> int:
            # Sum counts for all original orig_pid in this group
            total = 0
            for i in groups.get(pid, []):
                orig = str(au.loc[i, '__orig_pid'])
                total += aa_counts.get(orig, 0)
            return total

        name_pids = [pid for pid in list(groups.keys()) if isinstance(pid, str) and pid.startswith('NAME:')]
        for npid in name_pids:
            key = pid_rep_name_key.get(npid, '')
            if not key:
                continue
            # candidates: non-NAME groups with same name key OR same signature (more robust)
            key_sig = pid_rep_name_sig.get(npid, '')
            cands = [pid for pid, k in pid_rep_name_key.items() if pid != npid and not pid.startswith('NAME:') and (k == key or pid_rep_name_sig.get(pid,'') == key_sig)]
            if not cands:
                continue
            # score candidates
            scored = []
            # Precompute SR and coauthors sets for npid
            def articles_of_pid(pid: str) -> set:
                s = set()
                for i in groups.get(pid, []):
                    orig = str(au.loc[i, '__orig_pid'])
                    if 'PersonID' in aa.columns:
                        s.update(aa.loc[aa['PersonID'].astype(str)==orig, 'SR'].astype(str).tolist())
                return s
            def coauthors_of_pid(pid: str) -> set:
                # all PersonIDs (orig) that co-occur with this pid on same SRs
                arts = articles_of_pid(pid)
                if not arts:
                    return set()
                subset = aa[aa['SR'].astype(str).isin(arts)]
                return set(subset['PersonID'].astype(str).tolist()) if 'PersonID' in subset.columns else set()
            def jaccard(a: set, b: set) -> float:
                if not a and not b:
                    return 0.0
                inter = len(a & b)
                union = len(a | b)
                return inter/union if union else 0.0

            npid_articles = articles_of_pid(npid)
            npid_coauthors = coauthors_of_pid(npid)
            for pid in cands:
                sc = 0
                sc += 100 if has_orcid(pid) else 0
                sc += 20 if has_oa(pid) else 0
                sc += 10 if any_email(pid) else 0
                sc += article_count(pid)
                # add overlap signals
                pid_articles = articles_of_pid(pid)
                pid_coauthors = coauthors_of_pid(pid)
                sc += 50 * jaccard(npid_articles, pid_articles)
                sc += 50 * jaccard(npid_coauthors, pid_coauthors)
                scored.append((sc, pid))
            scored.sort(reverse=True)
            if not scored:
                continue
            best_score, best_pid = scored[0]
            # tie-breaker: if tie in score and multiple with non-ORCID, skip
            ties = [pid for sc, pid in scored if sc == best_score]
            if len(ties) > 1:
                # prefer ORCID among ties
                ties_orcid = [pid for pid in ties if has_orcid(pid)]
                if len(ties_orcid) == 1:
                    best_pid = ties_orcid[0]
                else:
                    # ambiguous; skip merge
                    continue
            # merge NAME group npid into best_pid
            idxs = groups.get(npid, [])
            if not idxs:
                continue
            groups.setdefault(best_pid, []).extend(idxs)
            del groups[npid]

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

    # Build clusters and conflict review (post-merge) for inspection
    # Representative name signature per PID
    pid_rep_sig: Dict[str, str] = {}
    def email_domain(s: str) -> str:
        if not isinstance(s, str):
            return ''
        s = s.strip()
        if '@' in s:
            return s.split('@')[-1].lower()
        return ''

    # Build aux maps for metrics
    # Article sets and coauthor sets per PID
    def articles_of_pid_final(pid: str) -> set:
        s = set()
        for i in groups.get(pid, []):
            orig = str(au.loc[i, '__orig_pid'])
            if 'PersonID' in aa.columns:
                s.update(aa.loc[aa['PersonID'].astype(str)==orig, 'SR'].astype(str).tolist())
        return s
    def coauthors_of_pid_final(pid: str) -> set:
        arts = articles_of_pid_final(pid)
        if not arts:
            return set()
        subset = aa[aa['SR'].astype(str).isin(arts)]
        return set(subset['PersonID'].astype(str).tolist()) if 'PersonID' in subset.columns else set()
    def orcid_of_pid(pid: str) -> str:
        if pid.startswith('ORCID:'):
            return pid
        for i in groups.get(pid, []):
            oc = _norm_orcid(str(au.loc[i, 'Orcid']))
            if oc:
                return 'ORCID:' + oc
        return ''
    # Representative email domain
    pid_email_domain: Dict[str, str] = {}

    # Build name signature map
    for pid, idxs in groups.items():
        best = _best_row([au.loc[j] for j in idxs])
        rep = best.get('AuthorFullName', '') or best.get('AuthorName', '')
        pid_rep_sig[pid] = _name_signature(rep)
        # email domain
        dom = ''
        for j in idxs:
            d = email_domain(str(au.loc[j].get('Email','')))
            if d:
                dom = d; break
        pid_email_domain[pid] = dom

    # Build clusters table
    cluster_rows: List[Dict[str, str]] = []
    for pid, idxs in groups.items():
        cluster_id = 'CLUSTER:' + pid_rep_sig.get(pid, '')
        fullname = ''
        orcid_url = ''
        oa_ids: List[str] = []
        for j in idxs:
            r = au.loc[j]
            if not fullname:
                fullname = str(r.get('AuthorFullName','') or r.get('AuthorName',''))
            oc = _norm_orcid(str(r.get('Orcid','')))
            if oc:
                orcid_url = f'https://orcid.org/{oc}'
            oid = _norm_openalex_author(str(r.get('OpenAlexAuthorID','')))
            if oid:
                oa_ids.append('https://openalex.org/' + oid)
        arts = articles_of_pid_final(pid)
        cluster_rows.append({
            'ClusterID': cluster_id,
            'PersonID': pid,
            'AuthorFullName': fullname,
            'Orcid': orcid_url,
            'OpenAlexAuthorID': '; '.join(sorted(set(oa_ids))) if oa_ids else '',
            'Articles': len(arts),
            'EmailDomain': pid_email_domain.get(pid, ''),
        })

    # Build conflicts review: within same signature, produce pairs with metrics
    review_rows: List[Dict[str, str]] = []
    from itertools import combinations
    # Group pids by signature
    sig_to_pids: Dict[str, List[str]] = {}
    for p, sig in pid_rep_sig.items():
        sig_to_pids.setdefault(sig, []).append(p)
    def jaccard(a: set, b: set) -> float:
        if not a and not b:
            return 0.0
        inter = len(a & b)
        union = len(a | b)
        return inter/union if union else 0.0
    for sig, pids in sig_to_pids.items():
        if len(pids) < 2:
            continue
        for a_pid, b_pid in combinations(pids, 2):
            if a_pid == b_pid:
                continue
            # Metrics
            a_orcid = orcid_of_pid(a_pid)
            b_orcid = orcid_of_pid(b_pid)
            arts_a = articles_of_pid_final(a_pid)
            arts_b = articles_of_pid_final(b_pid)
            co_a = coauthors_of_pid_final(a_pid)
            co_b = coauthors_of_pid_final(b_pid)
            review_rows.append({
                'NameSignature': sig,
                'PID_A': a_pid,
                'PID_B': b_pid,
                'ORCID_A': a_orcid,
                'ORCID_B': b_orcid,
                'Articles_A': len(arts_a),
                'Articles_B': len(arts_b),
                'Jaccard_Articles': round(jaccard(arts_a, arts_b), 3),
                'Jaccard_Coauthors': round(jaccard(co_a, co_b), 3),
                'EmailDomain_A': pid_email_domain.get(a_pid, ''),
                'EmailDomain_B': pid_email_domain.get(b_pid, ''),
            })
    authors_out.to_csv(authors_csv, index=False)
    aa_out.to_csv(aa_csv, index=False)
    # Remap All_Affiliation with global PersonID if available
    try:
        if os.path.exists(aff_csv):
            aff = pd.read_csv(aff_csv)
            aff_out = aff.copy()
            if 'PersonID' in aff_out.columns:
                aff_out['PersonID'] = aff_out['PersonID'].astype(str).map(lambda x: pid_map.get(x, x))
            elif 'AuthorID' in aff_out.columns:
                aff_out = aff_out.merge(authors_out[['AuthorID','PersonID']].drop_duplicates(), how='left', on='AuthorID')
            subset = [c for c in ['SR','PersonID','Affiliation'] if c in aff_out.columns]
            if subset:
                aff_out = aff_out.drop_duplicates(subset=subset)
            aff_out.to_csv(aff_csv, index=False)
    except Exception:
        pass
    try:
        if alias_rows:
            pd.DataFrame(alias_rows).to_csv(os.path.join(all_dir, 'All_AuthorAlias.csv'), index=False)
        if conflicts:
            pd.DataFrame(conflicts).to_csv(os.path.join(all_dir, 'All_AuthorConflicts.csv'), index=False)
        if cluster_rows:
            pd.DataFrame(cluster_rows).to_csv(os.path.join(all_dir, 'All_AuthorClusters.csv'), index=False)
        if review_rows:
            pd.DataFrame(review_rows).to_csv(os.path.join(all_dir, 'All_AuthorConflictsReview.csv'), index=False)
    except Exception:
        pass

    return authors_out, aa_out
