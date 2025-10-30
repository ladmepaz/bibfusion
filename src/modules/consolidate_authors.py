import re
import unicodedata
from typing import Tuple, List

import pandas as pd


def _ascii_upper(text: str) -> str:
    if not isinstance(text, str):
        return ''
    norm = unicodedata.normalize('NFKD', text)
    stripped = ''.join(ch for ch in norm if not unicodedata.combining(ch))
    return stripped.upper().strip()


def _norm_orcid(val: str) -> str:
    if not isinstance(val, str):
        return ''
    s = val.strip()
    if not s or s.upper() == 'NO ORCID':
        return ''
    # Accept bare id or full URL
    s = s.split('/')[-1]
    return s.upper()


def _short_openalex(val: str) -> str:
    if not isinstance(val, str):
        return ''
    s = val.strip()
    return s.split('/')[-1].upper() if s else ''


def _name_key(fullname: str) -> str:
    if not isinstance(fullname, str):
        return ''
    name = _ascii_upper(fullname)
    # If comma present: FAMILY, GIVEN
    if ',' in name:
        family, given = [p.strip() for p in name.split(',', 1)]
    else:
        parts = name.split()
        family = parts[-1] if parts else ''
        given = ' '.join(parts[:-1]) if len(parts) > 1 else ''
    initials = ''.join(p[0] for p in re.split(r"[-\s]+", given) if p)
    family = family.replace('-', ' ').strip()
    family = ' '.join(family.split())
    return f"{family}|{initials}"


def consolidate_authors(author_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Consolidate author rows (e.g., from WoS_results/Author.csv) into person-level identities.

    Returns (author_person, author_alias, flagged_conflicts).
    - author_person: one row per person with canonical fields (PersonID, CanonicalName, Orcid, OpenAlexIDs,...)
    - author_alias: mapping of original Author rows -> PersonID
    - flagged_conflicts: detected OA<->ORCID inconsistencies
    """
    df = author_df.copy()

    # Ensure expected columns exist
    for col in ['AuthorID', 'AuthorFullName', 'AuthorName', 'Orcid', 'Email', 'OpenAlexAuthorID']:
        if col not in df.columns:
            df[col] = ''

    # Normalized helper columns
    df['__orcid'] = df['Orcid'].astype(str).apply(_norm_orcid)
    df['__oa'] = df['OpenAlexAuthorID'].astype(str).apply(_short_openalex)
    df['__name_key'] = df['AuthorFullName'].astype(str).apply(_name_key)
    df['__email'] = df['Email'].astype(str).str.strip().str.lower()

    # Step 1: group by ORCID when present
    groups = {}
    assigned_idx = set()

    for oc, g in df[df['__orcid'] != ''].groupby('__orcid'):
        idxs = list(g.index)
        groups[f'ORCID:{oc}'] = idxs
        assigned_idx.update(idxs)

    # Step 2: remaining with OpenAlex ID
    remaining = df.index.difference(list(assigned_idx))
    for oa, g in df.loc[remaining][df.loc[remaining]['__oa'] != ''].groupby('__oa'):
        idxs = list(g.index)
        groups[f'OA:{oa}'] = idxs
        assigned_idx.update(idxs)

    # Step 3: remaining with name_key + email
    remaining = df.index.difference(list(assigned_idx))
    with_email = df.loc[remaining][df.loc[remaining]['__email'] != '']
    for _, g in with_email.groupby(['__name_key', '__email']):
        idxs = list(g.index)
        # Build a deterministic PersonID
        nk = g['__name_key'].iloc[0]
        em = g['__email'].iloc[0]
        groups[f'NAMEEMAIL:{nk}|{em}'] = idxs
        assigned_idx.update(idxs)

    # Step 4: remaining by name_key only
    remaining = df.index.difference(list(assigned_idx))
    for nk, g in df.loc[remaining].groupby('__name_key'):
        idxs = list(g.index)
        groups[f'NAME:{nk}'] = idxs
        assigned_idx.update(idxs)

    # Build author_alias
    alias_rows: List[dict] = []
    for pid, idxs in groups.items():
        for i in idxs:
            r = df.loc[i]
            alias_rows.append({
                'PersonID': pid,
                'AuthorID': r.get('AuthorID', ''),
                'AuthorFullName': r.get('AuthorFullName', ''),
                'AuthorName': r.get('AuthorName', ''),
                'Orcid': r.get('Orcid', ''),
                'Email': r.get('Email', ''),
                'OpenAlexAuthorID': r.get('OpenAlexAuthorID', ''),
            })
    author_alias = pd.DataFrame(alias_rows)

    # Build author_person aggregation
    person_rows: List[dict] = []
    for pid, g in author_alias.groupby('PersonID'):
        names = [n for n in g['AuthorFullName'].astype(str) if n]
        # Choose canonical name: longest
        canonical_name = max(names, key=len) if names else ''
        orcids = sorted({ _norm_orcid(o) for o in g['Orcid'].astype(str) if _norm_orcid(o) })
        orcid_primary = orcids[0] if orcids else ''
        oas = sorted({ _short_openalex(o) for o in g['OpenAlexAuthorID'].astype(str) if _short_openalex(o) })
        emails = sorted({ e for e in g['Email'].astype(str).str.lower() if e })
        person_rows.append({
            'PersonID': pid,
            'CanonicalName': canonical_name,
            'Orcid': orcid_primary,
            'OpenAlexIDs': '; '.join(oas) if oas else '',
            'Emails': '; '.join(emails) if emails else '',
            'NameVariants': '; '.join(sorted(set(names))) if names else '',
            'AliasCount': len(g),
        })
    author_person = pd.DataFrame(person_rows)

    # Conflicts: OA -> multiple ORCIDs (non-empty)
    conflicts: List[dict] = []
    if '__oa' in df.columns:
        for oa, g in df[df['__oa'] != ''].groupby('__oa'):
            orcids = sorted({ oc for oc in g['__orcid'] if oc })
            if len(orcids) > 1:
                conflicts.append({'type': 'OA_to_multiple_ORCID', 'openalex_id': oa, 'orcids': '; '.join(orcids)})

    # Conflicts: NAMEKEY+EMAIL -> multiple ORCIDs
    for (nk, em), g in df.groupby(['__name_key', '__email']):
        orcids = sorted({ oc for oc in g['__orcid'] if oc })
        if len(orcids) > 1:
            conflicts.append({'type': 'NameEmail_to_multiple_ORCID', 'name_key': nk, 'email': em, 'orcids': '; '.join(orcids)})

    flagged_conflicts = pd.DataFrame(conflicts)

    return author_person, author_alias, flagged_conflicts


def consolidate_authors_from_csv(path: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    df = pd.read_csv(path)
    return consolidate_authors(df)

