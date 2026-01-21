import requests
import urllib.parse
import re

def get_openalex_metadata(title: str) -> dict | None:
    """
    Given a paper title, query OpenAlex and return a dict with:
      - openalex_id: the OpenAlex work ID
      - doi
      - authors: list of rich author dicts
      - abstract
      - keywords
      - journal
      - source: dict with id, display_name, issn_l, issn list,
                is_oa, is_in_doaj, is_indexed_in_scopus, is_core,
                host_organization, host_organization_name

    Returns None if no match is found.
    """
    # 1) Query OpenAlex
    q = urllib.parse.quote(title)
    url = f"https://api.openalex.org/works?filter=title.search:{q}&per-page=1"
    resp = requests.get(url)
    resp.raise_for_status()
    results = resp.json().get("results", [])
    if not results:
        return None
    work = results[0]

    # 2) OpenAlex work ID
    openalex_id = work.get("id")

    # 3) DOI
    doi = work.get("doi")

    # 4) Authorships → rich author objects
    authors = []
    for a in work.get("authorships", []):
        person = a.get("author", {})
        insts = [
            {
                "id": i.get("id"),
                "display_name": i.get("display_name"),
                "ror": i.get("ror"),
                "country_code": i.get("country_code"),
                "type": i.get("type"),
            }
            for i in a.get("institutions", [])
        ]
        affs = [
            {
                "raw_affiliation_string": af.get("raw_affiliation_string"),
                "institution_ids": af.get("institution_ids", []),
            }
            for af in a.get("affiliations", [])
        ]
        authors.append({
            "display_name": person.get("display_name"),
            "orcid": person.get("orcid"),
            "author_position": a.get("author_position"),
            "raw_author_name": a.get("raw_author_name"),
            "is_corresponding": a.get("is_corresponding"),
            "raw_affiliation_strings": a.get("raw_affiliation_strings", []),
            "institutions": insts,
            "affiliations": affs,
        })

    # 5) Abstract: rebuild from inverted index if present
    inv = work.get("abstract_inverted_index")
    if inv:
        max_pos = max(pos for poses in inv.values() for pos in poses)
        tokens = [""] * (max_pos + 1)
        for w, poses in inv.items():
            for p in poses:
                tokens[p] = w
        abstract = " ".join(tokens).strip()
    else:
        abstract = work.get("abstract", "") or ""

    # 6) Keywords (concepts)
    keywords = [c["display_name"] for c in work.get("concepts", [])]

    # 7) Journal name
    journal = (
        work.get("host_venue", {}).get("display_name")
        or work.get("primary_location", {}).get("source", {}).get("display_name")
    )

    # 8) Source details
    src = work.get("primary_location", {}).get("source", {})
    source = {
        "id":                     src.get("id"),
        "display_name":          src.get("display_name"),
        "issn_l":                src.get("issn_l"),
        "issn":                  src.get("issn", []),
        "is_oa":                 src.get("is_oa"),
        "is_in_doaj":            src.get("is_in_doaj"),
        "is_indexed_in_scopus":  src.get("is_indexed_in_scopus"),
        "is_core":               src.get("is_core"),
        "host_organization":     src.get("host_organization"),
        "host_organization_name":src.get("host_organization_name"),
    }

    return {
        "openalex_id": openalex_id,
        "doi":         doi,
        "authors":     authors,
        "abstract":    abstract,
        "keywords":    keywords,
        "journal":     journal,
        "source":      source,
    }
