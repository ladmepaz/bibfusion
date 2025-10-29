Title: Robust author–affiliation mapping across WoS and OpenAlex

Summary
- Affiliations in WoS appear in two formats: a bracketed, per‑author form in `affiliations` (e.g., `[LAST, FIRST] Affil; [LAST, FIRST] …`) and a flat, semi‑colon‑separated form in `affiliation_2`.
- References enriched via OpenAlex expose affiliations per authorship via `authorships[*].institutions[*].display_name`.
- Current pipeline cannot unambiguously attach multiple affiliations to individual authors, especially when author counts ≠ affiliation counts (e.g., one author with two affiliations).

Problem
- `wos_author_raw` only holds a single `Affiliation` per author and loses multi‑affiliation.
- `affiliation_2` is ambiguous (flat list), cannot be safely aligned when counts differ.
- We need a structure that preserves 1:N author–affiliation relationships with clear provenance.

Proposed Solution
1) Canonical per‑author source: parse WoS `affiliations` with bracketed names `[LAST, FIRST] Affiliation…;`. For each bracketed block, emit one row per affiliation for that author.
   - If the same author appears multiple times (e.g., two affiliations), assign `affiliation_rank = 1,2,…` and set `is_primary = True` for rank 1.
2) OpenAlex integration: extract `authorships[*].institutions[*].display_name` per author (aligned by authorship order). Emit the same rows with `source = 'openalex'` and `affiliation_rank` by position within institutions.
3) Safe fallback (only if neither 1 nor 2 are available): when `affiliation_2` count equals author count exactly, align by position with `source = 'affiliation_2'`. Otherwise, skip mapping (avoid incorrect assignments).
4) Data products:
   - New edge table `AuthorAffiliationEdge` with columns:
     - `SR`, `openalex_work_id`, `AuthorOrder`, `AuthorFullName`, `OpenAlexAuthorID`,
       `AffiliationRaw`, `affiliation_rank`, `is_primary` (bool), `source`.
   - In `wos_author_raw`: keep `Affiliation` = primary affiliation (rank 1) and add `AffiliationsAll` = joined list of all affiliations for the author.
5) Matching strategy:
   - Prefer `OpenAlexAuthorID` when available; otherwise, match by standardized `AuthorFullName` (reuse existing standardization).

Acceptance Criteria
- For a record with 4 authors and 5 affiliations where one author has two affiliations, `AuthorAffiliationEdge` contains 5 rows with correct `affiliation_rank`, and `wos_author_raw` shows that author’s primary in `Affiliation` and both in `AffiliationsAll`.
- For references enriched via OpenAlex, author institutions appear in `AuthorAffiliationEdge` with `source = 'openalex'` and correct author alignment.
- When counts mismatch and no per‑author markers or OpenAlex data exist, no incorrect mappings are created.

Implementation Outline
- Parser: extend `get_wos_author_data` to parse bracketed `affiliations`, emitting per‑author affiliation rows and aggregating to `Affiliation` (rank 1) and `AffiliationsAll`.
- OpenAlex: during `enrich_references_with_openalex`, retain authorship institutions; later, when merging, emit edges for references too.
- Assembly: create a helper `build_author_affiliation_edges(wos_author_raw, wos_df_3)` returning the `AuthorAffiliationEdge` dataframe.
- Ordering: maintain authorship order and institution order as ranks.
- Provenance: populate `source` with one of `wos_affiliations`, `openalex`, `affiliation_2`.

Schema Changes
- `wos_author_raw`: add `AffiliationsAll` (string, `;` separated) next to `Affiliation` (primary only).
- New CSV output: `AuthorAffiliationEdge.csv` with the columns listed above.

Backward Compatibility
- Existing consumers of `Affiliation` continue to work (now receiving rank‑1 affiliation).
- New edge table is additive and optional for downstream steps.

Risks and Mitigations
- Name matching ambiguity: prefer `OpenAlexAuthorID`; otherwise, use existing normalized `AuthorFullName` matching utilities.
- Ambiguous `affiliation_2`: apply only when counts match; otherwise skip to avoid false positives.

Open Questions
- Should we prioritize WoS vs OpenAlex when both provide affiliations but conflict? (Default suggestion: prefer WoS for main articles, OpenAlex for references.)
- Do we need institution IDs (ROR/OpenAlex) for dedup? If yes, add optional normalization step later.

Definition of Done
- New edge table generated for WoS main articles; optional generation for references when available.
- `wos_author_raw` includes `AffiliationsAll` and preserves `Affiliation` as primary.
- Documentation updated with schema and usage.

