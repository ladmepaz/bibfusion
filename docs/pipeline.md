# WoS Pipeline Overview

This document summarizes the end‑to‑end WoS preprocessing flow and main artifacts.

## High‑Level Flow

1) WoS import
   - `wos_txt_to_df(path_wos)` → `wos_df`
   - `remove_duplicates_df(wos_df)` → deduped `wos_df`

2) Enrich WoS main articles (authors)
   - `enrich_wos_with_openalex_authors(wos_df, replace=True, keep_raw=False, uppercase_ascii=True)`
   - Replaces `author_full_names`/`orcid` with OpenAlex data; adds `author_id_openalex`, `openalex_work_id`.

3) References extraction + enrichment
   - `get_wos_references(wos_df)` → `wos_references` (final_df) + `wos_citation`
   - `enrich_references_with_openalex(wos_references)` → `wos_ref_enriched`
     - `author_full_names` in UPPERCASE ASCII
     - `affiliation_2` (OpenAlex institutions, UPPERCASE ASCII)
     - `author_id_openalex`, `openalex_work_id`

4) Merge main articles + references
   - `merge_wos_ref(wos_df, wos_ref_enriched)` → `wos_df_3`
   - Adds `ismainarticle` and keeps both `affiliations` (WoS main) and `affiliation_2` (references).

5) Journal metadata (Scimago)
   - `standarize_journal_data` → `wos_df_4`
   - `fill_missing_issn_eissn_with_scimago` → `wos_df_5`
   - `aggregate_sr_and_attach_scimago_ids` → `wos_df_6`
   - `resolve_duplicate_sourceids` + `add_year_and_scimago_info`

6) Author tables from merged data
   - `get_wos_author_data(wos_df_3)` → `wos_author_raw`
     - Per‑author: `SR`, `AuthorOrder`, `AuthorName`, `AuthorFullName`, `Affiliation`, `CorrespondingAuthor`, `Orcid`, `ResearcherID`, `Email`
     - Also: `OpenAlexAuthorID`, `openalex_work_id`
   - `enrich_wos_author_data` → `wos_author_enriched`
   - `unify_author_fullname_and_orcid` →
     - `Author.csv`: unified names/ORCID, stable `AuthorID` (prefers ORCID → OpenAlex → NAME)
     - `ArticleAuthor.csv`: adds `openalex_work_id`, `OpenAlexAuthorID`
     - `10_temp_wos_author_affiliation.csv`: base affiliations per author
   - Countries
     - `fill_missing_affiliations` + `extract_countries` → `Affiliation.csv`

7) Article entity + ToS
   - `get_article_entity(wos_df_3)` → `Article.csv`
   - `get_citation_network(wos_citation)` → clean → branches → `get_tos` → `tos_df_nodes.csv` / `tos_df_edges.csv` → `TreeOfScience.csv`

8) Author consolidation (person‑level)
   - `consolidate_authors(Author.csv)` →
     - `AuthorPerson.csv` (canonical PersonID, ORCID, OpenAlexIDs, Emails, NameVariants)
     - `AuthorAlias.csv` (map from original author rows to PersonID)
     - `AuthorConflicts.csv` (ambiguous ORCID/OpenAlex cases)

9) Export
   - `export_csvs_as_excel(output_dir)` → `data.xlsx`

## Affiliation Columns

- `affiliations` (WoS main only): bracketed, per‑author format `[LAST, FIRST] AFFIL; ...`.
- `affiliation_2` (references only): OpenAlex institutions, UPPERCASE ASCII, `;` separated.

## Notes

- OpenAlex DOI coverage is partial; some references won’t enrich.
- AuthorID in `Author.csv` prefers ORCID; if one ORCID maps to multiple OpenAlex IDs, the unify step may use OA to avoid collapsing distinct profiles.
- For analyses, prefer `AuthorPerson.csv` with `AuthorAlias.csv` for lookups.

