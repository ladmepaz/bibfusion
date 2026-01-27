# BibFusion Data Dictionary (v1.0.0)

This document provides the data dictionary for the **seven core CSV outputs** produced by **BibFusion** (v1.0.0).  
It lists each variable, its operational definition, provenance (source), key transformations/normalizations applied by BibFusion, and notes/examples.

**Repository:** https://github.com/ladmepaz/preprocessing

## Entities (tables)
- [Articles](#articles)
- [Authors](#authors)
- [ArticleAuthor](#articleauthor)
- [Citation](#citation)
- [Affiliation](#affiliation)
- [Journal](#journal)
- [Scimagodb](#scimagodb)

---
## Articles

Canonical work-level records (main query results and reference-only records).

| Variable | Definition | Source | Transformation / normalization | Notes / example |
|---|---|---|---|---|
| abstract | Abstract text when available. | Scopus export; WoS export; OpenAlex | ASCII/uppercase; trimmed. | May be empty for references or some Scopus rows. |
| accession_number | WoS accession number (UT). | WoS export | Trimmed. | Null for Scopus. |
| affiliations | Affiliation text as exported by the source. | Scopus export; WoS export | ASCII/uppercase; trimmed. | Parsed to derive country; may include brackets. |
| article_number | Publisher's article number (if applicable). | Scopus export; WoS export | Trimmed. | Often null; format varies by publisher. |
| author | Primary author string as provided by the source export. | Scopus export; WoS export | ASCII/uppercase; trimmed. | Often abbreviated (e.g., 'GUDAS S'); may be null for references. |
| author_full_names | Full author list as a semicolon-separated string. | Scopus export; WoS export; OpenAlex | ASCII/uppercase; trimmed; OpenAlex values may replace source values. | May be missing for reference-only rows. |
| author_keywords | Author-supplied keywords. | Scopus export; WoS export | ASCII/uppercase; trimmed. | Delimiter varies by source; may be null. |
| authors_id | Source-specific author IDs (often Scopus IDs). | Scopus export | Trimmed; uppercased where applicable. | May be empty for WoS; semicolon-separated. |
| authors_with_affiliations | Author strings paired with affiliations. | Scopus export; WoS export | ASCII/uppercase; trimmed. | May be incomplete; delimiter varies. |
| chemicals_cas | CAS registry numbers (if provided). | WoS export | ASCII/uppercase; trimmed. | Rarely populated. |
| cited_by | Citation count from the source. | Scopus export; WoS export | Trimmed; best-record merge used. | Counts may differ across sources. |
| cited_reference_count | Number of references cited by the work. | WoS export | Trimmed. | Null for Scopus. |
| coden | CODEN identifier. | Scopus export; WoS export | Trimmed. | Rarely populated. |
| conference_code | Conference code/ID. | Scopus export; WoS export | Trimmed. | Rarely populated. |
| conference_date | Conference date. | Scopus export; WoS export | Trimmed. | Null for journal articles. |
| conference_location | Conference location. | Scopus export; WoS export | ASCII/uppercase; trimmed. | Null for journal articles. |
| conference_name | Conference name (if applicable). | Scopus export; WoS export | ASCII/uppercase; trimmed. | Null for journal articles. |
| country | Country tags extracted from affiliations (semicolon-separated). | Derived by BibFusion | Parsed from affiliations; ASCII/uppercase; duplicates collapsed per record. | May be empty when affiliations lack countries; Scopus coverage varies. |
| document_delivery_number | WoS document delivery number. | WoS export | Trimmed. | Null for Scopus. |
| document_type | Document type (e.g., article, conference paper). | Scopus export; WoS export | ASCII/uppercase; trimmed. | May differ across sources; best-record merge used. |
| doi | Digital Object Identifier for the work. | Scopus export; WoS export; OpenAlex | Lowercased/trimmed; normalized for deduplication; invalid DOIs ignored in matching. | Primary key for merge when present; can be null. |
| early_access_date | Early access/online date if available. | WoS export | Trimmed. | Typically null for Scopus rows. |
| editors | Editor names. | Scopus export; WoS export | ASCII/uppercase; trimmed. | Often null. |
| eid | Scopus EID identifier. | Scopus export | Trimmed. | Null for WoS. |
| funding_agency | Funding agency names. | WoS export | ASCII/uppercase; trimmed. | Often null; multiple agencies may be delimited. |
| funding_details | Funding details text. | WoS export | ASCII/uppercase; trimmed. | May be null. |
| funding_texts | Funding acknowledgments text. | Scopus export | ASCII/uppercase; trimmed. | Null for WoS. |
| index_keywords | Indexed keywords from the source. | Scopus export; WoS export | ASCII/uppercase; trimmed. | May be null. |
| isbn | ISBN for books/chapters. | Scopus export; WoS export | Trimmed. | Often null for journal articles. |
| ismainarticle | TRUE if record is from the original search query; FALSE if from references. | Derived by BibFusion | Assigned during ingestion and preserved through merge. | Should be TRUE/FALSE; may be empty for malformed rows. |
| issn | ISSN of the source. | Scopus export; WoS export; Scimago | Trimmed; normalized in journal consolidation. | Can be multiple or empty. |
| journal | Normalized journal name for the work. | Scopus export; WoS export; Scimago; Derived by BibFusion | ASCII/uppercase; trimmed; harmonized across sources. | May differ from source_title; can be null for non-journal items. |
| language | Language of the document. | Scopus export; WoS export | ASCII/uppercase; trimmed. | Often null for references. |
| link | Primary URL link to the record. | Scopus export; WoS export | Trimmed. | May be empty. |
| molecular_sequence_numbers | Molecular sequence identifiers. | WoS export | ASCII/uppercase; trimmed. | Rarely populated. |
| open_access_indicator | Open access indicator/category. | Scopus export; WoS export | ASCII/uppercase; trimmed. | Values vary by source. |
| orcid | ORCID identifiers linked to authors (semicolon-separated). | Scopus export; WoS export; OpenAlex | Trimmed; OpenAlex may provide full URL; stored as-is when available. | Can include 'NO ORCID' or URLs; often null for references. |
| page_count | Page count when available. | Scopus export; WoS export | Trimmed. | May be null or derived from pages. |
| page_end | End page. | Scopus export; WoS export | Trimmed. | May be null or non-numeric. |
| page_start | Start page. | Scopus export; WoS export | Trimmed. | May be null or non-numeric. |
| publication_stage | Publication stage (e.g., final, in press). | Scopus export | ASCII/uppercase; trimmed. | Null for WoS. |
| publisher | Publisher name. | Scopus export; WoS export | ASCII/uppercase; trimmed. | May be null. |
| pubmed_id | PubMed ID (if available). | Scopus export; WoS export | Trimmed. | Rare; may be null. |
| references | Raw reference list string as exported. | Scopus export; WoS export | ASCII/uppercase; trimmed. | Used to build Citation; may be long or empty. |
| source | Source label or content source field from export. | Scopus export; WoS export | ASCII/uppercase; trimmed. | May reflect database/publisher naming. |
| source_title | Source/journal title as provided by the source export. | Scopus export; WoS export | ASCII/uppercase; trimmed. | May include abbreviations; sometimes empty. |
| sources_merged | Provenance indicator for merged sources (scopus, wos, scopus;wos). | Derived by BibFusion | Assigned during merge based on source presence. | Useful for filtering by origin; not null for merged rows. |
| sponsors | Conference sponsors. | Scopus export; WoS export | ASCII/uppercase; trimmed. | Often null. |
| SR | Canonical reference key for the work, used to link citations and merges. | Scopus export; WoS export; Derived by BibFusion | ASCII/uppercase; trimmed; Scopus SR constructed from first author+year; cleanup removes .0 and empty/invalid keys. | May be missing or ambiguous for reference-only rows; used as FK in Citation (SR/SR_ref). |
| subject_category | Subject categories from source export. | Scopus export; WoS export | ASCII/uppercase; trimmed. | Definition varies by source; may be null. |
| title | Title of the work in the unified corpus. | Scopus export; WoS export; OpenAlex | ASCII/uppercase; trimmed; best-record merge prefers richer title. | References may have partial or inconsistent casing; can be null. |
| usage_count_last_180_days | WoS usage metric (last 180 days). | WoS export | Trimmed. | Null for Scopus. |
| usage_count_since_2013 | WoS usage metric (since 2013). | WoS export | Trimmed. | Null for Scopus. |
| web_of_science_categories | WoS subject categories. | WoS export | ASCII/uppercase; trimmed. | Null for Scopus-only rows. |
| year | Publication year of the work. | Scopus export; WoS export; OpenAlex | Normalized to numeric/string; preserved from source. | May be missing for some references. |

---

## Authors

Person-level records anchored by a canonical PersonID (ORCID → OpenAlexAuthorID → normalized name).

| Variable | Definition | Source | Transformation / normalization | Notes / example |
|---|---|---|---|---|
| AuthorFullName | Full author name in the unified corpus. | Scopus export; WoS export; OpenAlex | ASCII/uppercase; trimmed; OpenAlex may replace source value. | May vary across sources; used for display, not unique. |
| AuthorID | Source-specific author identifier as carried into the unified Authors table. | Scopus export; WoS export; OpenAlex | Trimmed; uppercased/ASCII where applicable; preserved through merge. | May contain ORCID/OA/NAME-style IDs; not guaranteed unique. |
| AuthorName | Abbreviated author name as provided by the source. | Scopus export; WoS export | ASCII/uppercase; trimmed. | Often initials (e.g., 'GIRALDO S.R.'); may be null. |
| Email | Author email address when available. | WoS export; Scopus export | Trimmed; uppercased/ASCII if present. | Often null; not reliable for dedup. |
| OpenAlexAuthorID | OpenAlex author identifier or URL resolved via DOI enrichment. | OpenAlex | Trimmed; stored as URL/ID; uppercased/ASCII where applicable. | May be null if OpenAlex lookup failed. |
| Orcid | ORCID associated with the author, if available. | Scopus export; WoS export; OpenAlex | Trimmed; stored as-is (may be URL or bare ID). | Often null; may include 'NO ORCID' from source. |
| PersonID | Canonical author identifier used for deduplication and joins. | Derived by BibFusion | Constructed by priority ORCID ? OpenAlexAuthorID ? normalized name; ASCII/uppercase; trimmed. | Primary FK for ArticleAuthor; may be ambiguous for name-only matches. |
| ResearcherID | WoS ResearcherID when provided. | WoS export | Trimmed; uppercased/ASCII where applicable. | Null for Scopus-only rows. |

---

## ArticleAuthor

Authorship linkage table connecting Articles (SR) to Authors (PersonID), preserving author order.

| Variable | Definition | Source | Transformation / normalization | Notes / example |
|---|---|---|---|---|
| AuthorID | Source-specific author identifier from the original export. | Scopus export; WoS export; OpenAlex | Trimmed; uppercased/ASCII when applicable; preserved through merge. | May be ORCID/OA/NAME-style; not unique across sources. |
| AuthorOrder | Author position in the byline (1=first author). | Scopus export; WoS export | Parsed to numeric/string; preserved from source. | May be null or inconsistent for references. |
| CorrespondingAuthor | Flag indicating corresponding author when provided. | Scopus export; WoS export | Normalized to TRUE/FALSE when available. | Often null; not consistently reported across sources. |
| openalex_work_id | OpenAlex work identifier associated with the article. | OpenAlex | Trimmed; stored as URL/ID when available. | Often null when DOI lookup fails; can aid cross-source linking. |
| OpenAlexAuthorID | OpenAlex author identifier resolved via DOI enrichment. | OpenAlex | Trimmed; stored as URL/ID when available. | May be null if OpenAlex lookup fails or author not matched. |
| PersonID | Canonical author identifier used for deduplication and joins. | Derived by BibFusion | Constructed by priority ORCID ? OpenAlexAuthorID ? normalized name; ASCII/uppercase; trimmed. | Primary FK to Authors.PersonID; ambiguity possible for name-only matches. |
| SR | Canonical reference key for the article used to link to Articles. | Scopus export; WoS export; Derived by BibFusion | ASCII/uppercase; trimmed; standardized SR built/cleaned during ingestion and merge. | Primary FK to Articles.SR; may be missing for malformed refs. |

---

## Citation

Directed citation edges (citing SR → cited SR_ref), where SR_ref may be FK-like and optional.

| Variable | Definition | Source | Transformation / normalization | Notes / example |
|---|---|---|---|---|
| SR | Canonical reference key for the citing work; links to Articles.SR when available. | Scopus export; WoS export; Derived by BibFusion | Parsed from source references or main records; ASCII/uppercase; trimmed; .0 removed; invalid/empty keys dropped. | FK-like to Articles.SR; may be missing if source reference is malformed. |
| SR_ref | Canonical reference key for the cited work; links to Articles.SR when resolved. | Scopus export; WoS export; Derived by BibFusion | Parsed from raw reference strings; ASCII/uppercase; trimmed; year-only/empty/dash references removed; standardized format. | FK-like and may not resolve to an Articles row for out-of-coverage citations. |

---

## Affiliation

Affiliation/address-level information used for institutional and country extraction.

| Variable | Definition | Source | Transformation / normalization | Notes / example |
|---|---|---|---|---|
| Affiliation | Affiliation text string as provided by source, linked to a specific author and article. | Scopus export; WoS export | ASCII/uppercase; trimmed; basic cleanup of punctuation/spacing. | Multiple affiliations per author may appear as separate rows; may be incomplete. |
| AuthorID | Operational author identifier associated with the affiliation string. | Scopus export; WoS export | Trimmed; uppercased/ASCII where applicable; preserved through merge. | In this release uses AuthorID (not PersonID); may be ambiguous across sources. |
| Country | Country extracted from the affiliation string. | Derived by BibFusion | Parsed from affiliation; ASCII/uppercase; duplicates collapsed when generated. | Can be empty if country is not explicit; may reflect city/state strings. |
| SR | Canonical reference key for the article linked to the affiliation record. | Scopus export; WoS export; Derived by BibFusion | ASCII/uppercase; trimmed; standardized SR built/cleaned during ingestion and merge. | FK-like to Articles.SR; may be missing for malformed refs. |

---

## Journal

Normalized journal outlet table used to create stable journal_id for joining journal-level metadata.

| Variable | Definition | Source | Transformation / normalization | Notes / example |
|---|---|---|---|---|
| journal | Normalized journal title used for consolidation across sources. | Scopus export; WoS export; Derived by BibFusion | ASCII/uppercase; trimmed; standardized to reduce variants. | May differ from source_title; can be null if missing in source. |
| journal_id | Canonical journal identifier assigned during normalization and deduplication. | Derived by BibFusion | Generated by clustering normalized titles/ISSNs; ASCII/uppercase; trimmed. | Used to join Articles with Journal/Scimagodb; may collapse variants. |
| source_title | Journal/source title as provided by the original export. | Scopus export; WoS export | ASCII/uppercase; trimmed. | May include abbreviations or variants; not unique. |

---

## Scimagodb

Journal-level metrics (e.g., SJR/quartiles) joined via journal_id / ISSN/EISSN/title rules.

| Variable | Definition | Source | Transformation / normalization | Notes / example |
|---|---|---|---|---|
| %Female | TBD | TBD | TBD | TBD |
| Areas | TBD | TBD | TBD | TBD |
| Categories | TBD | TBD | TBD | TBD |
| Citable Docs. (3years) | TBD | TBD | TBD | TBD |
| Citations / Doc. (2years) | TBD | TBD | TBD | TBD |
| Country | TBD | TBD | TBD | TBD |
| Coverage | TBD | TBD | TBD | TBD |
| eIssn | Electronic ISSN from Scimago for the journal. | Scimago | Trimmed; normalized for join (remove spaces/punct where needed). | May be empty; used with Issn/title to match journals. |
| H index | TBD | TBD | TBD | TBD |
| Issn | Primary ISSN from Scimago for the journal. | Scimago | Trimmed; normalized for join (remove spaces/punct where needed). | May contain multiple ISSNs or be empty. |
| journal_abbr | TBD | TBD | TBD | TBD |
| journal_id | Canonical journal identifier used to link Scimagodb to Journal. | Derived by BibFusion | Assigned during journal normalization (title/ISSN clustering); ASCII/uppercase; trimmed. | FK to Journal.journal_id; may merge title variants. |
| Overton | TBD | TBD | TBD | TBD |
| Publisher | TBD | TBD | TBD | TBD |
| Rank | Scimago rank for the journal/year. | Scimago | Trimmed; numeric where possible. | May be missing or non-numeric for some records. |
| Ref. / Doc. | TBD | TBD | TBD | TBD |
| Region | TBD | TBD | TBD | TBD |
| SDG | TBD | TBD | TBD | TBD |
| SJR | TBD | TBD | TBD | TBD |
| SJR Best Quartile | TBD | TBD | TBD | TBD |
| SR | Reference key sometimes used as a seed link to articles/journals. | Derived by BibFusion | ASCII/uppercase; trimmed; carried through joins when available. | May be null; not a primary Scimago field. |
| Title | Journal title as provided by Scimago. | Scimago | ASCII/uppercase; trimmed; preserved for matching. | May differ from source_title/journal in Articles. |
| Title_clean | TBD | TBD | TBD | TBD |
| Total Citations (3years) | TBD | TBD | TBD | TBD |
| Total Docs. (1999) | TBD | TBD | TBD | TBD |
| Total Docs. (3years) | TBD | TBD | TBD | TBD |
| Total Refs. | TBD | TBD | TBD | TBD |
| Type | Scimago source type (e.g., Journal, Conference). | Scimago | ASCII/uppercase; trimmed. | Null for some rows. |
| year | Scimago metric year for the journal record. | Scimago | Parsed/trimmed; numeric where possible. | Year-specific metrics; not all journals have all years. |
| year.1 | TBD | TBD | TBD | TBD |

---
