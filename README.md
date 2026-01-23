# Preprocessing Package

This package provides utilities to preprocess, clean, and harmonize bibliographic data from multiple scientific sources, primarily **Web of Science (WoS)** and **Scopus**.  
It is designed to support bibliometric and scientometric analyses by transforming raw exports into structured pandas DataFrames.

⚠️ **Status:** under active development. APIs and internal structures may change.

---

## Features

- Parsing of raw bibliographic exports into structured DataFrames
- Support for multiple data sources (WoS, Scopus, Crossref, OpenAlex)
- Reference enrichment and linkage across sources
- Designed for reproducible research workflows

---

## Core Functions

- **`wos_df()`**  
  Transforms Web of Science `.txt` export files into pandas DataFrames.

- **`scopus_df()`**  
  Converts Scopus `.bib` export files into pandas DataFrames.

- **`doi_crossref()`**  
  Queries the Crossref API to retrieve metadata associated with a given DOI.

- **`scopus_ref()`**  
  Processes and links article references, identifying relationships between cited documents.

---

## Installation

Clone the repository and install the required dependencies:

```bash
pip install -r requirements.txt
