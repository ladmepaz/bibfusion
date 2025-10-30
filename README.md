# Preprocessing Package

Este paquete contiene funciones para procesar datos bibliográficos desde diferentes fuentes.

## Funciones

-   **`wos_df()`**: Transforma archivos .txt de Web of Science a DataFrames de pandas.
-   **`scopus_df()`**: Convierte archivos .bib de Scopus a DataFrames de pandas.
-   **`doi_crossref()`**: Realiza una consulta a la API de Crossref y extrae información de un DOI.
-   **`scopus_ref()`**: Gestiona referencias de artículos, encontrando conexiones entre ellas.

## Instalación

Usa el siguiente comando para instalar las dependencias necesarias:

```
$ pip install -r requirements.txt
```

## Uso

Ejemplo de cómo usar el paquete:

```python
from preprocessing import wos_df, scopus_df

df_wos = wos_df()
df_scopus = scopus_df()
```

## Pipeline

- See `docs/pipeline.md` for a concise end‑to‑end overview of the WoS preprocessing flow, key steps, and main outputs.

## Affiliations: Column Conventions

- `affiliations` (WoS main articles only):
  - Preserves the original WoS bracketed, per‑author format, e.g. `[LASTNAME, FIRSTNAME] AFFILIATION; ...`.
  - Produced by the WoS parser and kept for rows where `ismainarticle == TRUE`.

- `affiliation_2` (OpenAlex references only):
  - Stores institutions returned by OpenAlex per reference/authorship.
  - Normalized to uppercase ASCII and semicolon‑separated.
  - Used for rows where `ismainarticle == FALSE` to avoid mixing formats with `affiliations`.

- Rationale:
  - Keeps WoS main article affiliations intact and readable per author.
  - Keeps OpenAlex reference institutions separate, with a consistent flat format.

- Implementation notes:
  - `enrich_references_with_openalex` writes institutions into `affiliation_2` (uppercase ASCII).
  - `merge_wos_ref` preserves both columns in the merged dataframe.
  - See also `docs/issue-author-affiliation-mapping.md` for a proposed per‑author affiliation edge table.
