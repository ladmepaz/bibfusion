import pandas as pd

def enrich_scopus_author_data(scopus_authors: pd.DataFrame) -> pd.DataFrame:

    df = scopus_authors.copy()

    # -----------------------------
    # 1. Exclude ANONYMOUS
    # -----------------------------
    df = df[df["AuthorName"].str.upper() != "ANONYMOUS"].copy()
    if df.empty:
        return scopus_authors

    # -----------------------------
    # 2. Most common FullName by AuthorName
    # -----------------------------
    most_common_fullname = (
        df.groupby("AuthorName")["AuthorFullName"]
        .agg(lambda x: x.value_counts().idxmax())
    )

    # Replace where AuthorFullName == AuthorName
    mask_fullname = df["AuthorFullName"] == df["AuthorName"]

    df.loc[mask_fullname, "AuthorFullName"] = (
        df.loc[mask_fullname, "AuthorName"].map(most_common_fullname)
    )

    # -----------------------------
    # 3. Most common Orcid by AuthorName (valid)
    # -----------------------------
    valid_mask = (
        df["Orcid"].notna() &
        (df["Orcid"] != "") &
        (df["Orcid"].str.upper() != "NO ORCID")
    )

    most_common_orcid = (
        df[valid_mask]
        .groupby("AuthorName")["Orcid"]
        .agg(lambda x: x.value_counts().idxmax())
    )

    # -----------------------------
    # 4. Fill missing Orcid
    # -----------------------------
    missing_mask = (
        df["Orcid"].isna() |
        (df["Orcid"] == "") |
        (df["Orcid"].str.upper() == "NO ORCID")
    )

    df.loc[missing_mask, "Orcid"] = (
        df.loc[missing_mask, "AuthorName"].map(most_common_orcid)
    )

    # -----------------------------
    # 5. Strip final
    # -----------------------------
    df["AuthorName"] = df["AuthorName"].astype(str).str.strip()
    df["AuthorFullName"] = df["AuthorFullName"].astype(str).str.strip()

    return df
