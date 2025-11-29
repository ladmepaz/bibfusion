import pandas as pd
from itertools import combinations
from pathlib import Path


def build_main_coauthor_edges(
    all_dir: str,
    out_edges: str = "AllArticles_Edges.csv",
) -> pd.DataFrame:
    """
    Build coauthor edges from all articles (main + references) using All_ArticleAuthor + All_Authors.
    Outputs an edge list with columns: Source, Target, SR (Gephi-friendly).
    """
    all_dir = Path(all_dir)
    aa_path = all_dir / "All_ArticleAuthor.csv"
    au_path = all_dir / "All_Authors.csv"
    if not aa_path.exists() or not au_path.exists():
        raise FileNotFoundError("All_ArticleAuthor.csv or All_Authors.csv not found in " + str(all_dir))

    aa = pd.read_csv(aa_path)
    au = pd.read_csv(au_path)

    # Determine join key
    key = "PersonID" if ("PersonID" in aa.columns and "PersonID" in au.columns) else "AuthorID"
    aa[key] = aa[key].astype(str)
    au[key] = au[key].astype(str)

    # Attach AuthorFullName
    aa = aa.merge(au[[key, "AuthorFullName"]], on=key, how="left")

    # Build edges per SR
    rows = []
    aa["SR"] = aa["SR"].astype(str)
    for sr, grp in aa.groupby("SR"):
        authors = sorted(set(grp["AuthorFullName"].dropna().astype(str).tolist()))
        if len(authors) < 2:
            continue
        for a, b in combinations(authors, 2):
            rows.append({"Source": a, "Target": b, "SR": sr})

    edges = pd.DataFrame(rows, columns=["Source", "Target", "SR"])
    out_path = all_dir / out_edges
    edges.to_csv(out_path, index=False)
    print("Guardado:", out_path)
    return edges


if __name__ == "__main__":
    build_main_coauthor_edges("all_data_wos_scopus")
