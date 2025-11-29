import pandas as pd
from itertools import combinations
from pathlib import Path


def build_main_coauthor_edges(all_dir: str, out_edges: str = "MainArticles_Edges.csv") -> pd.DataFrame:
    """
    Build coauthor edges from MainArticles_Authors.csv (SR, AuthorFullName).
    Outputs an edge list with columns: from, to, SR.
    """
    all_dir = Path(all_dir)
    main_authors_path = all_dir / "MainArticles_Authors.csv"
    if not main_authors_path.exists():
        raise FileNotFoundError(f"{main_authors_path} not found. Run build_top_authors first.")

    df = pd.read_csv(main_authors_path)
    if not {"SR", "AuthorFullName"}.issubset(df.columns):
        raise ValueError("MainArticles_Authors.csv must contain SR and AuthorFullName columns")

    df["SR"] = df["SR"].astype(str)
    df["AuthorFullName"] = df["AuthorFullName"].astype(str)

    rows = []
    for sr, grp in df.groupby("SR"):
        authors = sorted(set(grp["AuthorFullName"].tolist()))
        if len(authors) < 2:
            continue
        for a, b in combinations(authors, 2):
            rows.append({"from": a, "to": b, "SR": sr})

    edges = pd.DataFrame(rows, columns=["from", "to", "SR"])
    out_path = all_dir / out_edges
    edges.to_csv(out_path, index=False)
    print("Guardado:", out_path)
    return edges


if __name__ == "__main__":
    build_main_coauthor_edges("all_data_wos_scopus")
