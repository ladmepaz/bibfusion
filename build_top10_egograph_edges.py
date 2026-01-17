import pandas as pd
from itertools import combinations
from pathlib import Path


def build_top10_egograph_edges(
    all_dir: str,
    top10_file: str = "Top10_Authors.csv",
    edges_file: str = "AllArticles_Edges.csv",
    out_file: str = "Top10_Egograph_Edges.csv",
) -> pd.DataFrame:
    """
    Build a subgraph of coauthor edges restricted to the ego-networks of the Top10 authors
    (as defined in Top10_Authors.csv). Uses AllArticles_Edges.csv (Source,Target,SR).
    """
    all_dir = Path(all_dir)
    top_path = all_dir / top10_file
    edges_path = all_dir / edges_file
    if not top_path.exists() or not edges_path.exists():
        raise FileNotFoundError("Top10_Authors.csv or AllArticles_Edges.csv not found in " + str(all_dir))

    top = pd.read_csv(top_path)
    edges = pd.read_csv(edges_path)

    # Set of top author names
    top_authors = set(top["AuthorFullName"].astype(str))

    # Ego-network union: keep edges where at least one endpoint is in top_authors
    sub = edges[(edges["Source"].astype(str).isin(top_authors)) | (edges["Target"].astype(str).isin(top_authors))].copy()

    out_path = all_dir / out_file
    sub.to_csv(out_path, index=False)
    print("Guardado:", out_path)
    return sub


if __name__ == "__main__":
    build_top10_egograph_edges("all_data_wos_scopus")
