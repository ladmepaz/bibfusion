import pandas as pd
from pathlib import Path


def build_top_authors(all_dir: str, out_all: str = "MainArticles_Authors.csv", out_top10: str = "Top10_Authors.csv"):
    """
    Build helper tables:
      - MainArticles_Authors.csv: SR and AuthorFullName for ismainarticle == TRUE
      - Top10_Authors.csv: top 10 authors by count of distinct SR (main articles), grouped by AuthorFullName only
    """
    all_dir = Path(all_dir)
    art = pd.read_csv(all_dir / "All_Articles.csv")
    aa = pd.read_csv(all_dir / "All_ArticleAuthor.csv")
    au = pd.read_csv(all_dir / "All_Authors.csv")

    # Filter main articles
    flag = art["ismainarticle"]
    is_main = flag if str(flag.dtype) == "bool" else (flag.astype(str).str.upper().eq("TRUE") | flag.astype(str).eq("1"))
    sr_main = set(art.loc[is_main, "SR"].astype(str))

    # Keep only main SRs
    aa = aa[aa["SR"].astype(str).isin(sr_main)].copy()

    # Join with AuthorFullName via PersonID (fallback AuthorID)
    key = "PersonID" if ("PersonID" in aa.columns and "PersonID" in au.columns) else "AuthorID"
    aa[key] = aa[key].astype(str)
    au[key] = au[key].astype(str)
    aa = aa.merge(au[[key, "AuthorFullName"]], on=key, how="left")

    # Long table SR - AuthorFullName
    out_all_path = all_dir / out_all
    aa[["SR", "AuthorFullName"]].to_csv(out_all_path, index=False)

    # Top 10 by distinct SR, grouping by AuthorFullName (ignore PersonID/AuthorID to avoid duplicate IDs)
    top = (
        aa.groupby("AuthorFullName")["SR"]
        .nunique()
        .reset_index(name="total_articles")
        .sort_values("total_articles", ascending=False)
        .head(10)
    )
    out_top_path = all_dir / out_top10
    top.to_csv(out_top_path, index=False)

    print("Guardado:", out_all_path)
    print("Guardado:", out_top_path)


if __name__ == "__main__":
    # Ajusta la ruta antes de ejecutar directamente este archivo
    build_top_authors("all_data_wos_scopus")
