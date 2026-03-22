# main.py
from .pipeline import preprocessing_df

def main():
    preprocessing_df(
        path_wos=r"data/WoS_303.txt",
        path_scopus=r"data/Scopus_339.csv",
        path_scimago=r"data/scimago.csv",
        path_country=r"data/country.csv",
        API_KEY_OPENALEX="YOUR_OPENALEX_API_KEY",
    )

if __name__ == "__main__":
    main()