# main.py
from .pipeline import preprocessing_df

def main():
    preprocessing_df(
        path_wos=None,
        path_scopus=None,
        path_scimago=r"data\scimago.csv"
    )

if __name__ == "__main__":
    main()