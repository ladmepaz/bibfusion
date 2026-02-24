# main.py
from .pipeline import preprocessing_df

def main():
    preprocessing_df(
        path_wos=r"PATH/TO/YOUR/FILE/wos_data.txt",
        path_scopus=r"PATH/TO/YOUR/FILE/scopus_data.csv",
        path_scimago=r"data\scimago.csv",
        path_country=r"data\country.csv",
    )

if __name__ == "__main__":
    main()