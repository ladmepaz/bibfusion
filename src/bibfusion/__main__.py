# main.py
from .pipeline import preprocessing_df

def main():
    preprocessing_df(
        path_wos=r"C:\Users\User\Documents\Preprocesamiento\Tests\test_3 articles\Wos.txt",
        path_scopus=r"C:\Users\User\Documents\Preprocesamiento\Tests\test_3 articles\LLM.csv",
        path_scimago=r"..\data\scimago.csv",
        path_country=r"..\data\country.csv",
    )

if __name__ == "__main__":
    main()