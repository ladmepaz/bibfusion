import pandas as pd
import re

def scopus_get_author_country(df, abbreviations_file):
    """
        Adds the 'AU_CO' column to the DataFrame by processing the 'C1' (affiliation) column.

        Parameters:
        ----------
        df : pd.DataFrame
            DataFrame returned by bib_to_df function.
        abbreviations_file : str
            Path to the CSV file containing country abbreviations.

        Returns:
        -------
        pd.DataFrame
            DataFrame with the 'AU_CO' column added.
    """
    # Read the country abbreviations file
    abbreviations_df = pd.read_csv(abbreviations_file, sep='; ', header=None, encoding='utf-8', engine='python')
    abbreviations_dict = dict(zip(abbreviations_df[0].str.upper(), abbreviations_df[1].str.upper()))

    # Process the 'C1' column to extract countries
    def extract_countries(affiliation):
        if isinstance(affiliation, str):  # Check if the value is a string
            # Regular expression to capture affiliation countries
            match = r'\b([A-Z\s]+(?:[ -][A-Z\s]+)*)\b(?=\s*(?:;|$))'
            countries = re.findall(match, affiliation)
            formatted_countries = [abbreviations_dict.get(country.strip(), country.strip()) for country in countries]
            return ';'.join(formatted_countries).upper()
        else:
            return ''  # Return an empty string if the affiliation is not a string (e.g., NaN)

    df['AU_CO'] = df['C1'].apply(extract_countries)

    return df

