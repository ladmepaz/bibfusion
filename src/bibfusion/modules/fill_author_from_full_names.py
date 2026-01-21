import pandas as pd


def fill_author_from_full_names(df):
    """
    Fills the 'author' column using 'author_full_names'
    when 'author' is empty or NaN.
    Converts full names to the format: LASTNAME Initials.
    """

    def convert_to_author_format(full_name):
        # Basic validation
        if not isinstance(full_name, str):
            return ''

        full_name = full_name.strip()
        if full_name == '':
            return ''

        parts = full_name.split()

        # Case: single word
        if len(parts) == 1:
            return parts[0].upper()

        # Last name = last word
        last_name = parts[-1].upper()

        # Initials of the names
        initials = ''.join(
            p[0].upper() + '.'
            for p in parts[:-1]
            if p
        )

        return f"{last_name} {initials}".strip()

    def generate_author(row):
        # Only fill if 'author' is empty or NaN
        if pd.isna(row['author']) or str(row['author']).strip() == '':
            if pd.notna(row['author_full_names']):
                # Separate authors and remove empty entries
                name = [
                    n.strip()
                    for n in str(row['author_full_names']).split(';')
                    if n.strip() != ''
                ]

                if not name:
                    return row['author']

                converted_authors = [
                    convert_to_author_format(name)
                    for name in name
                ]

                # Remove possible empty results
                converted_authors = [
                    a for a in converted_authors if a != ''
                ]

                if converted_authors:
                    return '; '.join(converted_authors)

        # If 'author' already exists, keep it
        return row['author']

    df['author'] = df.apply(generate_author, axis=1)
    return df
