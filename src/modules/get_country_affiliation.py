import os
import re
import logging
from typing import List, Tuple

import pandas as pd

# Configure module-level logger
logger = logging.getLogger(__name__)

# Constants
AFFILIATION_PLACEHOLDER = "NO AFFILIATION"
COUNTRY_PLACEHOLDER = "NO COUNTRY"
# Aliases for regions to their parent country
ALIASES = {
    'HONG KONG': 'CHINA',
}


def fill_missing_affiliations(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fill NaN or blank Affiliations with a consistent placeholder.

    Args:
        df: DataFrame containing an 'Affiliation' column.
    Returns:
        DataFrame with missing or blank affiliations replaced.
    """
    df = df.copy()
    df['Affiliation'] = (
        df['Affiliation']
        .fillna(AFFILIATION_PLACEHOLDER)
        .replace(r'^\s*$', AFFILIATION_PLACEHOLDER, regex=True)
    )
    return df

AFFILIATION_PLACEHOLDER = "NO AFFILIATION"
COUNTRY_PLACEHOLDER = "NO COUNTRY"
ALIASES = {}  # Puedes definir alias si deseas mapear nombres alternativos


def extract_countries(df: pd.DataFrame, country_codes_file: str) -> pd.DataFrame:
    if not os.path.exists(country_codes_file):
        raise FileNotFoundError(f"Country codes file not found: {country_codes_file}")

    country_df = pd.read_csv(country_codes_file, sep=';', dtype=str)
    country_df.columns = [col.strip().lower() for col in country_df.columns]


    # Ensure expected column names are present
    country_df.columns = [col.strip().lower() for col in country_df.columns]

    # Verify that valid columns exist
    expected_cols = {'name', 'alpha-2', 'alpha-3'}
    actual_cols = set(country_df.columns)
    if not expected_cols.issubset(actual_cols):
        raise ValueError(f"The file must have the columns: {expected_cols}. Columns found: {actual_cols}")


    # Normalize columns
    country_df = country_df.rename(columns={
        'name': 'name',
        'alpha-2': 'alpha2',
        'alpha-3': 'alpha3'
    })

    # Convert to uppercase and drop nulls
    country_df = country_df.dropna(subset=['name', 'alpha2', 'alpha3'])
    country_df = country_df.astype(str).apply(lambda col: col.str.strip().str.upper())

    # Create long table with all possible variants
    long_df = pd.DataFrame({
        'code': pd.concat([country_df['name'], country_df['alpha2'], country_df['alpha3']]),
        'name': pd.concat([country_df['name']] * 3)
    })

    # Compile regex patterns
    name_patterns: List[Tuple[re.Pattern, str]] = [
        (re.compile(rf"\b{re.escape(n)}\b"), n)
        for n in sorted(long_df['name'].unique(), key=len, reverse=True)
    ]

    code_patterns: List[Tuple[re.Pattern, str]] = [
        (re.compile(rf"(?:,|\b){re.escape(c)}\b"), n)
        for c, n in zip(long_df['code'], long_df['name'])
    ]

    valid_names = set(long_df['name'])
    valid_codes = set(long_df['code'])

    def extract_from_one(text: str) -> List[str]:
        if not text or text.strip().upper() == AFFILIATION_PLACEHOLDER:
            return []
        txt = str(text).upper()
        found: List[str] = []
        for pat, cname in name_patterns:
            if pat.search(txt):
                alias = ALIASES.get(cname, cname)
                if alias not in found:
                    found.append(alias)
        if found:
            return found
        for pat, cname in code_patterns:
            if pat.search(txt):
                alias = ALIASES.get(cname, cname)
                if alias not in found:
                    found.append(alias)
        return found

    def process_affiliation_cell(cell: str) -> str:
        parts = re.split(r';\s*', cell) if cell and ';' in cell else [cell]
        if len(parts) == 1:
            matches = extract_from_one(parts[0])
            if not matches:
                return COUNTRY_PLACEHOLDER
            if len(matches) > 1:
                occurrences = []
                upper = parts[0].upper()
                for cname in matches:
                    idx = upper.rfind(cname)
                    occurrences.append((idx, cname))
                return max(occurrences)[1]
            return matches[0]

        filtered = []
        for p in parts:
            up = p.strip().upper()
            if not up or up in valid_names or up in valid_codes:
                continue
            filtered.append(p)
        if not filtered:
            return COUNTRY_PLACEHOLDER
        agg: List[str] = []
        for p in filtered:
            for cname in extract_from_one(p):
                if cname not in agg:
                    agg.append(cname)
        return '; '.join(agg) if agg else COUNTRY_PLACEHOLDER

    df = df.copy()
    df['Country'] = df['Affiliation'].apply(process_affiliation_cell)
    return df
