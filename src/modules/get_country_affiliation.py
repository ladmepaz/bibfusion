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


def extract_countries(df: pd.DataFrame, country_codes_file: str) -> pd.DataFrame:
    """
    Extract country names from the 'Affiliation' column into a new 'Country' column.
    Handles single vs. multiple affiliation parts and avoids false positives.

    Args:
        df: DataFrame with an 'Affiliation' column.
        country_codes_file: Path to a CSV with 'code;name' rows.
    Returns:
        DataFrame with an added 'Country' column.
    """
    if not os.path.exists(country_codes_file):
        logger.error(f"Country codes file not found: {country_codes_file}")
        raise FileNotFoundError(f"Country codes file not found: {country_codes_file}")

    country_df = pd.read_csv(
        country_codes_file,
        sep=';',
        header=None,
        names=['code', 'name'],
        dtype=str
    )
    country_df['code'] = country_df['code'].str.strip().str.upper()
    country_df['name'] = country_df['name'].str.strip().str.upper()

    # Build regex patterns for full names
    name_patterns: List[Tuple[re.Pattern, str]] = []
    for country in sorted(country_df['name'].unique(), key=len, reverse=True):
        name_patterns.append((re.compile(rf"\b{re.escape(country)}\b"), country))

    # Build regex patterns for codes
    code_patterns: List[Tuple[re.Pattern, str]] = []
    for code, name in zip(country_df['code'], country_df['name']):
        code_patterns.append((re.compile(rf"(?:,|\b){re.escape(code)}\b"), name))

    # Set of valid full names and codes for quick lookup
    valid_names = set(country_df['name'])
    valid_codes = set(country_df['code'])

    def extract_from_one(text: str) -> List[str]:
        """Extract country matches -- names first, then codes as fallback."""
        if not text or text.strip().upper() == AFFILIATION_PLACEHOLDER:
            return []
        txt = text.upper()
        found: List[str] = []
        # 1) Full name matches
        for pat, cname in name_patterns:
            if pat.search(txt):
                alias = ALIASES.get(cname, cname)
                if alias not in found:
                    found.append(alias)
        if found:
            return found
        # 2) Code matches only if no full name
        for pat, cname in code_patterns:
            if pat.search(txt):
                alias = ALIASES.get(cname, cname)
                if alias not in found:
                    found.append(alias)
        return found

    def process_affiliation_cell(cell: str) -> str:
        # Split on semicolon, but skip trivial splits
        parts = re.split(r';\s*', cell) if cell and ';' in cell else [cell]
        # If single part, apply single-part logic
        if len(parts) == 1:
            matches = extract_from_one(parts[0])
            if not matches:
                return COUNTRY_PLACEHOLDER
            # If multiple, take the one appearing last in text
            if len(matches) > 1:
                # find last by scanning positions
                occurrences = []
                upper = parts[0].upper()
                for cname in matches:
                    idx = upper.rfind(cname)
                    occurrences.append((idx, cname))
                return max(occurrences)[1]
            return matches[0]

        # Multiple parts: filter out parts that are empty or exact country codes/names
        filtered = []
        for p in parts:
            up = p.strip().upper()
            if not up or up in valid_names or up in valid_codes:
                continue
            filtered.append(p)
        if not filtered:
            return COUNTRY_PLACEHOLDER
        # Extract from each remaining part and aggregate
        agg: List[str] = []
        for p in filtered:
            for cname in extract_from_one(p):
                if cname not in agg:
                    agg.append(cname)
        return '; '.join(agg) if agg else COUNTRY_PLACEHOLDER

    df = df.copy()
    df['Country'] = df['Affiliation'].apply(process_affiliation_cell)
    return df
