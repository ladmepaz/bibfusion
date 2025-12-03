import os
import re
import unicodedata
import pandas as pd

# Manual map for characters that do not decompose cleanly (e.g., Ø -> O)
MAPEO_ESPECIAL = str.maketrans({
    "Ø": "O", "ø": "o",
    "Æ": "AE", "æ": "ae",
    "Å": "A", "å": "a",
    "Ð": "D", "ð": "d",
    "Þ": "TH", "þ": "th",
    "ß": "SS",
})


def ascii_upper(val: str) -> str:
    """
    Normalize to ASCII-only uppercase:
    - NFD to split base/diacritics
    - drop combining marks
    - apply manual mapping for symbols that do not decompose (e.g., Ø)
    - encode/decode ASCII ignoring leftovers
    """
    if val is None:
        return ""
    s = str(val)
    norm = unicodedata.normalize("NFD", s)
    no_marks = "".join(ch for ch in norm if not unicodedata.combining(ch))
    mapped = no_marks.translate(MAPEO_ESPECIAL)
    return mapped.encode("ascii", "ignore").decode("ascii").upper().strip()


def wos_txt_to_df(file_paths):
    """
    Converts Web of Science .txt exports into a DataFrame.
    """
    try:
        if isinstance(file_paths, str):
            file_paths = [file_paths]

        all_records = []

        for file_path in file_paths:
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"The file '{file_path}' does not exist.")

            _, ext = os.path.splitext(file_path)
            if ext.lower() != ".txt":
                raise ValueError(f"The file '{file_path}' is not a .txt file.")

            # Quick validation of WoS markers
            with open(file_path, "r", encoding="utf-8-sig") as f:
                first_lines = [next(f).strip() for _ in range(5)]
                if not any(line.startswith("FN ") for line in first_lines) or not any(
                    line.startswith("VR ") for line in first_lines
                ):
                    raise ValueError(
                        f"The file '{file_path}' does not appear to be a valid Web of Science text file."
                    )

            with open(file_path, "r", encoding="utf-8-sig") as f:
                records = []
                current_record = {}
                current_field = None

                multiple_fields = {"AU", "AF", "CR", "ID", "DE", "C1", "C3", "EM", "RI", "OI", "FX", "RP"}
                ignore_fields = {"FN", "VR"}
                uppercase_fields = {
                    "AB",
                    "AF",
                    "AU",
                    "C1",
                    "CR",
                    "DE",
                    "DT",
                    "EM",
                    "FU",
                    "FX",
                    "JI",
                    "J9",
                    "LA",
                    "OA",
                    "OI",
                    "PT",
                    "RI",
                    "RP",
                    "SC",
                    "SP",
                    "TI",
                    "WC",
                    "WE",
                }

                for line in f:
                    line = line.rstrip("\n")
                    if not line.strip():
                        continue

                    if line == "ER":
                        if current_record:
                            records.append(current_record)
                        current_record = {}
                        current_field = None
                        continue

                    if line[:2] in ignore_fields and line[2] == " ":
                        current_field = None
                        continue

                    match = re.match(r"^([A-Z0-9]{2}) (.*)", line)
                    if match:
                        current_field, value = match.groups()
                        value = value.strip()

                        if current_field in ignore_fields:
                            current_field = None
                            continue

                        if current_field in multiple_fields:
                            current_record.setdefault(current_field, []).append(value)
                        else:
                            current_record[current_field] = value
                    else:
                        # Continuation of previous field
                        value = line.strip()
                        if current_field:
                            if current_field in multiple_fields:
                                current_record.setdefault(current_field, []).append(value)
                            else:
                                current_record[current_field] = current_record[current_field] + " " + value

                if current_record:
                    records.append(current_record)

                # Process records
                for record in records:
                    # AU: "Last, F" -> "LAST F"
                    if "AU" in record:
                        processed = []
                        for name in record["AU"]:
                            parts = name.split(", ")
                            if len(parts) == 2:
                                lastname, firstname = parts
                                processed_name = f"{ascii_upper(lastname)} {ascii_upper(firstname)}"
                            else:
                                processed_name = ascii_upper(name)
                            processed.append(processed_name)
                        record["AU"] = ";".join(processed)

                    # AF full names
                    if "AF" in record:
                        record["AF"] = ";".join(ascii_upper(n) for n in record["AF"])

                    # CR references
                    if "CR" in record:
                        record["CR"] = "; ".join(ascii_upper(r) for r in record["CR"])

                    # Addresses
                    if "C1" in record:
                        record["C1"] = "; ".join(ascii_upper(addr) for addr in record["C1"])
                    if "C3" in record:
                        record["C3"] = "; ".join(ascii_upper(addr) for addr in record["C3"])

                    # Descriptors
                    if "DE" in record:
                        record["DE"] = "; ".join(ascii_upper(d) for d in record["DE"])

                    # Other uppercase fields
                    for field in uppercase_fields:
                        if field in record and field not in {"AU", "AF", "CR", "C1", "C3", "DE"}:
                            if field in multiple_fields and isinstance(record[field], list):
                                record[field] = "; ".join(ascii_upper(v) for v in record[field])
                            else:
                                record[field] = ascii_upper(record[field])

                    # Flatten remaining lists
                    for key, value in list(record.items()):
                        if key not in {"AU", "AF", "CR", "C1", "C3", "DE"} | uppercase_fields and isinstance(value, list):
                            record[key] = "; ".join(value)

                    record["DB"] = "WOS"

                    # SR key
                    if "AU" in record and "PY" in record and "J9" in record:
                        first_author = record["AU"].split(";")[0]
                        record["SR"] = f"{first_author}, {record['PY']}, {record['J9']}"
                    else:
                        record["SR"] = ""

                all_records.extend(records)

        df = pd.DataFrame(all_records)

        desired_columns = [
            "AU",
            "AF",
            "CR",
            "AB",
            "AR",
            "BP",
            "C1",
            "C3",
            "CL",
            "CT",
            "CY",
            "DA",
            "DE",
            "DI",
            "DT",
            "EA",
            "EF",
            "EI",
            "EM",
            "EP",
            "ER",
            "FU",
            "FX",
            "GA",
            "HC",
            "HO",
            "HP",
            "ID",
            "IS",
            "J9",
            "JI",
            "LA",
            "MA",
            "NR",
            "OA",
            "OI",
            "PA",
            "PD",
            "PG",
            "PI",
            "PM",
            "PN",
            "PT",
            "PU",
            "PY",
            "RI",
            "RP",
            "SC",
            "SI",
            "SN",
            "SO",
            "SP",
            "SU",
            "TC",
            "TI",
            "U1",
            "U2",
            "UT",
            "VL",
            "WC",
            "WE",
            "Z9",
            "DB",
            "SR",
        ]

        for col in desired_columns:
            if col not in df.columns:
                df[col] = ""

        rename_columns = {
            "AU": "author",
            "AF": "author_full_names",
            "CR": "references",
            "AB": "abstract",
            "AR": "article_number",
            "BP": "page_start",
            "C1": "affiliations",
            "C3": "affiliation_2",
            "CL": "conference_location",
            "CT": "conference_title",
            "CY": "conference_year",
            "DA": "date",
            "DE": "author_keywords",
            "DI": "doi",
            "DT": "document_type",
            "EA": "early_access_date",
            "EI": "eissn",
            "EM": "email_address",
            "EP": "page_end",
            "FU": "funding_agency",
            "FX": "funding_details",
            "GA": "document_delivery_number",
            "HC": "highly_cited",
            "HO": "conference_host",
            "HP": "epub_date",
            "ID": "index_keywords",
            "IS": "issue",
            "J9": "source_title",
            "JI": "journal_abbreviation",
            "LA": "language",
            "MA": "meeting_abstract",
            "NR": "cited_reference_count",
            "OA": "open_access_indicator",
            "OI": "orcid",
            "PA": "publisher_address",
            "PD": "publication_date",
            "PG": "page_count",
            "PI": "city_publisher",
            "PM": "pubmed_id",
            "PN": "part_number",
            "PT": "publication_type",
            "PU": "publisher",
            "PY": "year",
            "RI": "researcher_id_number",
            "RP": "reprint_address",
            "SC": "subject_category",
            "SI": "special_issue",
            "SN": "issn",
            "SO": "journal",
            "SP": "conference_sponsor",
            "SU": "supplement",
            "TC": "TC",
            "TI": "title",
            "U1": "usage_count_last_180_days",
            "U2": "usage_count_since_2013",
            "UT": "accession_number",
            "VL": "volume",
            "WC": "web_of_science_categories",
            "WE": "web_of_science_entry",
            "Z9": "cited_by",
            "DB": "source",
        }

        df = df[desired_columns]
        df.rename(columns=rename_columns, inplace=True)

        # Country extraction based on affiliations
        country_map = {}
        try:
            repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
            country_csv = os.path.join(repo_root, "tests", "files", "country.csv")
            if os.path.exists(country_csv):
                cdf = pd.read_csv(country_csv, sep=";")
                for name in cdf.get("Name", pd.Series(dtype=str)).astype(str):
                    up = ascii_upper(name)
                    if up:
                        country_map[up] = up
        except Exception:
            country_map = {}

        synonyms = {
            "USA": "UNITED STATES",
            "U S A": "UNITED STATES",
            "UNITED STATES OF AMERICA": "UNITED STATES",
            "U ARAB EMIR": "UNITED ARAB EMIRATES",
            "UNITED ARAB EMIR": "UNITED ARAB EMIRATES",
            "PEOPLES R CHINA": "CHINA",
            "PEOPLES REPUBLIC OF CHINA": "CHINA",
            "P R CHINA": "CHINA",
            "ENGLAND": "UNITED KINGDOM",
            "SCOTLAND": "UNITED KINGDOM",
            "WALES": "UNITED KINGDOM",
            "NORTHERN IRELAND": "UNITED KINGDOM",
        }

        def extract_countries(aff_str: str) -> str:
            if not isinstance(aff_str, str) or not aff_str.strip():
                return ""
            s = aff_str.strip()
            segs = re.findall(r"\[[^\]]+\][^;]*", s)
            if not segs:
                segs = [p for p in s.split(";") if p.strip()]
            countries = []
            for seg in segs:
                parts = seg.rsplit(",", 1)
                cand = parts[-1] if parts else seg
                cand = ascii_upper(cand).rstrip(".")
                cand = re.sub(r"\s+", " ", cand)

                if re.search(r"\bUSA\b$", cand):
                    norm = "UNITED STATES"
                else:
                    m = re.search(r"([A-Z][A-Z ]+)$", cand)
                    tail = m.group(1).strip() if m else cand
                    norm = synonyms.get(tail) or country_map.get(tail)
                    if not norm:
                        last_tok = tail.split()[-1]
                        norm = synonyms.get(last_tok) or country_map.get(last_tok)
                    if not norm:
                        norm = tail
                countries.append(norm)
            return "; ".join(countries) if countries else ""

        try:
            if "affiliations" in df.columns:
                df["country"] = df["affiliations"].apply(extract_countries)
        except Exception:
            df["country"] = ""

        for col in ["ER", "EF", "TC"]:
            if col in df.columns:
                df.drop(columns=[col], inplace=True)

        return df

    except FileNotFoundError as e:
        print(f"Error: {e}")
        return None
    except ValueError as e:
        print(f"Error: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None


if __name__ == "__main__":
    df = wos_txt_to_df(["savedrecs.txt", "savedrecs_2.txt"])
    if df is not None:
        print(f"DataFrame created with {len(df)} records")
        print(df.head())
    else:
        print("Failed to create DataFrame from the given file.")
