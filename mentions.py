import os
import re
import csv
import json
from collections import defaultdict
from datetime import datetime, timezone

import pandas as pd


# =========================
# CONFIG
# =========================

INPUT_FILE = r"C:\Users\pcn801\OneDrive - Sky\Media Strategy - Documents\Measurement Projects\2026\AI-ChatGPT\02. Excel Outputs\01.Updated_Analysis\5.3_OUPUTS\5.3_outputs.csv"

OUTPUT_JSON = r"C:\Users\pcn801\OneDrive - Sky\Media Strategy - Documents\Measurement Projects\2026\AI-ChatGPT\02. Excel Outputs\01.Updated_Analysis\5.3_OUPUTS\brand_counts_per_row.json"

OUTPUT_AUDIT_CSV = r"C:\Users\pcn801\OneDrive - Sky\Media Strategy - Documents\Measurement Projects\2026\AI-ChatGPT\02. Excel Outputs\01.Updated_Analysis\5.3_OUPUTS\brand_match_audit.csv"

OUTPUT_ENRICHED_CSV = r"C:\Users\pcn801\OneDrive - Sky\Media Strategy - Documents\Measurement Projects\2026\AI-ChatGPT\02. Excel Outputs\01.Updated_Analysis\5.3_OUPUTS\raw_responses_with_mentions.csv"

# Column D = zero-based index 3
TARGET_COL_INDEX = 3


# =========================
# BRAND DICTIONARY
# =========================

BRANDS = {
    "Sky": [
        "Sky",
        "Sky Broadband",
    ],
    "Virgin Media": [
        "Virgin Media",
        "Virgin",
    ],
    "BT": [
        "BT",
        "BT Broadband",
        "BT Full Fibre",
    ],
    "TalkTalk": [
        "TalkTalk",
    ],
    "Plusnet": [
        "Plusnet",
    ],
    "Vodafone": [
        "Vodafone",
    ],
    "EE": [
        "EE",
        "EE Broadband",
    ],
    "Hyperoptic": [
        "Hyperoptic",
    ],
    "Zen Internet": [
        "Zen Internet",
        "Zen",
    ],
    "Community Fibre": [
        "Community Fibre",
    ],
    "NOW Broadband": [
        "NOW Broadband",
        "NOW",
    ],
    "YouFibre": [
        "YouFibre",
    ],
}

# Keep these case-sensitive to reduce false matches
CASE_SENSITIVE_BRANDS = {"EE", "BT", "NOW Broadband"}


# =========================
# HELPERS
# =========================

def index_to_excel_col(idx: int) -> str:
    """
    Convert zero-based index to Excel-style column label.
    0 -> A
    1 -> B
    25 -> Z
    26 -> AA
    """
    idx += 1
    letters = ""
    while idx > 0:
        idx, remainder = divmod(idx - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters


def generate_column_names(ncols: int) -> list:
    """
    Create generic header names for a no-header file.
    Example:
    col_A, col_B, col_C, ...
    """
    return [f"col_{index_to_excel_col(i)}" for i in range(ncols)]


# =========================
# FILE LOADING
# =========================

def load_file(file_path: str) -> pd.DataFrame:
    """
    Load CSV or Excel into a DataFrame.
    Assumes the file has NO header row.
    Then assigns generated column names.
    """
    ext = os.path.splitext(file_path)[1].lower()

    if ext == ".csv":
        try:
            df = pd.read_csv(
                file_path,
                header=None,
                dtype=str,
                keep_default_na=False
            )
        except pd.errors.ParserError:
            df = pd.read_csv(
                file_path,
                header=None,
                dtype=str,
                keep_default_na=False,
                engine="python",
                quoting=csv.QUOTE_MINIMAL
            )

    elif ext == ".xlsx":
        df = pd.read_excel(
            file_path,
            header=None,
            dtype=str,
            engine="openpyxl"
        )

    elif ext == ".xls":
        df = pd.read_excel(
            file_path,
            header=None,
            dtype=str,
            engine="xlrd"
        )

    else:
        raise ValueError("Unsupported file type. Use .csv, .xlsx, or .xls")

    df.columns = generate_column_names(df.shape[1])
    return df


# =========================
# TEXT CLEANING
# =========================

def clean_text(text: str) -> str:
    """
    Clean the text before matching brands.
    - keep visible text from markdown links
    - remove bare URLs
    - remove simple markdown formatting
    - normalise spaces
    """
    if not isinstance(text, str):
        return ""

    # Convert markdown links [text](url) -> text
    text = re.sub(r"\[([^\]]+)\]\((https?://[^)]+)\)", r"\1", text)

    # Remove bare URLs
    text = re.sub(r"https?://\S+", " ", text)

    # Remove simple markdown markers
    text = text.replace("**", " ")
    text = text.replace("__", " ")
    text = text.replace("`", " ")

    # Normalise whitespace
    text = re.sub(r"\s+", " ", text).strip()

    return text


# =========================
# PATTERN BUILDING
# =========================

def build_patterns(brands: dict) -> dict:
    """
    Build regex patterns for each brand.
    Whole-word matching only.
    Longer aliases are checked first.
    """
    patterns = {}

    for brand, aliases in brands.items():
        aliases_sorted = sorted(aliases, key=len, reverse=True)
        compiled = []

        for alias in aliases_sorted:
            pattern = r"(?<!\w)" + re.escape(alias) + r"(?!\w)"
            flags = 0 if brand in CASE_SENSITIVE_BRANDS else re.IGNORECASE
            compiled.append((alias, re.compile(pattern, flags)))

        patterns[brand] = compiled

    return patterns


# =========================
# MATCHING
# =========================

def find_unique_brands_in_text(text: str, patterns: dict) -> tuple:
    """
    Find unique brands in one row of text.
    Each brand can appear at most once per row.
    Returns:
    - list of unique brands
    - list of audit rows
    """
    unique_brands = []
    audit_rows = []

    for brand, compiled_patterns in patterns.items():
        matched_alias = None
        matched_text = None

        for alias, pattern in compiled_patterns:
            match = pattern.search(text)
            if match:
                matched_alias = alias
                matched_text = match.group(0)
                break

        if matched_alias is not None:
            unique_brands.append(brand)
            audit_rows.append(
                {
                    "brand": brand,
                    "matched_alias": matched_alias,
                    "matched_text": matched_text,
                }
            )

    return unique_brands, audit_rows


def analyse_rows(texts, patterns):
    """
    Analyse cleaned column D.
    Returns:
    - brand row counts
    - audit rows
    - mentions list for enriched CSV
    """
    brand_row_counts = defaultdict(int)
    audit_rows = []
    mentions_output = []

    for row_num, text in enumerate(texts, start=1):
        text = "" if pd.isna(text) else str(text)

        unique_brands, row_audit = find_unique_brands_in_text(text, patterns)

        for brand in unique_brands:
            brand_row_counts[brand] += 1

        mentions_output.append(", ".join(unique_brands))

        for item in row_audit:
            audit_rows.append(
                {
                    "row_number": row_num,
                    "brand": item["brand"],
                    "matched_alias": item["matched_alias"],
                    "matched_text": item["matched_text"],
                }
            )

    return brand_row_counts, audit_rows, mentions_output


# =========================
# OUTPUT
# =========================

def ensure_output_dir(file_path: str):
    output_dir = os.path.dirname(file_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)


def save_json_output(result_df: pd.DataFrame, output_file: str, input_file: str, rows_analysed: int):
    """
    Save the summary result as JSON.
    """
    ensure_output_dir(output_file)

    payload = {
        "run_metadata": {
            "input_file": input_file,
            "output_created_at": datetime.now(timezone.utc).isoformat(),
            "column_analysed": "D",
            "column_index": TARGET_COL_INDEX,
            "rows_analysed": rows_analysed,
        },
        "matching_rules": {
            "counting_mode": "once_per_brand_per_row",
            "whole_word_matching_only": True,
            "case_sensitive_brands": sorted(list(CASE_SENSITIVE_BRANDS)),
            "urls_stripped_before_counting": True,
            "markdown_cleaned_before_counting": True,
            "logic": "Each brand is counted at most once per row in column D",
        },
        "brand_dictionary": BRANDS,
        "results": result_df.to_dict(orient="records"),
    }

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def save_audit_csv(audit_rows: list, output_file: str):
    """
    Save audit CSV to help QA the matches.
    """
    ensure_output_dir(output_file)
    audit_df = pd.DataFrame(audit_rows)
    audit_df.to_csv(output_file, index=False)


def save_enriched_csv(df: pd.DataFrame, mentions_output: list, output_file: str):
    """
    Save the raw file with an extra column called mentions.
    The output file will HAVE headers.
    """
    ensure_output_dir(output_file)

    enriched_df = df.copy()
    enriched_df["mentions"] = mentions_output
    enriched_df.to_csv(output_file, index=False, encoding="utf-8-sig")


# =========================
# MAIN
# =========================

def main():
    print("Loading file...")
    df = load_file(INPUT_FILE)

    if df.shape[1] <= TARGET_COL_INDEX:
        raise ValueError("The file does not have a column D")

    target_col_name = df.columns[TARGET_COL_INDEX]

    print("Cleaning column D only...")
    texts = (
        df[target_col_name]
        .fillna("")
        .astype(str)
        .apply(clean_text)
        .tolist()
    )

    print("Building patterns...")
    patterns = build_patterns(BRANDS)

    print("Analysing rows...")
    brand_row_counts, audit_rows, mentions_output = analyse_rows(texts, patterns)

    result = pd.DataFrame(
        {
            "brand": list(BRANDS.keys()),
            "rows_mentioned": [brand_row_counts.get(brand, 0) for brand in BRANDS.keys()],
        }
    )

    total_rows = len(texts)

    if total_rows > 0:
        result["pct_of_rows"] = (result["rows_mentioned"] / total_rows * 100).round(1)
    else:
        result["pct_of_rows"] = 0.0

    result = result.sort_values(
        ["rows_mentioned", "brand"],
        ascending=[False, True]
    ).reset_index(drop=True)

    print("Saving summary JSON...")
    save_json_output(
        result_df=result,
        output_file=OUTPUT_JSON,
        input_file=INPUT_FILE,
        rows_analysed=total_rows,
    )

    print("Saving audit CSV...")
    save_audit_csv(
        audit_rows=audit_rows,
        output_file=OUTPUT_AUDIT_CSV,
    )

    print("Saving enriched raw responses CSV...")
    save_enriched_csv(
        df=df,
        mentions_output=mentions_output,
        output_file=OUTPUT_ENRICHED_CSV,
    )

    print()
    print("Done")
    print(f"Input file: {INPUT_FILE}")
    print(f"Output JSON: {OUTPUT_JSON}")
    print(f"Output audit CSV: {OUTPUT_AUDIT_CSV}")
    print(f"Output enriched CSV: {OUTPUT_ENRICHED_CSV}")
    print("Column analysed: D only")
    print(f"Resolved column name: {target_col_name}")
    print(f"Resolved column index: {TARGET_COL_INDEX}")
    print(f"Rows analysed: {total_rows}")
    print()
    print(result.to_string(index=False))


if __name__ == "__main__":
    main()