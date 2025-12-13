import re
from pathlib import Path

import numpy as np
import pandas as pd
from rich import print as rprint

from .clean_location_names import clean_meta_location_names
from .clean_school_names import clean_school_name

# -----------------------------------------
# 1. Constants (Centralized)
# -----------------------------------------
META_COLS = [
    "sector",
    "school_management",
    "school_id",
    "school_name",
    "region",
    "province",
    "municipality",
    "barangay",
    "street_address",
    "legislative_district",
    "division",
    "school_district",
    "annex_status",
]

OFFER_COLS = ["offers_es", "offers_jhs", "offers_shs"]

LONG_COLS = [
    "school_year",
    "school_id",
    "grade",
    "sex",
    "strand",
    "num_students",
]

COLS_TO_CLEAN = [
    "province",
    "municipality",
    "barangay",
    "division",
    "school_district",
    "legislative_district",
    "street_address",
    "school_name",
    "school_management",
]


# -----------------------------------------
# 2. Utility functions
# -----------------------------------------
def extract_school_year(file_name: str) -> str:
    """Extract school year (yyyy-yyyy) from a filename."""
    match = re.search(r"(\d{4}-\d{4})", file_name)
    if not match:
        raise ValueError(f"Filename '{file_name}' has no school year pattern.")
    return match.group(1)


def extract_grade_sex_columns(df: pd.DataFrame, id_vars: list[str]) -> list[str]:
    """Identify all enrollment columns (exclude meta & school year)."""
    return [col for col in df.columns if col not in id_vars]


def split_grade_strand_sex(col_series: pd.Series) -> pd.DataFrame:
    CUSTOM = {"sshs_acad", "sshs_techpro"}

    # Split into wide columns
    parts = col_series.str.split("_", expand=True)

    # Count number of actual tokens per row
    n = parts.notna().sum(axis=1)

    out = pd.DataFrame(index=col_series.index)
    out["grade"] = parts[0]

    # ---- Branch A: custom strands (exactly 3 parts AND middle matches custom) ----
    mask_custom = (n == 3) & (parts[1].isin(CUSTOM))
    out.loc[mask_custom, "strand"] = parts.loc[mask_custom, 1]
    out.loc[mask_custom, "sex"] = parts.loc[mask_custom, 2]

    # ---- Branch B: 2 part format → grade + sex ----
    mask_two = n == 2
    out.loc[mask_two, "strand"] = pd.NA
    out.loc[mask_two, "sex"] = parts.loc[mask_two, 1]

    # ---- Branch C: standard old 3-part format ----
    mask_standard3 = (n == 3) & ~mask_custom
    out.loc[mask_standard3, "strand"] = parts.loc[mask_standard3, 1]
    out.loc[mask_standard3, "sex"] = parts.loc[mask_standard3, 2]

    # ---- Branch D: Fallback for >3 parts ----
    mask_fallback = n > 3
    if mask_fallback.any():
        middle = (
            parts.loc[mask_fallback]
            .iloc[:, 1:-1]
            .apply(lambda row: "_".join(row.dropna().astype(str)), axis=1)
        )
        out.loc[mask_fallback, "strand"] = middle
        out.loc[mask_fallback, "sex"] = parts.loc[mask_fallback].iloc[:, -1]

    return out


# -----------------------------------------
# 3. Transform single file
# -----------------------------------------
def load_and_melt_file(path: Path) -> pd.DataFrame:
    """Load a single CSV of enrollment counts and melt it to long format."""
    school_year = extract_school_year(path.name)
    rprint(f"[green]Processing file:[/green] {path.name}")

    df = pd.read_csv(path)

    # Add school_year column
    df["school_year"] = school_year

    # Id vars = school year + metadata
    id_vars = ["school_year"] + META_COLS + OFFER_COLS

    # Enrollment columns
    value_cols = extract_grade_sex_columns(df, id_vars=id_vars)

    rprint("[cyan]Melting wide enrollment columns → long...[/cyan]")
    melted = df.melt(
        id_vars=id_vars,
        value_vars=value_cols,
        var_name="grade_sex",
        value_name="num_students",
    )

    # Drop empty / zero entries early
    melted = melted[(melted["num_students"].notna()) & (melted["num_students"] != 0)]

    # Split grade/strand/sex
    split_df = split_grade_strand_sex(melted["grade_sex"])
    melted = pd.concat([melted, split_df], axis=1)

    return melted


# -----------------------------------------
# 4. Process an entire folder
# -----------------------------------------
def process_enrollment_folder(folder_path: Path) -> pd.DataFrame:
    """Load all CSV files under a folder and output one unified long-form dataframe."""
    folder = Path(folder_path)
    if not folder.exists():
        raise FileNotFoundError(f"Folder does not exist: {folder_path}")
    if not folder.is_dir():
        raise ValueError(f"Expected a folder, got a file: {folder_path}")

    files = sorted(folder.glob("*.csv"))
    if not files:
        raise ValueError("No CSV files found in folder.")

    all_dfs = (load_and_melt_file(path) for path in files)

    rprint("[blue]Combining all dataframes...[/blue]")
    df_long = pd.concat(all_dfs, ignore_index=True)

    for col in COLS_TO_CLEAN:
        df_long[col] = df_long[col].str.replace(r"\s+", " ", regex=True).str.strip()

    df_long["street_address"] = (
        df_long["street_address"]
        .str.lower()
        .str.removeprefix("-")
        .str.removesuffix("-")
        .replace(
            to_replace=r"^(not applicable|na|null|none|-|0|n\*/\s*a)$",
            value=np.nan,
            regex=True,
        )
        .str.title()
    )

    # Ensure numeric
    df_long["num_students"] = pd.to_numeric(
        df_long["num_students"], errors="coerce"
    ).astype("Int64")

    return df_long


def build_school_year_offered_levels(
    cleaned_meta: pd.DataFrame,
    enroll: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Creates a long-format dataframe containing offered levels (ES/JHS/SHS)
    per school_id per school_year, based on metadata + enrollment years.

    Returns:
    - cleaned_meta_no_offers: cleaned_meta without offer flags
    - school_year_offered_levels: long format dataframe:
        school_id | school_year | level | offered
    """

    OFFER_COLS = ["offers_es", "offers_jhs", "offers_shs"]

    # 1. Extract offer columns
    offerings = cleaned_meta[["school_id"] + OFFER_COLS].copy()

    # 2. Get unique school/year pairs seen in enrollment
    school_years = (
        enroll[["school_id", "school_year"]]
        .drop_duplicates()
        .merge(offerings, on="school_id", how="left")
    )

    # 3. Convert to long format (offers_es → level=ES)
    school_year_offered_levels = school_years.melt(
        id_vars=["school_id", "school_year"],
        value_vars=OFFER_COLS,
        var_name="level",
        value_name="offered",
    )

    # 4. Clean level labels: offers_es → ES
    school_year_offered_levels["level"] = (
        school_year_offered_levels["level"]
        .str.replace("offers_", "", regex=False)
        .str.upper()
    )

    # 5. Remove offering columns from cleaned_meta
    cleaned_meta_no_offers = cleaned_meta.drop(columns=OFFER_COLS)

    return cleaned_meta_no_offers, school_year_offered_levels


def make_school_year_offered_levels(df_long: pd.DataFrame) -> pd.DataFrame:
    """
    Build a long-format dataframe of offered levels per school_id per school_year.

    Output columns:
    - school_id
    - school_year
    - level (ES/JHS/SHS)
    - offered (0/1)
    """

    # 1. Sort so we can identify latest metadata per school
    df_sorted = df_long.sort_values(
        ["school_id", "school_year"], ascending=[True, False]
    )

    # 2. Extract latest metadata (contains offer columns)
    meta_latest = df_sorted.drop_duplicates(subset=["school_id"], keep="first")

    offerings = meta_latest[["school_id"] + OFFER_COLS].copy()

    # 3. All school-year pairs present in df_long
    school_years = (
        df_long[["school_id", "school_year"]]
        .drop_duplicates()
        .merge(offerings, on="school_id", how="left")
    )

    # 4. Long-format melt
    school_year_offered_levels = school_years.melt(
        id_vars=["school_id", "school_year"],
        value_vars=OFFER_COLS,
        var_name="level",
        value_name="offered",
    )

    # 5. Clean level names
    school_year_offered_levels["level"] = (
        school_year_offered_levels["level"]
        .str.replace("offers_", "", regex=False)
        .str.lower()
    )

    return school_year_offered_levels


# -----------------------------------------
# 5. Extract metadata (latest year per school) and full enrollment
# -----------------------------------------
def unpack_enroll_data(
    enrolment_folder: Path,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Output:
    - meta (latest metadata per school_id)
    - enroll (long enrollment dataset for all years)
    """
    df_long = process_enrollment_folder(folder_path=enrolment_folder)

    rprint("[blue]Extracting school levels into separate df...[/blue]")
    school_year_offered_levels = make_school_year_offered_levels(df_long)

    # Sort by school + school_year descending so first occurrence = newest (prior to dropping non-latest values)
    rprint("[blue]Sorting consolidated enrollment data...[/blue]")
    df_sorted = df_long.sort_values(
        ["school_id", "school_year"], ascending=[True, False]
    )

    # Latest metadata per school_id
    rprint("[blue]Keeping only most recent school metadata...[/blue]")
    meta = df_sorted[META_COLS].drop_duplicates(subset=["school_id"], keep="first")

    # Clean location names
    rprint("[blue]Cleaning location names...[/blue]")
    cleaned_meta = clean_meta_location_names(meta)

    # Normalize school names
    rprint("[blue]Cleaning school names...[/blue]")
    cleaned_meta["school_name"] = cleaned_meta["school_name"].apply(clean_school_name)

    # Enrollment dataset
    rprint("[blue]Extracting enrollment data...[/blue]")
    enroll = df_long[LONG_COLS].copy()

    return cleaned_meta, enroll, school_year_offered_levels
