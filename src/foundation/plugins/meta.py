import re
from pathlib import Path

import polars as pl
from rich import print as rprint

from ..transforms.location import clean_meta_location_names
from ..transforms.school_name import clean_school_name

# -----------------------------------------
# 1. Constants (Centralized)
# -----------------------------------------
ADDRESS_COLS = [
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
]


META_COLS = ["sector", "school_management", "annex_status"] + ADDRESS_COLS


OFFER_COLS = [
    "offers_es",
    "offers_jhs",
    "offers_shs",
]

LONG_COLS = [
    "school_year",
    "school_id",
    "sector",
    "school_management",
    "annex_status",
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


def extract_grade_sex_columns(df: pl.DataFrame, id_vars: list[str]) -> list[str]:
    """Identify all enrollment columns (exclude meta & school year)."""
    return [col for col in df.columns if col not in id_vars]


def split_grade_strand_sex(col_series: pl.Series) -> pl.DataFrame:
    """Parse grade_sex column using Polars with multi-branch logic."""
    CUSTOM = {"sshs_acad", "sshs_techpro"}

    # Count parts per row using map_elements
    def count_parts(x):
        return len(x) if x is not None else 0

    def parse_row(grade_sex_str):
        """Parse a single grade_sex string into (grade, strand, sex)."""
        if not grade_sex_str:
            return (None, None, None)

        parts_list = grade_sex_str.split("_")
        n = len(parts_list)

        # ---- Branch A: custom strands (exactly 3 parts AND middle matches custom) ----
        if n == 3 and parts_list[1] in CUSTOM:
            return (parts_list[0], parts_list[1], parts_list[2])

        # ---- Branch B: 2 part format → grade + sex ----
        if n == 2:
            return (parts_list[0], None, parts_list[1])

        # ---- Branch C: standard old 3-part format ----
        if n == 3:
            return (parts_list[0], parts_list[1], parts_list[2])

        # ---- Branch D: Fallback for >3 parts ----
        if n > 3:
            strand = "_".join(parts_list[1:-1])
            return (parts_list[0], strand, parts_list[-1])

        return (parts_list[0], None, None)

    # Map the parsing function to get tuples
    parsed = col_series.map_elements(parse_row, return_dtype=pl.Object)

    # Extract individual components
    grade = parsed.map_elements(lambda x: x[0] if x else None, return_dtype=pl.Utf8)
    strand = parsed.map_elements(lambda x: x[1] if x else None, return_dtype=pl.Utf8)
    sex = parsed.map_elements(lambda x: x[2] if x else None, return_dtype=pl.Utf8)

    return pl.DataFrame(
        {
            "grade": grade,
            "strand": strand,
            "sex": sex,
        }
    )


def normalize_num_students(expr: pl.Expr) -> pl.Expr:
    """Normalize raw enrollment counts into clean integers.

    The expression removes commas, trims whitespace, and converts digit-only
    strings to `Int64`, returning `null` for invalid values.

    Args:
        expr (pl.Expr): Expression resolving to the raw count values.

    Returns:
        pl.Expr: Sanitized integer expression with `Int64` dtype.
    """

    return (
        expr.cast(pl.Utf8)
        .str.replace_all(",", "")
        .str.strip_chars()
        .map_elements(
            lambda v: int(v) if v and v.isdigit() else None, return_dtype=pl.Int64
        )
    )


def _log_invalid_num_student_rows(df: pl.DataFrame, context: str) -> None:
    """Log rows where the normalized enrollment counts could not be parsed."""

    invalid_rows = df.filter(
        pl.col("__raw_num_students").is_not_null() & pl.col("num_students").is_null()
    )
    count = invalid_rows.height
    if not count:
        return

    samples = (
        invalid_rows.select("__raw_num_students")
        .unique()
        .limit(5)
        .to_series()
        .to_list()
    )

    rprint(
        f"[yellow]Dropped {count} invalid num_students rows during {context}.[/yellow]"
    )
    rprint(f"[yellow]Sample values: {samples}[/yellow]")


# -----------------------------------------
# 3. Transform single file
# -----------------------------------------
def load_and_melt_file(path: Path) -> pl.DataFrame:
    """Load a CSV and return enrollment counts in long form.

    Args:
        path (Path): Path to the enrollment CSV file.

    Returns:
        pl.DataFrame: Melted data with parsed grade, strand, and sex columns.
    """
    school_year = extract_school_year(path.name)
    rprint(f"[green]Processing file:[/green] {path.name}")

    df = pl.read_csv(path)

    # Add school_year column
    df = df.with_columns(pl.lit(school_year).alias("school_year"))

    # Id vars = school year + metadata
    id_vars = ["school_year"] + META_COLS + OFFER_COLS

    # Enrollment columns
    value_cols = extract_grade_sex_columns(df, id_vars=id_vars)

    rprint("[cyan]Melting wide enrollment columns → long...[/cyan]")
    melted = df.unpivot(
        index=id_vars,
        on=value_cols,
        variable_name="grade_sex",
        value_name="num_students",
    )

    melted = melted.with_columns(pl.col("num_students").alias("__raw_num_students"))

    # Normalize the number of students values before filtering
    melted = melted.with_columns(
        normalize_num_students(pl.col("__raw_num_students")).alias("num_students")
    )

    _log_invalid_num_student_rows(melted, school_year)

    # Drop empty / zero entries early
    melted = melted.filter(
        (pl.col("num_students").is_not_null()) & (pl.col("num_students") != 0)
    )

    # Split grade/strand/sex
    split_df = split_grade_strand_sex(melted["grade_sex"])
    melted = melted.with_columns(split_df.to_struct().alias("__split")).unnest(
        "__split"
    )

    return melted


# -----------------------------------------
# 4. Process an entire folder
# -----------------------------------------
def process_enrollment_folder(
    folder_path: Path, test_only: bool = False
) -> pl.DataFrame:
    """Process each enrollment CSV in a folder and return combined data.

    Args:
        folder_path (Path): Directory containing the enrollment CSV files.
        test_only (bool): If True, only the most recent file is processed.

    Returns:
        pl.DataFrame: Concatenated, cleaned enrollment data for all years.
    """
    folder = Path(folder_path)
    if not folder.exists():
        raise FileNotFoundError(f"Folder does not exist: {folder_path}")
    if not folder.is_dir():
        raise ValueError(f"Expected a folder, got a file: {folder_path}")

    files = sorted(folder.glob("*.csv"))
    if test_only:
        files = files[-1:]  # only process last file for testing
    if not files:
        raise ValueError("No CSV files found in folder.")

    all_dfs = [load_and_melt_file(path) for path in files]

    rprint("[blue]Combining all dataframes...[/blue]")
    df_long = pl.concat(all_dfs, how="diagonal")

    for col in COLS_TO_CLEAN:
        df_long = df_long.with_columns(
            pl.col(col).str.replace_all(r"\s+", " ").str.strip_chars().alias(col)
        )

    # Clean annex status field
    df_long = df_long.with_columns(
        pl.col("annex_status")
        .str.strip_chars()
        .str.to_lowercase()
        .alias("annex_status")
    )

    # Clean street addresses
    df_long = df_long.with_columns(
        pl.col("street_address")
        .str.to_lowercase()
        .str.strip_chars_start("-")
        .str.strip_chars_end("-")
        .map_elements(
            lambda x: None
            if x and re.match(r"^(not applicable|na|null|none|-|0|n\*/\s*a)$", x)
            else x,
            return_dtype=pl.Utf8,
        )
        .str.to_titlecase()
        .alias("street_address")
    )

    # Ensure numeric
    df_long = df_long.with_columns(
        pl.col("num_students").cast(pl.Float64).cast(pl.Int64).alias("num_students")
    )

    return df_long


def build_school_year_offered_levels(
    cleaned_meta: pl.DataFrame,
    enroll: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Build offer-level dataframes from cleaned metadata and enrollments.

    Args:
        cleaned_meta (pl.DataFrame): Metadata that contains `offers_*` flags.
        enroll (pl.DataFrame): Enrollment facts that drive school-year coverage.

    Returns:
        tuple[pl.DataFrame, pl.DataFrame]:
            clean_meta (pl.DataFrame): Metadata without `offers_*` columns.
            school_year_offered_levels (pl.DataFrame): Long-format level offers.
    """

    OFFER_COLS = ["offers_es", "offers_jhs", "offers_shs"]

    # 1. Extract offer columns
    offerings = cleaned_meta.select(["school_id"] + OFFER_COLS)

    # 2. Get unique school/year pairs seen in enrollment
    school_years = (
        enroll.select(["school_id", "school_year"])
        .unique()
        .join(offerings, on="school_id", how="left")
    )

    # 3. Convert to long format (offers_es → level=ES)
    school_year_offered_levels = school_years.unpivot(
        index=["school_id", "school_year"],
        on=OFFER_COLS,
        variable_name="level",
        value_name="offered",
    )

    # 4. Clean level labels: offers_es → ES
    school_year_offered_levels = school_year_offered_levels.with_columns(
        pl.col("level").str.replace("offers_", "").str.to_uppercase().alias("level")
    )

    # 5. Remove offering columns from cleaned_meta
    cleaned_meta_no_offers = cleaned_meta.drop(OFFER_COLS)

    return cleaned_meta_no_offers, school_year_offered_levels


def make_school_year_offered_levels(df_long: pl.DataFrame) -> pl.DataFrame:
    """Derive an offer matrix per school-year from melted enrollment data.

    Args:
        df_long (pl.DataFrame): Long-form enrollment metadata including `offers_*`.

    Returns:
        pl.DataFrame: Rows with `school_id`, `school_year`, `level`, and `offered`.
    """

    # 1. Sort so we can identify latest metadata per school
    df_sorted = df_long.sort(["school_id", "school_year"], descending=[False, True])

    # 2. Extract latest metadata (contains offer columns)
    meta_latest = df_sorted.unique(subset=["school_id"], keep="first")

    offerings = meta_latest.select(["school_id"] + OFFER_COLS)

    # 3. All school-year pairs present in df_long
    school_years = (
        df_long.select(["school_id", "school_year"])
        .unique()
        .join(offerings, on="school_id", how="left")
    )

    # 4. Long-format melt
    school_year_offered_levels = school_years.unpivot(
        index=["school_id", "school_year"],
        on=OFFER_COLS,
        variable_name="level",
        value_name="offered",
    )

    # 5. Clean level names
    school_year_offered_levels = school_year_offered_levels.with_columns(
        pl.col("level").str.replace("offers_", "").str.to_lowercase().alias("level")
    )

    return school_year_offered_levels


# -----------------------------------------
# 5. Extract metadata (latest year per school) and full enrollment
# -----------------------------------------
def unpack_enroll_data(
    enrolment_folder: Path, test_only: bool = False
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Assemble metadata, enroll facts, and offer levels from source files.

    Args:
        enrolment_folder (Path): Directory storing each yearly enrollment CSV.
        test_only (bool): If True, only the most recent CSV is processed.

    Returns:
        tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]: School-year metadata,
            enrollment facts, and school-year offered levels.
    """
    df_long = process_enrollment_folder(
        folder_path=enrolment_folder, test_only=test_only
    )

    rprint("[blue]Extracting school-year offered levels...[/blue]")
    school_year_offered_levels = make_school_year_offered_levels(df_long)

    # -----------------------------
    # School-year metadata (NO collapsing)
    # -----------------------------
    REVISED_COLS = [
        "school_year",
        "school_id",
        "school_name",
        "sector",
        "school_management",
        "annex_status",
        "region",
        "province",
        "municipality",
        "barangay",
        "street_address",
        "legislative_district",
        "division",
        "school_district",
    ]

    rprint("[blue]Extracting school-year metadata...[/blue]")
    school_year_meta = df_long.select(REVISED_COLS).unique(
        subset=["school_id", "school_year"]
    )

    # Clean location + school names *once*
    rprint("[blue]Cleaning school-year metadata...[/blue]")
    school_year_meta = clean_meta_location_names(school_year_meta)
    school_year_meta = school_year_meta.with_columns(
        pl.col("school_name").map_elements(clean_school_name, return_dtype=pl.Utf8)
    )

    # -----------------------------
    # Enrollment facts (thin)
    # -----------------------------
    rprint("[blue]Extracting enrollment facts...[/blue]")
    enroll = df_long.select(
        [
            "school_year",
            "school_id",
            "grade",
            "sex",
            "strand",
            "num_students",
        ]
    )

    return school_year_meta, enroll, school_year_offered_levels
