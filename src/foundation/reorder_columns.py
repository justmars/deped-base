import pandas as pd
from rich import print as rprint


def reorganize_school_geo_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reorganize columns into a clean hierarchical structure:

    1. School identifiers
    2. Region (with PSGC)
    3. Province (with PSGC)
    4. Municipality (with PSGC)
    5. Barangay (with PSGC)
    6. Other school attributes
    7. Coordinates

    Ensures all PSGC ID fields are strings.
    """
    rprint("[cyan]Reordering dataframe columns...[/cyan]")

    df = df.copy()

    # --- ensure PSGC fields are strings ---
    psgc_cols = ["psgc_region_id", "psgc_provhuc_id", "psgc_muni_id", "psgc_brgy_id"]
    for col in psgc_cols:
        if col in df.columns:
            df[col] = df[col].astype("string")

    # --- define column groups ---
    school_identifiers = ["school_id", "school_name"]

    division_fields = ["division", "division_id"]

    region_fields = ["region", "psgc_region_id"]

    province_fields = ["province", "psgc_provhuc_id"]

    municipality_fields = ["municipality", "psgc_muni_id"]

    barangay_fields = ["barangay", "psgc_brgy_id"]

    attributes = [
        "school_district",
        "legislative_district",
        "street_address",
        "sector",
        "school_management",
        "annex_status",
    ]

    # --- build final ordered column list ---
    ordered_columns = (
        school_identifiers
        + division_fields
        + region_fields
        + province_fields
        + municipality_fields
        + barangay_fields
        + attributes
    )

    # Include any remaining columns automatically (if dataset expands)
    remaining = [c for c in df.columns if c not in ordered_columns]
    ordered_columns = ordered_columns + remaining

    # --- reorder the dataframe ---
    df = df[ordered_columns]

    return df
