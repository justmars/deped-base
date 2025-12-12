from pathlib import Path

import numpy as np
import pandas as pd


def format_id_column(cell_value) -> str:
    """Ensure PSGC ID is a 10-character zero-padded string."""
    return str(cell_value).zfill(10)


def convert_to_int(cell_value):
    """Convert numeric-like cell values to int, returning NaN if invalid."""
    try:
        return int(float(str(cell_value).strip()))
    except (ValueError, TypeError):
        return np.nan


def set_psgc(f: Path) -> pd.DataFrame:
    """Load and clean PSGC Excel data."""
    print(f"Initializing PSGC data from {f=}")
    df = pd.read_excel(
        io=f,
        sheet_name="PSGC",
        usecols="A:I,K",
        converters={
            "10-digit PSGC": format_id_column,
            "Correspondence Code": convert_to_int,
            "2024 Population": convert_to_int,
        },
    )

    df.columns = [
        "id",
        "name",
        "cc",
        "geo",
        "old_names",
        "city_class",
        "income_class",
        "urban_rural",
        "2024_pop",
        "status",
    ]

    # prefer old_names for provinces when present (handles historical name differences)
    df.loc[(df["geo"] == "Prov") & (df["old_names"].notna()), "name"] = df.loc[
        (df["geo"] == "Prov") & (df["old_names"].notna()), "old_names"
    ]

    df.replace({"-": np.nan}, inplace=True)
    df["income_class"] = df["income_class"].str.replace("*", "", regex=False)
    # safe fill for city_class
    df["city_class"] = df["city_class"].fillna("").astype(str)

    return df
