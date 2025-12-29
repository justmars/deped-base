#!/usr/bin/env python
from pathlib import Path

import polars as pl

from src.foundation.plugins.meta import (
    META_COLS,
    OFFER_COLS,
)

path = Path("data/enroll/enrollment_2017-2018.csv")
school_year = "2017-2018"
df = pl.read_csv(path)
df = df.with_columns(pl.lit(school_year).alias("school_year"))

print("DataFrame shape:", df.shape)
print("\nColumns that will be id_vars:")
id_vars = ["school_year"] + META_COLS + OFFER_COLS
for col in id_vars:
    if col in df.columns:
        print(f"  {col}: {df.schema[col]}")
    else:
        print(f"  {col}: NOT IN DF")

print("\nValue columns to unpivot:")
value_cols = [col for col in df.columns if col not in id_vars]
print(f"  Count: {len(value_cols)}")
print(f"  First few: {value_cols[:5]}")
print(f"  Types: {[str(df.schema[col]) for col in value_cols[:5]]}")

# Try the unpivot
print("\nUnpivoting...")
try:
    melted = df.unpivot(
        index=id_vars,
        on=value_cols,
        variable_name="grade_sex",
        value_name="num_students",
    )
    melted = melted.with_columns(
        pl.col("num_students")
        .cast(pl.Utf8)
        .str.replace_all(",", "")
        .str.strip_chars()
        .map_elements(
            lambda v: int(v) if v and v.isdigit() else None, return_dtype=pl.Int64
        )
        .alias("num_students")
    )
    print("  Success!")
    print(f"  Melted shape: {melted.shape}")
    print(f"  num_students type: {melted.schema['num_students']}")

    # Now try the filter
    print("\nFiltering...")
    filtered = melted.filter(
        (pl.col("num_students").is_not_null()) & (pl.col("num_students") != 0)
    )
    print("  Filter success!")
except Exception as e:
    print(f"  Error: {type(e).__name__}: {e}")
    import traceback

    traceback.print_exc()
