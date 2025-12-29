from pathlib import Path

import polars as pl
from rich import print as rprint


def set_coordinates(geo_file: Path, meta_df: pl.DataFrame) -> pl.DataFrame:
    """Add longitude and latitude values from `geo_file`."""
    rprint(f"[cyan]Attaching coordinates from {geo_file=}...[/cyan]")
    geo_df = pl.read_csv(geo_file)
    geo_df = geo_df.select(["id", "longitude", "latitude"]).rename({"id": "school_id"})
    school_geo_df_long_lat = meta_df.join(geo_df, on="school_id", how="left")
    return school_geo_df_long_lat
