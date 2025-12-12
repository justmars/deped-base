from pathlib import Path

import pandas as pd
from rich import print as rprint


def set_coordinates(geo_file: Path, meta_df: pd.DataFrame) -> pd.DataFrame:
    """Add longitude and latitude values from `geo_file`."""
    rprint(f"[cyan]Attaching coordinates from {geo_file=}...[/cyan]")
    _df = pd.read_csv(geo_file)
    _df = _df[["id", "longitude", "latitude"]]
    _df.rename(columns={"id": "school_id"}, inplace=True)
    school_geo_df_long_lat = meta_df.merge(_df, on="school_id", how="left")
    return school_geo_df_long_lat
