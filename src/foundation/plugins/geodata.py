from pathlib import Path

import polars as pl

from ..common import console
from ..plugin import BaseExtractor, ExtractionContext, ExtractionResult


def set_coordinates(geo_file: Path, meta_df: pl.DataFrame) -> pl.DataFrame:
    """Add longitude and latitude values from `geo_file`."""
    console.log(f"[cyan]Attaching coordinates from {geo_file=}...[/cyan]")
    geo_df = pl.read_csv(geo_file)
    geo_df = geo_df.select(["id", "longitude", "latitude"]).rename({"id": "school_id"})
    school_geo_df_long_lat = meta_df.join(geo_df, on="school_id", how="left")
    return school_geo_df_long_lat


class GeoExtractor(BaseExtractor):
    """Attach longitude/latitude metadata to the canonical address rows."""

    name = "geo"
    depends_on = ["meta_with_hash", "address"]
    outputs = ["geo"]
    schema_name = "geo"

    def extract(
        self,
        context: ExtractionContext,
        dependencies: dict[str, pl.DataFrame],
    ) -> ExtractionResult:
        geo_df = set_coordinates(
            geo_file=context.paths.geo_file,
            meta_df=dependencies["meta_with_hash"],
        )
        geo_df = geo_df.with_columns(pl.col("_addr_hash").cast(pl.Int64))

        address_df = dependencies["address"]
        geo_df = geo_df.join(
            address_df.select(["school_id", "school_year", "_addr_hash", "address_id"]),
            on=["school_id", "school_year", "_addr_hash"],
            how="left",
        )

        return ExtractionResult(tables={"geo": geo_df})
