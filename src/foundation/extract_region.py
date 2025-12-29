import polars as pl
from rich import print as rprint

from .common import PSGC_REGION_MAP, normalize_region_name


def map_psgc_region(region_name: str, psgc_map: dict) -> str | None:
    norm = normalize_region_name(region_name)
    alias = PSGC_REGION_MAP.get(norm, norm)  # use alias if exists
    if alias is None:
        return None  # intentionally ignored (e.g., PSO)
    return psgc_map.get(alias)


def attach_psgc_region_codes(meta: pl.DataFrame, psgc: pl.DataFrame) -> pl.DataFrame:
    """
    Attach PSGC region codes to a school metadata DataFrame.
    Only PSGC entries where geo == 'Reg' are allowed as region matches.
    """

    rprint("[cyan]Attaching PSGC region codes...[/cyan]")

    # ---------------------------------------------------------
    # 1. Filter PSGC to REGION rows only
    # ---------------------------------------------------------
    psgc_regions = psgc.filter(pl.col("geo") == "Reg")

    # ---------------------------------------------------------
    # 2. Normalize PSGC region names
    # ---------------------------------------------------------
    psgc_regions = psgc_regions.with_columns(
        normalized=pl.col("name").map_elements(
            normalize_region_name, return_dtype=pl.Utf8
        )
    )

    # Build lookup: normalized PSGC region name → PSGC ID
    psgc_map = dict(
        zip(psgc_regions["normalized"].to_list(), psgc_regions["id"].to_list())
    )

    # ---------------------------------------------------------
    # 3. Normalize school metadata region names
    # ---------------------------------------------------------
    meta = meta.with_columns(
        normalized_region=pl.col("region").map_elements(
            normalize_region_name, return_dtype=pl.Utf8
        )
    )

    # ---------------------------------------------------------
    # 4. Map school → PSGC region ID (using alias table)
    # ---------------------------------------------------------
    meta = meta.with_columns(
        psgc_region_id=pl.col("normalized_region").map_elements(
            lambda r: map_psgc_region(r, psgc_map), return_dtype=pl.Utf8
        )
    )

    # Remove schools with unmapped region (e.g., PSO or ARMM if intentionally excluded)
    meta = meta.filter(pl.col("psgc_region_id").is_not_null())

    # ---------------------------------------------------------
    # 5. Reorder columns for clarity
    # ---------------------------------------------------------
    priority = ["region", "psgc_region_id", "school_id", "school_name"]
    remaining = [c for c in meta.columns if c not in priority]

    return meta.select(priority + remaining)
