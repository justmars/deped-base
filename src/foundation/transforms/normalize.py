"""Normalization helpers shared across PSGC/matching extractors."""

from __future__ import annotations

import re

import polars as pl


def _digits_only(s: str) -> str:
    """Return only the digits contained in a string or empty when missing.

    Args:
        s: Input string to sanitize.

    Returns:
        Digits extracted from ``s`` or an empty string if ``s`` is None or blank.
    """
    if s is None:
        return ""
    return re.sub(r"\D", "", str(s))


def get_unique_regions(df: pl.DataFrame) -> pl.DataFrame:
    """Return distinct PSGC region IDs and their normalized names.

    Args:
        df: DataFrame containing ``psgc_region_id`` and ``region`` columns.

    Returns:
        DataFrame with two columns: ``id`` (psgc region) and ``name`` (region name),
        sorted by ``id``.
    """
    _df = df.select(["psgc_region_id", "region"]).unique()
    _df = _df.rename({"psgc_region_id": "id", "region": "name"})
    return _df.sort("id")


def get_unique_provinces(df: pl.DataFrame, psgc_df: pl.DataFrame) -> pl.DataFrame:
    """Canonicalize province PSGC IDs and attach their region references.

    This helper trims incoming ``psgc_provhuc_id`` values to the first five
    digits, matches them to the authoritative PSGC province list, and then
    tags each province with the region ``psgc`` identifier derived from the
    province code prefix.

    Args:
        df: Source schools DataFrame that contains ``psgc_provhuc_id``.
        psgc_df: Master PSGC table with ``id``, ``geo``, and ``name``.

    Returns:
        DataFrame with columns ``id`` (authoritative province id), ``name``,
        and ``region_id`` (linked PSGC region id).
    """

    # --- 1) Clean school dataframe province IDs ---
    prov_series = (
        df.select("psgc_provhuc_id")
        .to_series()
        .cast(pl.Utf8)
        .map_elements(_digits_only, return_dtype=pl.Utf8)
    )
    prov_series = prov_series.filter(prov_series.str.len_chars() > 0).unique()
    prov_df = pl.DataFrame({"raw_provhuc_id": prov_series})

    # Canonical 5-digit province code
    prov_df = prov_df.with_columns(prov_key=pl.col("raw_provhuc_id").str.slice(0, 5))

    # --- 2) Prepare PSGC province layer ---
    psgc_prov = psgc_df.filter(pl.col("geo") == "Prov").with_columns(
        id=pl.col("id").cast(pl.Utf8).map_elements(_digits_only, return_dtype=pl.Utf8)
    )
    psgc_prov = psgc_prov.with_columns(prov_key=pl.col("id").str.slice(0, 5))
    psgc_prov = psgc_prov.select(["prov_key", "id", "name"]).unique(subset=["prov_key"])

    # --- 3) Prepare PSGC region layer (for region mapping) ---
    psgc_reg = psgc_df.filter(pl.col("geo") == "Reg").with_columns(
        id=pl.col("id").cast(pl.Utf8).map_elements(_digits_only, return_dtype=pl.Utf8)
    )
    psgc_reg = psgc_reg.with_columns(
        reg_key=pl.col("id").str.slice(0, 2)  # Regions map via 2-digit code
    )
    psgc_reg = psgc_reg.select(["reg_key", "id"]).rename({"id": "region_id"})

    # --- 4) Merge school province list → PSGC provinces ---
    merged = prov_df.join(psgc_prov, on="prov_key", how="left")

    # Remove unmatched provinces
    merged = merged.filter(pl.col("id").is_not_null())

    # --- 5) Add region_code based on province PSGC code prefix ---
    merged = merged.with_columns(reg_key=pl.col("id").str.slice(0, 2))
    merged = merged.join(psgc_reg, on="reg_key", how="left")

    # --- 6) Final cleanup ---
    final = merged.select(["id", "name", "region_id"]).unique(subset=["id"])

    return final


def get_divisions(df: pl.DataFrame) -> pl.DataFrame:
    """Generate deterministic division identifiers grouped by region.

    Sequentially numbers each unique division within a region and composes a
    ``division_id`` of the form ``<psgc_region_id>-<division_seq>`` which can
    be used as a stable key.

    Args:
        df: Input frame containing ``psgc_region_id`` and ``division`` columns.

    Returns:
        DataFrame with unique combinations of regions and divisions, along with
        ``division_seq`` and ``division_id``.
    """
    div = (
        df.select(["psgc_region_id", "division"])
        .unique()
        .sort(["psgc_region_id", "division"])
    )
    div = div.with_columns(
        division_seq=pl.col("psgc_region_id").cum_count().over("psgc_region_id") + 1
    )

    div = div.with_columns(
        division_id=pl.concat_str(
            [
                pl.col("psgc_region_id").cast(pl.Utf8),
                pl.col("division_seq").cast(pl.Utf8),
            ],
            separator="-",
        )
    )
    return div
