import re

import polars as pl


def _digits_only(s: str) -> str:
    """Return only the digits from the string, or '' if NA."""
    if s is None:
        return ""
    return re.sub(r"\D", "", str(s))


def get_unique_regions(df: pl.DataFrame):
    _df = df.select(["psgc_region_id", "region"]).unique()
    _df = _df.rename({"psgc_region_id": "id", "region": "name"})
    _df = _df.sort("id")
    return _df


def get_unique_provinces(df: pl.DataFrame, psgc_df: pl.DataFrame) -> pl.DataFrame:
    """
    Extract unique provinces from df['psgc_provhuc_id'], canonicalize them
    to the first 5 digits, match to PSGC masterlist where geo == 'Prov',
    and return authoritative province PSGC id, province name,
    and the region PSGC code per province.

    Returns columns:
        - id        (from PSGC master, authoritative)
        - name   (canonical PSGC name)
        - region_id    (PSGC region code, 2 digits or full PSGC id depending on PSGC file)
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


def get_divisions(df: pl.DataFrame):
    """Generate a unique division lookup table from a PSGC-aligned dataframe.

    This function extracts unique (region, division) pairs from the input
    dataframe and assigns each division a deterministic ID within its region.
    The ID is created by ordering divisions alphabetically within each region
    and assigning a sequential counter. The resulting identifier takes the form:

        "<psgc_region_id>-<sequence_number>"

    For example, if Region 01 has three divisions sorted alphabetically,
    their division IDs will be: `01-1`, `01-2`, `01-3`.

    Args:
        df (pl.DataFrame): A dataframe containing at least the columns
            `psgc_region_id` (region code) and `division`
            (DepEd division name).

    Returns:
        pl.DataFrame: A dataframe with one row per unique division containing:
            - psgc_region_id (str or int): PSGC region code.
            - division (str): Division name.
            - division_seq (int): Sequential index of the division within
              its region (starting at 1).
            - division_id (str): Deterministic division identifier formed as
              "<psgc_region_id>-<division_seq>".

    Notes:
        - Sorting is alphabetical by division name within each region.
        - Output is stable as long as the region and division values do not change.
        - Additional metadata columns in the input dataframe are ignored.

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
