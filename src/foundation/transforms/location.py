from pathlib import Path

import polars as pl

from foundation.common import FIXES


# ========================================================
# YAML-DRIVEN CLEANER (clean + optimized)
# ========================================================
def clean_meta_location_names(meta: pl.DataFrame) -> pl.DataFrame:
    """
    Clean incorrect municipality, province, and region names
    fully driven by YAML configurations.
    """

    meta = meta.clone()

    # Normalized lowercase helpers using Polars expressions
    prov_series = meta["province"].str.to_lowercase().str.strip_chars()
    muni_series = meta["municipality"].str.to_lowercase().str.strip_chars()
    school_ids = meta["school_id"].cast(pl.Utf8)

    # =====================================================
    # 1. Provincial-level municipality corrections
    # =====================================================
    for rule in FIXES.get("provincial_muni_fixes", []):
        province_raw = rule["province"].strip().lower()
        muni_raw = rule["municipality"].strip().lower()
        corrected = rule["corrected"]

        mask = (prov_series == province_raw) & (muni_series == muni_raw)
        meta = meta.with_columns(
            pl.when(mask)
            .then(pl.lit(corrected))
            .otherwise(pl.col("municipality"))
            .alias("municipality")
        )

    muni_series = meta["municipality"].str.to_lowercase().str.strip_chars()

    # =====================================================
    # 2. Municipality-only corrections
    # =====================================================
    for raw, corrected in FIXES.get("municipality_fixes", {}).items():
        raw_norm = raw.lower()
        mask = muni_series == raw_norm
        meta = meta.with_columns(
            pl.when(mask)
            .then(pl.lit(corrected))
            .otherwise(pl.col("municipality"))
            .alias("municipality")
        )

    muni_series = meta["municipality"].str.to_lowercase().str.strip_chars()

    # =====================================================
    # 3. Province fixes by school ID
    # =====================================================
    for province_name, id_list in FIXES.get("province_fixes_by_school_id", {}).items():
        normalized_ids = [str(x) for x in id_list]
        mask = school_ids.is_in(normalized_ids)
        meta = meta.with_columns(
            pl.when(mask)
            .then(pl.lit(province_name.upper()))
            .otherwise(pl.col("province"))
            .alias("province")
        )

    prov_series = meta["province"].str.to_lowercase().str.strip_chars()

    # =====================================================
    # 4. Region overrides
    # =====================================================
    for rule in FIXES.get("special_fixes", []):
        # Build condition from "when" dict
        mask = pl.lit(True)
        for col, val in rule["when"].items():
            col_series = meta[col].str.to_lowercase().str.strip_chars()
            mask = mask & (col_series == val.lower())

        # Apply updates from "set" dict
        for col, val in rule["set"].items():
            meta = meta.with_columns(
                pl.when(mask).then(pl.lit(val)).otherwise(pl.col(col)).alias(col)
            )

    # =====================================================
    # 5. NIR assignment
    # =====================================================
    nir = FIXES.get("nir_rule", {})
    nir_provinces = {x.lower() for x in nir.get("provinces", [])}
    nir_region = nir.get("set_region", "NIR")

    prov_series = meta["province"].str.to_lowercase().str.strip_chars()
    mask_nir = prov_series.is_in(list(nir_provinces))
    meta = meta.with_columns(
        pl.when(mask_nir)
        .then(pl.lit(nir_region))
        .otherwise(pl.col("region"))
        .alias("region")
    )

    # =====================================================
    # 6. Maguindanao split
    # =====================================================
    split_cfg = FIXES.get("maguindanao_split", {})

    norte_munis = {m.lower() for m in split_cfg.get("norte_municipalities", [])}
    sur_is_default = split_cfg.get("sur_is_default", True)

    prov_series = meta["province"].str.to_lowercase().str.strip_chars()
    muni_series = meta["municipality"].str.to_lowercase().str.strip_chars()

    is_maguindanao = prov_series == "maguindanao"

    # Norte
    mask_norte = is_maguindanao & muni_series.is_in(list(norte_munis))
    meta = meta.with_columns(
        pl.when(mask_norte)
        .then(pl.lit("Maguindanao del Norte"))
        .otherwise(pl.col("province"))
        .alias("province")
    )

    # Sur = remaining
    if sur_is_default:
        prov_series = meta["province"].str.to_lowercase().str.strip_chars()
        muni_series = meta["municipality"].str.to_lowercase().str.strip_chars()
        is_maguindanao = prov_series == "maguindanao"
        mask_sur = is_maguindanao & ~muni_series.is_in(list(norte_munis))
        meta = meta.with_columns(
            pl.when(mask_sur)
            .then(pl.lit("Maguindanao del Sur"))
            .otherwise(pl.col("province"))
            .alias("province")
        )

    return meta
