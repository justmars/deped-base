import polars as pl


def fill_missing_psgc(meta_df: pl.DataFrame, psgc_df: pl.DataFrame) -> pl.DataFrame:
    """
    For rows in meta_df where psgc_muni_id is NA, match barangay names
    against filtered psgc_df rows and fill PSGC fields accordingly.
    """

    # Filter PSGC subset (your rules)
    psgc_subset = psgc_df.filter(
        (pl.col("cc").fill_null("").cast(pl.Utf8).str.starts_with("1247"))
        & (pl.col("id").cast(pl.Utf8).str.starts_with("19999"))
        & (pl.col("geo") == "Bgy")
    ).with_columns(barangay_norm=pl.col("name").str.strip_chars().str.to_lowercase())

    # Rows in meta_df needing PSGC data
    missing = meta_df.filter(
        (pl.col("psgc_muni_id").is_null()) | (pl.col("psgc_brgy_id").is_null())
    ).with_columns(
        barangay_norm=pl.col("barangay").str.strip_chars().str.to_lowercase()
    )

    # Join by normalized barangay name
    merged = missing.join(
        psgc_subset.select(["id", "barangay_norm"]), on="barangay_norm", how="left"
    )

    # Keep only matched rows
    matched = merged.filter(pl.col("id").is_not_null())

    # Compute PSGC fields
    matched = matched.with_columns(
        psgc_region_id=pl.lit("1900000000"),
        psgc_provhuc_id=pl.lit("1999900000"),
        psgc_muni_id=pl.col("id").str.slice(0, 7) + "000",
        psgc_brgy_id=pl.col("id"),
    )

    # Keep only necessary output columns
    matched_updates = matched.select(
        [
            "school_id",
            "psgc_region_id",
            "psgc_provhuc_id",
            "psgc_muni_id",
            "psgc_brgy_id",
        ]
    )

    # Join updates back using school_id as key
    meta_df = meta_df.join(matched_updates, on="school_id", how="left", suffix="_new")

    # Apply updates for only the matched school_id rows
    for col in ["psgc_region_id", "psgc_provhuc_id", "psgc_muni_id", "psgc_brgy_id"]:
        meta_df = meta_df.with_columns(
            **{col: pl.coalesce(pl.col(f"{col}_new"), pl.col(col))}
        ).drop(f"{col}_new")

    return meta_df
