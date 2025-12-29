import polars as pl

from foundation.common import FIXES, console, convert_trailing_roman, normalize_geo_name


def fix_barangay_enye_value(barangay):
    if barangay is None:
        return barangay
    return barangay.replace("Ã‘", "Ñ")


def apply_barangay_corrections(meta: pl.DataFrame, psgc: pl.DataFrame) -> pl.DataFrame:
    """
    Apply barangay name corrections based on CORRECTIONS,
    then look up the correct PSGC barangay code from psgc_df.
    """

    # Make a copy to avoid modifying original df unexpectedly
    df = meta.clone()

    rules = FIXES["barangay_corrections"]

    # Ensure psgc_brgy_id column exists
    if "psgc_brgy_id" not in df.columns:
        df = df.with_columns(psgc_brgy_id=pl.lit(None, dtype=pl.Utf8))

    for rule in rules:
        muni_code = rule["psgc_muni_id"]
        old_name = rule["old"]
        new_name = rule["new"]

        # Compute first 7 digits of the municipality ID
        muni_prefix = muni_code[:7]

        # Filter rows that need correction
        mask = (
            (pl.col("psgc_brgy_id").is_null())
            & (pl.col("psgc_muni_id") == muni_code)
            & (pl.col("barangay").str.to_uppercase() == old_name.upper())
        )

        # Check if any rows match
        matching = df.filter(mask)
        if matching.height > 0:
            # Apply the updated barangay name
            df = df.with_columns(
                pl.when(mask)
                .then(pl.lit(new_name))
                .otherwise(pl.col("barangay"))
                .alias("barangay")
            )

            # --- PSGC lookup ---
            matched = psgc.filter(
                (pl.col("id").str.starts_with(muni_prefix))
                & (pl.col("geo") == "Bgy")
                & (pl.col("name").str.to_uppercase() == new_name.upper())
            )

            if matched.height > 0:
                # Assign the PSGC barangay ID
                matched_id = matched.row(0)[matched.columns.index("id")]
                df = df.with_columns(
                    pl.when(mask)
                    .then(pl.lit(matched_id))
                    .otherwise(pl.col("psgc_brgy_id"))
                    .alias("psgc_brgy_id")
                )

    return df


def attach_psgc_brgy_id(meta: pl.DataFrame, psgc: pl.DataFrame) -> pl.DataFrame:
    """
    Attach PSGC barangay-level codes to schools.
    Requires that psgc_muni_id is already assigned by attach_psgc_muni_id().
    """
    if "psgc_muni_id" not in meta.columns:
        raise Exception("Missing dependency.")
    console.log("[cyan]Attaching PSGC barangay codes...[/cyan]")

    df = meta.clone()

    # ---------------------------------------------------------
    # 1. Prepare barangay-level PSGC (geo == 'Bgy')
    # ---------------------------------------------------------
    psgc_bgy = psgc.filter(pl.col("geo") == "Bgy").with_columns(
        normalized_name=pl.col("name").map_elements(
            lambda x: convert_trailing_roman(normalize_geo_name(x)),
            return_dtype=pl.Utf8,
        ),
        mun_prefix=pl.col("id").cast(pl.Utf8).str.slice(0, 7),
    )

    # ---------------------------------------------------------
    # 2. Normalize school barangay names
    # ---------------------------------------------------------
    df = df.with_columns(
        normalized_brgy=pl.col("barangay").map_elements(
            lambda x: convert_trailing_roman(
                normalize_geo_name(fix_barangay_enye_value(x))
            )
            if x is not None
            else None,
            return_dtype=pl.Utf8,
        )
    )

    # ---------------------------------------------------------
    # 3. Extract municipality prefix from assigned psgc_muni_id
    # ---------------------------------------------------------
    df = df.with_columns(
        mun_prefix=pl.col("psgc_muni_id").cast(pl.Utf8).str.slice(0, 7)
    )

    # ---------------------------------------------------------
    # 4. Join on (mun_prefix + normalized barangay name)
    # ---------------------------------------------------------
    merged = df.join(
        psgc_bgy.select(["normalized_name", "mun_prefix", "id"]),
        left_on=["mun_prefix", "normalized_brgy"],
        right_on=["mun_prefix", "normalized_name"],
        how="left",
    )

    # ---------------------------------------------------------
    # 5. Assign PSGC barangay code
    # ---------------------------------------------------------
    merged = merged.rename({"id": "psgc_brgy_id"})

    # ---------------------------------------------------------
    # 6. Cleanup temporary columns
    # ---------------------------------------------------------
    merged = merged.drop(
        [
            "normalized_brgy",
            "normalized_municipality",
            "normalized_name",
            "mun_prefix",
        ],
        strict=False,
    )

    return merged
