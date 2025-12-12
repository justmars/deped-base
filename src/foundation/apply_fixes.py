import pandas as pd


def fill_missing_psgc(meta_df: pd.DataFrame, psgc_df: pd.DataFrame) -> pd.DataFrame:
    """
    For rows in meta_df where psgc_muni_id is NA, match barangay names
    against filtered psgc_df rows and fill PSGC fields accordingly.
    """

    # Filter PSGC subset (your rules)
    psgc_subset = psgc_df[
        (psgc_df["cc"].fillna("").astype(str).str.startswith("1247"))
        & (psgc_df["id"].str.startswith("19999"))
        & (psgc_df["geo"] == "Bgy")
    ].copy()

    psgc_subset["barangay_norm"] = psgc_subset["name"].str.strip().str.lower()

    # Rows in meta_df needing PSGC data
    missing = meta_df[
        (meta_df["psgc_muni_id"].isna()) | (meta_df["psgc_brgy_id"].isna())
    ].copy()
    missing["barangay_norm"] = missing["barangay"].str.strip().str.lower()

    # Merge by normalized barangay name
    merged = missing.merge(
        psgc_subset[["id", "barangay_norm"]], on="barangay_norm", how="left"
    )

    # Keep only matched rows
    matched = merged[merged["id"].notna()].copy()

    # Compute PSGC fields
    matched["psgc_region_id"] = "1900000000"
    matched["psgc_provhuc_id"] = "1999900000"
    matched["psgc_muni_id"] = matched["id"].str[:7] + "000"
    matched["psgc_brgy_id"] = matched["id"]

    # Keep only necessary output columns
    matched_updates = matched[
        [
            "school_id",
            "psgc_region_id",
            "psgc_provhuc_id",
            "psgc_muni_id",
            "psgc_brgy_id",
        ]
    ]

    # Merge updates back using school_id as key
    meta_df = meta_df.merge(
        matched_updates, on="school_id", how="left", suffixes=("", "_new")
    )

    # Apply updates for only the matched school_id rows
    for col in ["psgc_region_id", "psgc_provhuc_id", "psgc_muni_id", "psgc_brgy_id"]:
        meta_df[col] = meta_df[f"{col}_new"].combine_first(meta_df[col])
        meta_df = meta_df.drop(columns=[f"{col}_new"])

    return meta_df
