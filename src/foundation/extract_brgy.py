import pandas as pd
from rich import print as rprint

from .common import FIXES, convert_trailing_roman, normalize_geo_name


def fix_barangay_enye_value(barangay):
    if pd.isna(barangay):
        return barangay
    return barangay.replace("Ã‘", "Ñ")


def apply_barangay_corrections(meta: pd.DataFrame, psgc: pd.DataFrame) -> pd.DataFrame:
    """
    Apply barangay name corrections based on CORRECTIONS,
    then look up the correct PSGC barangay code from psgc_df.
    """

    # Make a copy to avoid modifying original df unexpectedly
    df = meta.copy()

    rules = FIXES["barangay_corrections"]

    # Ensure psgc_brgy_id column exists
    if "psgc_brgy_id" not in df.columns:
        df["psgc_brgy_id"] = None

    for rule in rules:
        muni_code = rule["psgc_muni_id"]
        old_name = rule["old"]
        new_name = rule["new"]

        # Compute first 7 digits of the municipality ID
        muni_prefix = muni_code[:7]

        # Mask rows that need correction
        mask = (
            df["psgc_brgy_id"].isna()
            & (df["psgc_muni_id"] == muni_code)
            & (df["barangay"].str.upper() == old_name.upper())
        )

        if mask.any():
            # Apply the updated barangay name
            df.loc[mask, "barangay"] = new_name

            # --- PSGC lookup ---
            matched = psgc[
                (psgc["id"].str.startswith(muni_prefix))
                & (psgc["geo"] == "Bgy")
                & (psgc["name"].str.upper() == new_name.upper())
            ]

            if not matched.empty:
                # Assign the PSGC barangay ID
                df.loc[mask, "psgc_brgy_id"] = matched.iloc[0]["id"]

    return df


def attach_psgc_brgy_id(meta: pd.DataFrame, psgc: pd.DataFrame) -> pd.DataFrame:
    """
    Attach PSGC barangay-level codes to schools.
    Requires that psgc_muni_id is already assigned by attach_psgc_muni_id().
    """
    if "psgc_muni_id" not in meta.columns:
        raise Exception("Missing dependency.")
    rprint("[cyan]Attaching PSGC barangay codes...[/cyan]")

    df = meta.copy()

    # ---------------------------------------------------------
    # 1. Prepare barangay-level PSGC (geo == 'Bgy')
    # ---------------------------------------------------------
    psgc_bgy = psgc[psgc["geo"] == "Bgy"].copy()
    psgc_bgy["normalized_name"] = (
        psgc_bgy["name"].apply(normalize_geo_name).apply(convert_trailing_roman)
    )
    psgc_bgy["mun_prefix"] = psgc_bgy["id"].str[:7]  # municipality-level PSGC prefix

    # ---------------------------------------------------------
    # 2. Normalize school barangay names
    # ---------------------------------------------------------
    df["normalized_brgy"] = (
        df["barangay"]
        .apply(fix_barangay_enye_value)
        .apply(normalize_geo_name)
        .apply(convert_trailing_roman)
    )

    # ---------------------------------------------------------
    # 3. Extract municipality prefix from assigned psgc_muni_id
    # ---------------------------------------------------------
    df["mun_prefix"] = df["psgc_muni_id"].astype(str).str[:7]

    # ---------------------------------------------------------
    # 4. Merge on (mun_prefix + normalized barangay name)
    # ---------------------------------------------------------
    merged = df.merge(
        psgc_bgy[["normalized_name", "mun_prefix", "id"]],
        how="left",
        left_on=["mun_prefix", "normalized_brgy"],
        right_on=["mun_prefix", "normalized_name"],
    )

    # ---------------------------------------------------------
    # 5. Assign PSGC barangay code
    # ---------------------------------------------------------
    merged = merged.rename(columns={"id": "psgc_brgy_id"})

    # ---------------------------------------------------------
    # 6. Cleanup temporary columns
    # ---------------------------------------------------------
    merged = merged.drop(
        columns=[
            "normalized_brgy",
            "normalized_municipality",
            "normalized_name",
            "mun_prefix",
        ],
        errors="ignore",
    )

    return merged
