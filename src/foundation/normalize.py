import re

import pandas as pd


def _digits_only(s: str) -> str:
    """Return only the digits from the string, or '' if NA."""
    if pd.isna(s):
        return ""
    return re.sub(r"\D", "", str(s))


def get_unique_regions(df: pd.DataFrame):
    _df = df[["psgc_region_id", "region"]].drop_duplicates(
        subset=["psgc_region_id", "region"]
    )
    _df.columns = ["id", "name"]
    _df.sort_values(by="id")
    return _df


def get_unique_provinces(df: pd.DataFrame, psgc_df: pd.DataFrame) -> pd.DataFrame:
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
        df["psgc_provhuc_id"]
        .astype("string")
        .replace(["", "None", "nan"], pd.NA)
        .dropna()
        .map(_digits_only)
    )
    prov_series = prov_series[prov_series.str.len() > 0].unique().tolist()
    prov_df = pd.DataFrame({"raw_provhuc_id": prov_series})

    # Canonical 5-digit province code
    prov_df["prov_key"] = prov_df["raw_provhuc_id"].str[:5]

    # --- 2) Prepare PSGC province layer ---
    psgc_prov = psgc_df[psgc_df["geo"] == "Prov"].copy()
    psgc_prov["id"] = psgc_prov["id"].astype("string").map(_digits_only)
    psgc_prov["prov_key"] = psgc_prov["id"].str[:5]
    psgc_prov = psgc_prov[["prov_key", "id", "name"]].drop_duplicates("prov_key")

    # --- 3) Prepare PSGC region layer (for region mapping) ---
    psgc_reg = psgc_df[psgc_df["geo"] == "Reg"].copy()
    psgc_reg["id"] = psgc_reg["id"].astype("string").map(_digits_only)
    psgc_reg["reg_key"] = psgc_reg["id"].str[:2]  # Regions map via 2-digit code
    psgc_reg = psgc_reg[["reg_key", "id"]].rename(columns={"id": "region_id"})

    # --- 4) Merge school province list → PSGC provinces ---
    merged = prov_df.merge(psgc_prov, how="left", on="prov_key", validate="m:1")

    # Remove unmatched provinces
    merged = merged.dropna(subset=["id"])

    # --- 5) Add region_code based on province PSGC code prefix ---
    merged["reg_key"] = merged["id"].str[:2]
    merged = merged.merge(psgc_reg, on="reg_key", how="left", validate="m:1")

    # --- 6) Final cleanup ---
    final = (
        merged[["id", "name", "region_id"]]
        .drop_duplicates(subset=["id"])
        .reset_index(drop=True)
    )

    return final


def get_divisions(df: pd.DataFrame):
    """Generate a unique division lookup table from a PSGC-aligned dataframe.

    This function extracts unique (region, division) pairs from the input
    dataframe and assigns each division a deterministic ID within its region.
    The ID is created by ordering divisions alphabetically within each region
    and assigning a sequential counter. The resulting identifier takes the form:

        "<psgc_region_id>-<sequence_number>"

    For example, if Region 01 has three divisions sorted alphabetically,
    their division IDs will be: `01-1`, `01-2`, `01-3`.

    Args:
        df (pd.DataFrame): A dataframe containing at least the columns
            `psgc_region_id` (region code) and `division`
            (DepEd division name).

    Returns:
        pd.DataFrame: A dataframe with one row per unique division containing:
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
        df[["psgc_region_id", "division"]]
        .drop_duplicates()
        .sort_values(["psgc_region_id", "division"])
        .reset_index(drop=True)
    )
    div["division_seq"] = div.groupby("psgc_region_id").cumcount() + 1

    div["division_id"] = (
        div["psgc_region_id"].astype(str) + "-" + div["division_seq"].astype(str)
    )
    return div
