import pandas as pd
from rich import print as rprint

from .common import PSGC_REGION_MAP, normalize_region_name


def map_psgc_region(region_name: str, psgc_map: dict) -> str | None:
    norm = normalize_region_name(region_name)
    alias = PSGC_REGION_MAP.get(norm, norm)  # use alias if exists
    if alias is None:
        return None  # intentionally ignored (e.g., PSO)
    return psgc_map.get(alias)


def attach_psgc_region_codes(meta: pd.DataFrame, psgc: pd.DataFrame) -> pd.DataFrame:
    """
    Attach PSGC region codes to a school metadata DataFrame.
    Only PSGC entries where geo == 'Reg' are allowed as region matches.
    """

    rprint("[cyan]Attaching PSGC region codes...[/cyan]")

    # ---------------------------------------------------------
    # 1. Filter PSGC to REGION rows only
    # ---------------------------------------------------------
    psgc_regions = psgc[psgc["geo"] == "Reg"].copy()

    # ---------------------------------------------------------
    # 2. Normalize PSGC region names
    # ---------------------------------------------------------
    psgc_regions["normalized"] = psgc_regions["name"].apply(normalize_region_name)

    # Build lookup: normalized PSGC region name → PSGC ID
    psgc_map = dict(zip(psgc_regions["normalized"], psgc_regions["id"]))

    # ---------------------------------------------------------
    # 3. Normalize school metadata region names
    # ---------------------------------------------------------
    meta = meta.copy()
    meta["normalized_region"] = meta["region"].apply(normalize_region_name)

    # ---------------------------------------------------------
    # 4. Map school → PSGC region ID (using alias table)
    # ---------------------------------------------------------
    meta["psgc_region_id"] = meta["normalized_region"].apply(
        lambda r: map_psgc_region(r, psgc_map)
    )

    # Remove schools with unmapped region (e.g., PSO or ARMM if intentionally excluded)
    meta = meta[meta["psgc_region_id"].notna()].copy()

    # ---------------------------------------------------------
    # 5. Reorder columns for clarity
    # ---------------------------------------------------------
    priority = ["region", "psgc_region_id", "school_id", "school_name"]
    remaining = [c for c in meta.columns if c not in priority]

    return meta[priority + remaining]
