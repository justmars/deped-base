from pathlib import Path

import pandas as pd

from .common import FIXES


# ========================================================
# YAML-DRIVEN CLEANER (clean + optimized)
# ========================================================
def clean_meta_location_names(meta: pd.DataFrame) -> pd.DataFrame:
    """
    Clean incorrect municipality, province, and region names
    fully driven by YAML configurations.
    """

    meta = meta.copy()

    # Normalized lowercase helpers
    prov_series = meta["province"].astype(str).str.strip().str.lower()
    muni_series = meta["municipality"].astype(str).str.strip().str.lower()
    school_ids = meta["school_id"].astype(int)

    # =====================================================
    # 1. Provincial-level municipality corrections
    # =====================================================
    for rule in FIXES.get("provincial_muni_fixes", []):
        province_raw = rule["province"].strip().lower()
        muni_raw = rule["municipality"].strip().lower()
        corrected = rule["corrected"]

        mask = (prov_series == province_raw) & (muni_series == muni_raw)
        meta.loc[mask, "municipality"] = corrected

    muni_series = meta["municipality"].astype(str).str.strip().str.lower()

    # =====================================================
    # 2. Municipality-only corrections
    # =====================================================
    for raw, corrected in FIXES.get("municipality_fixes", {}).items():
        raw_norm = raw.lower()
        mask = muni_series == raw_norm
        meta.loc[mask, "municipality"] = corrected

    muni_series = meta["municipality"].astype(str).str.strip().str.lower()

    # =====================================================
    # 3. Province fixes by school ID
    # =====================================================
    for province_name, id_list in FIXES.get("province_fixes_by_school_id", {}).items():
        mask = school_ids.isin(id_list)
        meta.loc[mask, "province"] = province_name.upper()

    prov_series = meta["province"].astype(str).str.strip().str.lower()

    # =====================================================
    # 4. Region overrides
    # =====================================================
    for rule in FIXES.get("special_fixes", []):
        cond = pd.Series(True, index=meta.index)

        # Build `when` condition
        for col, val in rule["when"].items():
            series = meta[col].astype(str).str.strip().str.lower()
            cond &= series == val.lower()

        # Apply `set` updates
        for col, val in rule["set"].items():
            meta.loc[cond, col] = val

    # =====================================================
    # 5. NIR assignment
    # =====================================================
    nir = FIXES.get("nir_rule", {})
    nir_provinces = {x.lower() for x in nir.get("provinces", [])}
    nir_region = nir.get("set_region", "NIR")

    mask_nir = prov_series.isin(nir_provinces)
    meta.loc[mask_nir, "region"] = nir_region

    # =====================================================
    # 6. Maguindanao split
    # =====================================================
    split_cfg = FIXES.get("maguindanao_split", {})

    norte_munis = {m.lower() for m in split_cfg.get("norte_municipalities", [])}
    sur_is_default = split_cfg.get("sur_is_default", True)

    is_maguindanao = prov_series == "maguindanao"

    # Norte
    mask_norte = is_maguindanao & muni_series.isin(norte_munis)
    meta.loc[mask_norte, "province"] = "Maguindanao del Norte"

    # Sur = remaining
    if sur_is_default:
        mask_sur = is_maguindanao & ~muni_series.isin(norte_munis)
        meta.loc[mask_sur, "province"] = "Maguindanao del Sur"

    return meta
