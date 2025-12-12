import pandas as pd
from rich import print as rprint

from .common import normalize_geo_name


def prepare_psgc(psgc: pd.DataFrame) -> pd.DataFrame:
    """Normalize PSGC and extract prefixes."""
    df = psgc.copy()
    df["id"] = df["id"].astype(str)
    df["normalized_name"] = df["name"].apply(normalize_geo_name)
    df["region_prefix"] = df["id"].str[:2]
    df["prov_prefix"] = df["id"].str[:5]
    df["mun_prefix"] = df["id"].str[:7]
    return df


def build_region_maps(psgc_prepped: pd.DataFrame) -> dict:
    """
    Build HUC, SubMun, Province, Municipality maps per region_prefix.
    Returns { region_prefix: { huc, submun, prov, mun } }
    """

    region_maps = {}

    for rp, grp in psgc_prepped.groupby("region_prefix"):
        # HUC rows
        if "city_class" in grp.columns:
            is_huc = (
                grp["city_class"].astype(str).str.contains("huc", case=False, na=False)
            )
            huc_df = (
                grp[is_huc]
                .loc[:, ["normalized_name", "id"]]
                .rename(columns={"normalized_name": "normalized", "id": "huc_id"})
            )
        else:
            huc_df = pd.DataFrame(columns=["normalized", "huc_id"])

        # SubMun rows
        submun_df = (
            grp[grp["geo"] == "SubMun"]
            .loc[:, ["normalized_name", "id"]]
            .rename(columns={"normalized_name": "normalized", "id": "submun_id"})
        )

        # Province rows
        prov_df = (
            grp[grp["geo"].str.lower() == "prov"]
            .loc[:, ["normalized_name", "id"]]
            .rename(columns={"normalized_name": "normalized", "id": "prov_id"})
        )

        # Municipality rows
        mun_df = (
            grp[grp["geo"].isin(["City", "Mun"])]
            .loc[:, ["normalized_name", "id"]]
            .rename(columns={"normalized_name": "normalized", "id": "mun_id"})
        )

        region_maps[rp] = {
            "huc": huc_df.reset_index(drop=True),
            "submun": submun_df.reset_index(drop=True),
            "prov": prov_df.reset_index(drop=True),
            "mun": mun_df.reset_index(drop=True),
        }

    return region_maps


def build_global_lookups(
    region_maps: dict,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Combine all region-maps into global lookup tables for merging.
    Returns: huc_lookup, submun_lookup, prov_lookup, mun_lookup
    """

    huc_parts, submun_parts, prov_parts, mun_parts = [], [], [], []

    for rp, maps in region_maps.items():
        for key, target_list, idcol in [
            ("huc", huc_parts, "huc_id"),
            ("submun", submun_parts, "submun_id"),
            ("prov", prov_parts, "prov_id"),
            ("mun", mun_parts, "mun_id"),
        ]:
            df = maps[key]
            if df.empty:
                continue
            temp = df.copy()
            temp["region_prefix"] = rp
            target_list.append(temp)

    # concat global tables
    huc_lookup = (
        pd.concat(huc_parts, ignore_index=True)
        if huc_parts
        else pd.DataFrame(columns=["normalized", "huc_id", "region_prefix"])
    )
    submun_lookup = (
        pd.concat(submun_parts, ignore_index=True)
        if submun_parts
        else pd.DataFrame(columns=["normalized", "submun_id", "region_prefix"])
    )
    prov_lookup = (
        pd.concat(prov_parts, ignore_index=True)
        if prov_parts
        else pd.DataFrame(columns=["normalized", "prov_id", "region_prefix"])
    )
    mun_lookup = (
        pd.concat(mun_parts, ignore_index=True)
        if mun_parts
        else pd.DataFrame(columns=["normalized", "mun_id", "region_prefix"])
    )

    return huc_lookup, submun_lookup, prov_lookup, mun_lookup


def normalize_meta_for_provhuc(meta: pd.DataFrame) -> pd.DataFrame:
    """Normalize province & municipality for prov/huc matching."""
    meta = meta.copy()
    meta["psgc_region_id"] = meta["psgc_region_id"].astype(str)
    meta["region_prefix"] = meta["psgc_region_id"].str[:2]
    meta["normalized_province"] = meta["province"].apply(normalize_geo_name)
    meta["normalized_municipality"] = meta["municipality"].apply(normalize_geo_name)
    return meta


def merge_and_resolve_provhuc(
    meta,
    huc_lookup,
    submun_lookup,
    prov_lookup,
    mun_lookup,
):
    """Perform merges and priority resolution (HUC → SubMun → Pateros → Prov)."""

    # --- HUC merge ---
    meta = meta.merge(
        huc_lookup.rename(columns={"normalized": "normalized_municipality"}),
        how="left",
        on=["region_prefix", "normalized_municipality"],
    )

    # --- SubMun merge ---
    meta = meta.merge(
        submun_lookup.rename(columns={"normalized": "normalized_municipality"}),
        how="left",
        on=["region_prefix", "normalized_municipality"],
    )

    # --- Province merge ---
    meta = meta.merge(
        prov_lookup.rename(columns={"normalized": "normalized_province"}),
        how="left",
        on=["region_prefix", "normalized_province"],
    )

    # --- Pateros (municipality row) merge ---
    pateros = mun_lookup[mun_lookup["normalized"] == "pateros"][
        ["region_prefix", "mun_id"]
    ]
    pateros = pateros.rename(columns={"mun_id": "pateros_mun_id"})
    meta = meta.merge(pateros, how="left", on="region_prefix")

    # --- resolve priority (vectorized) ---
    meta["psgc_provhuc_id"] = (
        meta["huc_id"]
        .fillna(meta["submun_id"])
        .fillna(meta["pateros_mun_id"])
        .fillna(meta["prov_id"])
    )

    meta["psgc_provhuc_id"] = meta["psgc_provhuc_id"].where(
        meta["psgc_provhuc_id"].notna(), None
    )

    # clean temp columns
    meta = meta.drop(
        columns=[
            "huc_id",
            "submun_id",
            "prov_id",
            "pateros_mun_id",
            "region_prefix",
            "normalized_region",
            "normalized_province",
            "normalized_municipality",
        ],
        errors="ignore",
    )

    return meta


def attach_psgc_provhuc_codes(meta: pd.DataFrame, psgc: pd.DataFrame) -> pd.DataFrame:
    """
    Full optimized, region-aware, priority-respecting PSGC Prov/HUC mapping
    with post-processing overrides for:
        - SGA - North Cotabato
        - City of Isabela
    """
    if "psgc_region_id" not in meta.columns:
        raise Exception("Missing dependency.")
    rprint("[cyan]Attaching PSGC provincial / HUC / SubMun codes...[/cyan]")

    # -------------------------------------------
    # Step 1 — Normalize meta
    # -------------------------------------------
    meta = normalize_meta_for_provhuc(meta)

    # -------------------------------------------
    # Step 2 — Preprocess PSGC
    # -------------------------------------------
    psgc_prep = prepare_psgc(psgc)

    # Step 3 — Build region maps
    region_maps = build_region_maps(psgc_prep)

    # Step 4 — Build global lookup tables
    huc_lookup, submun_lookup, prov_lookup, mun_lookup = build_global_lookups(
        region_maps
    )

    # -------------------------------------------
    # Step 5 — PSGC matching (normal behavior)
    # -------------------------------------------
    meta = merge_and_resolve_provhuc(
        meta, huc_lookup, submun_lookup, prov_lookup, mun_lookup
    )

    # ===========================================
    # Step 6 — APPLY SPECIAL OVERRIDES (AFTER MAPPING)
    # ===========================================

    # Normalized province series for matching
    prov_series = meta["province"].astype(str).str.strip().str.lower()

    # ----------------------
    # A. SGA – North Cotabato in BARMM
    # ----------------------
    mask_sga_nc = prov_series == "(sga - north cotabato)"
    if mask_sga_nc.any():
        meta.loc[mask_sga_nc, "region"] = "BARMM"
        meta.loc[mask_sga_nc, "province"] = "NORTH COTABATO"
        meta.loc[mask_sga_nc, "psgc_region_id"] = "1900000000"
        meta.loc[mask_sga_nc, "psgc_provhuc_id"] = "1999900000"

    # ----------------------
    # B. City of Isabela (Basilan)
    # ----------------------
    mask_isabela_city = prov_series == "city of isabela"
    if mask_isabela_city.any():
        meta.loc[mask_isabela_city, "psgc_region_id"] = "0900000000"
        meta.loc[mask_isabela_city, "psgc_provhuc_id"] = "0990101000"

    return meta
