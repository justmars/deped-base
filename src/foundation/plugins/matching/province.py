import polars as pl

from ...common import console, normalize_geo_name


def prepare_psgc(psgc: pl.DataFrame) -> pl.DataFrame:
    """Normalize PSGC and extract prefixes."""
    df = psgc.with_columns(
        id=pl.col("id").cast(pl.Utf8),
        normalized_name=pl.col("name").map_elements(
            normalize_geo_name, return_dtype=pl.Utf8
        ),
        region_prefix=pl.col("id").cast(pl.Utf8).str.slice(0, 2),
        prov_prefix=pl.col("id").cast(pl.Utf8).str.slice(0, 5),
        mun_prefix=pl.col("id").cast(pl.Utf8).str.slice(0, 7),
    )
    return df


def build_region_maps(psgc_prepped: pl.DataFrame) -> dict:
    """
    Build HUC, SubMun, Province, Municipality maps per region_prefix.
    Returns { region_prefix: { huc, submun, prov, mun } }
    """

    region_maps = {}

    for region_prefix, grp in psgc_prepped.group_by("region_prefix"):
        region_prefix = region_prefix[0]
        # HUC rows
        if "city_class" in grp.columns:
            is_huc = grp.filter(
                pl.col("city_class")
                .cast(pl.Utf8)
                .str.to_lowercase()
                .str.contains("huc")
            )
            huc_df = is_huc.select(["normalized_name", "id"]).rename(
                {"normalized_name": "normalized", "id": "huc_id"}
            )
        else:
            huc_df = pl.DataFrame(schema={"normalized": pl.Utf8, "huc_id": pl.Utf8})

        # SubMun rows
        submun_df = (
            grp.filter(pl.col("geo") == "SubMun")
            .select(["normalized_name", "id"])
            .rename({"normalized_name": "normalized", "id": "submun_id"})
        )

        # Province rows
        prov_df = (
            grp.filter(pl.col("geo").str.to_lowercase() == "prov")
            .select(["normalized_name", "id"])
            .rename({"normalized_name": "normalized", "id": "prov_id"})
        )

        # Municipality rows
        mun_df = (
            grp.filter(pl.col("geo").is_in(["City", "Mun"]))
            .select(["normalized_name", "id"])
            .rename({"normalized_name": "normalized", "id": "mun_id"})
        )

        region_maps[region_prefix] = {
            "huc": huc_df,
            "submun": submun_df,
            "prov": prov_df,
            "mun": mun_df,
        }

    return region_maps


def build_global_lookups(
    region_maps: dict,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
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
            if df.height == 0:
                continue
            temp = df.with_columns(region_prefix=pl.lit(str(rp)))
            target_list.append(temp)

    # concat global tables
    huc_lookup = (
        pl.concat(huc_parts)
        if huc_parts
        else pl.DataFrame(
            schema={"normalized": pl.Utf8, "huc_id": pl.Utf8, "region_prefix": pl.Utf8}
        )
    )
    submun_lookup = (
        pl.concat(submun_parts)
        if submun_parts
        else pl.DataFrame(
            schema={
                "normalized": pl.Utf8,
                "submun_id": pl.Utf8,
                "region_prefix": pl.Utf8,
            }
        )
    )
    prov_lookup = (
        pl.concat(prov_parts)
        if prov_parts
        else pl.DataFrame(
            schema={"normalized": pl.Utf8, "prov_id": pl.Utf8, "region_prefix": pl.Utf8}
        )
    )
    mun_lookup = (
        pl.concat(mun_parts)
        if mun_parts
        else pl.DataFrame(
            schema={"normalized": pl.Utf8, "mun_id": pl.Utf8, "region_prefix": pl.Utf8}
        )
    )

    return huc_lookup, submun_lookup, prov_lookup, mun_lookup


def normalize_meta_for_provhuc(meta: pl.DataFrame) -> pl.DataFrame:
    """Normalize province & municipality for prov/huc matching."""
    meta = meta.with_columns(
        region_prefix=pl.col("psgc_region_id").cast(pl.Utf8).str.slice(0, 2),
        normalized_province=pl.col("province").map_elements(
            normalize_geo_name, return_dtype=pl.Utf8
        ),
        normalized_municipality=pl.col("municipality").map_elements(
            normalize_geo_name, return_dtype=pl.Utf8
        ),
    )
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
    meta = meta.join(
        huc_lookup.rename({"normalized": "normalized_municipality"}),
        on=["region_prefix", "normalized_municipality"],
        how="left",
    )

    # --- SubMun merge ---
    meta = meta.join(
        submun_lookup.rename({"normalized": "normalized_municipality"}),
        on=["region_prefix", "normalized_municipality"],
        how="left",
    )

    # --- Province merge ---
    meta = meta.join(
        prov_lookup.rename({"normalized": "normalized_province"}),
        on=["region_prefix", "normalized_province"],
        how="left",
    )

    # --- Pateros (municipality row) merge ---
    pateros = mun_lookup.filter(pl.col("normalized") == "pateros").select(
        ["region_prefix", "mun_id"]
    )
    pateros = pateros.rename({"mun_id": "pateros_mun_id"})
    meta = meta.join(pateros, on="region_prefix", how="left")

    # --- resolve priority (vectorized) ---
    meta = meta.with_columns(
        psgc_provhuc_id=pl.coalesce(
            pl.col("huc_id"),
            pl.col("submun_id"),
            pl.col("pateros_mun_id"),
            pl.col("prov_id"),
        )
    )

    # clean temp columns
    meta = meta.drop(
        [
            "huc_id",
            "submun_id",
            "prov_id",
            "pateros_mun_id",
            "region_prefix",
            "normalized_region",
            "normalized_province",
            "normalized_municipality",
        ],
        strict=False,
    )

    return meta


def attach_psgc_provhuc_codes(meta: pl.DataFrame, psgc: pl.DataFrame) -> pl.DataFrame:
    """
    Full optimized, region-aware, priority-respecting PSGC Prov/HUC mapping
    with post-processing overrides for:
        - SGA - North Cotabato
        - City of Isabela
    """
    if "psgc_region_id" not in meta.columns:
        raise Exception("Missing dependency.")
    console.log("[cyan]Attaching PSGC provincial / HUC / SubMun codes...[/cyan]")

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

    # ----------------------
    # A. SGA – North Cotabato in BARMM
    # ----------------------
    mask_sga_nc = (
        pl.col("province").cast(pl.Utf8).str.strip_chars().str.to_lowercase()
        == "(sga - north cotabato)"
    )
    if meta.filter(mask_sga_nc).height > 0:
        meta = meta.with_columns(
            pl.when(mask_sga_nc)
            .then(pl.lit("BARMM"))
            .otherwise(pl.col("region"))
            .alias("region"),
            pl.when(mask_sga_nc)
            .then(pl.lit("NORTH COTABATO"))
            .otherwise(pl.col("province"))
            .alias("province"),
            pl.when(mask_sga_nc)
            .then(pl.lit("1900000000"))
            .otherwise(pl.col("psgc_region_id"))
            .alias("psgc_region_id"),
            pl.when(mask_sga_nc)
            .then(pl.lit("1999900000"))
            .otherwise(pl.col("psgc_provhuc_id"))
            .alias("psgc_provhuc_id"),
        )

    # ----------------------
    # B. City of Isabela (Basilan)
    # ----------------------
    mask_isabela_city = (
        pl.col("province").cast(pl.Utf8).str.strip_chars().str.to_lowercase()
        == "city of isabela"
    )
    if meta.filter(mask_isabela_city).height > 0:
        meta = meta.with_columns(
            pl.when(mask_isabela_city)
            .then(pl.lit("0900000000"))
            .otherwise(pl.col("psgc_region_id"))
            .alias("psgc_region_id"),
            pl.when(mask_isabela_city)
            .then(pl.lit("0990101000"))
            .otherwise(pl.col("psgc_provhuc_id"))
            .alias("psgc_provhuc_id"),
        )

    return meta
