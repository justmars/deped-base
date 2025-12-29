import hashlib

import polars as pl
from rich import print as rprint

from .common import env
from .extract_geodata import set_coordinates
from .extract_meta import unpack_enroll_data
from .extract_psgc import set_psgc
from .match_psgc_schools import match_psgc_schools


def extract_dataframes() -> tuple[
    pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame
]:
    enroll_dir = env.path("ENROLL_DIR")
    psgc_file = env.path("PSGC_FILE")
    geo_file = env.path("GEO_FILE")

    rprint(f"Detected: {enroll_dir=}")
    rprint(f"Detected: {psgc_file=}")
    rprint(f"Detected: {geo_file=}")

    # -----------------------------
    # Load reference data
    # -----------------------------
    psgc_df = set_psgc(f=psgc_file)

    # -----------------------------
    # Phase 1: enrollment extraction
    # -----------------------------
    school_year_meta, enroll_df, levels_df = unpack_enroll_data(
        enrolment_folder=enroll_dir
    )

    # -----------------------------
    # Phase 2: PSGC matching
    # -----------------------------
    rprint("[blue]Matching schools to PSGC...[/blue]")
    meta_psgc = match_psgc_schools(
        psgc_df=psgc_df,
        school_location_df=school_year_meta,
    )

    # -----------------------------
    # Phase 3: Address dimension
    # -----------------------------
    ADDR_KEY_COLS = [
        "psgc_region_id",
        "psgc_provhuc_id",
        "psgc_muni_id",
        "psgc_brgy_id",
    ]

    rprint("[blue]Building address dimension...[/blue]")

    # Create address hash by combining PSGC fields
    def hash_row(cols):
        """Hash concatenated PSGC fields."""
        key = "|".join(str(v) if v is not None else "" for v in cols)
        return int(hashlib.md5(key.encode()).hexdigest()[:15], 16)

    meta_psgc = meta_psgc.with_columns(
        pl.concat_list(ADDR_KEY_COLS)
        .map_elements(hash_row, return_dtype=pl.Int64)
        .alias("_addr_hash")
    )

    # Build addresses dimension
    addresses = meta_psgc.select(ADDR_KEY_COLS + ["_addr_hash"]).unique(
        subset=["_addr_hash"]
    )
    addresses = addresses.with_columns(
        pl.int_range(1, pl.len() + 1).alias("address_id")
    )

    # Join addresses back to meta
    addr_df = (
        meta_psgc.select(["school_id", "school_year", "_addr_hash"])
        .join(
            addresses.select(["_addr_hash", "address_id"]), on="_addr_hash", how="left"
        )
        .unique()
    )

    # Convert hash to int64 for SQLite
    addr_df = addr_df.with_columns(pl.col("_addr_hash").cast(pl.Int64))

    # -----------------------------
    # Phase 4: Geo enrichment
    # -----------------------------
    rprint("[blue]Setting coordinates...[/blue]")
    geo_df = set_coordinates(geo_file=geo_file, meta_df=meta_psgc)
    geo_df = geo_df.with_columns(pl.col("_addr_hash").cast(pl.Int64))

    return (psgc_df, enroll_df, geo_df, levels_df, addr_df)
