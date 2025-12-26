import pandas as pd
from rich import print as rprint

from .common import env
from .extract_geodata import set_coordinates
from .extract_meta import unpack_enroll_data
from .extract_psgc import set_psgc
from .match_psgc_schools import match_psgc_schools


def extract_dataframes() -> tuple[
    pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame
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
    # Phase 3: Address dimension (Option 2, now safe)
    # -----------------------------
    ADDR_KEY_COLS = [
        "psgc_region_id",
        "psgc_provhuc_id",
        "psgc_muni_id",
        "psgc_brgy_id",
    ]

    rprint("[blue]Building address dimension...[/blue]")
    addr_key = meta_psgc[ADDR_KEY_COLS].fillna("").astype(str).agg("|".join, axis=1)

    meta_psgc["_addr_hash"] = pd.util.hash_pandas_object(addr_key, index=False)

    addresses = (
        meta_psgc[ADDR_KEY_COLS + ["_addr_hash"]]
        .drop_duplicates("_addr_hash")
        .reset_index(drop=True)
    )
    addresses["address_id"] = addresses.index + 1

    addr_df = (
        meta_psgc[["school_id", "school_year", "_addr_hash"]]
        .merge(addresses[["_addr_hash", "address_id"]], on="_addr_hash")
        .drop_duplicates()
    )
    # SQLite does not have a native int64 type, so we convert to int64
    addr_df["_addr_hash"] = addr_df["_addr_hash"].astype(dtype="int64")

    # -----------------------------
    # Phase 4: Geo enrichment
    # -----------------------------
    rprint("[blue]Setting coordinates...[/blue]")
    geo_df = set_coordinates(geo_file=geo_file, meta_df=meta_psgc)
    # SQLite does not have a native int64 type, so we convert to int64
    geo_df["_addr_hash"] = geo_df["_addr_hash"].astype(dtype="int64")

    return (psgc_df, enroll_df, geo_df, levels_df, addr_df)
