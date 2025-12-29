import hashlib
from dataclasses import dataclass
from pathlib import Path

import polars as pl
from rich import print as rprint

from .common import env
from .extract_geodata import set_coordinates
from .extract_meta import unpack_enroll_data
from .extract_psgc import set_psgc
from .match_psgc_schools import match_psgc_schools


@dataclass(frozen=True)
class ExtractedFrames:
    """Named container for the pipeline tables produced during extraction."""

    psgc: pl.DataFrame
    enrollment: pl.DataFrame
    geo: pl.DataFrame
    levels: pl.DataFrame
    address: pl.DataFrame


@dataclass(frozen=True)
class _SourcePaths:
    enroll_dir: Path
    psgc_file: Path
    geo_file: Path


@dataclass(frozen=True)
class _AddressDimension:
    meta_with_hash: pl.DataFrame
    address_df: pl.DataFrame


def _resolve_source_paths() -> _SourcePaths:
    """Read configured source paths and log their locations.

    Returns:
        _SourcePaths: Paths to enrollment folder, PSGC file, and geo file.
    """

    enroll_dir = env.path("ENROLL_DIR")
    psgc_file = env.path("PSGC_FILE")
    geo_file = env.path("GEO_FILE")

    for label, path in (
        ("enroll_dir", enroll_dir),
        ("psgc_file", psgc_file),
        ("geo_file", geo_file),
    ):
        rprint(f"Detected: {label}={path}")

    return _SourcePaths(enroll_dir=enroll_dir, psgc_file=psgc_file, geo_file=geo_file)


def _build_address_dimension(meta_psgc: pl.DataFrame) -> _AddressDimension:
    """Create the address hash and canonical address table for PSGC metadata.

    Args:
        meta_psgc (pl.DataFrame): PSGC-matched school metadata.

    Returns:
        _AddressDimension: Meta enriched with `_addr_hash` plus address table.
    """

    ADDR_KEY_COLS = [
        "psgc_region_id",
        "psgc_provhuc_id",
        "psgc_muni_id",
        "psgc_brgy_id",
    ]

    rprint("[blue]Building address dimension...[/blue]")

    def _hash_row(cols):
        key = "|".join(str(v) if v is not None else "" for v in cols)
        return int(hashlib.md5(key.encode()).hexdigest()[:15], 16)

    meta_with_hash = meta_psgc.with_columns(
        pl.concat_list(ADDR_KEY_COLS)
        .map_elements(_hash_row, return_dtype=pl.Int64)
        .alias("_addr_hash")
    )

    addresses = (
        meta_with_hash.select(ADDR_KEY_COLS + ["_addr_hash"])
        .unique(subset=["_addr_hash"])
        .with_columns(pl.int_range(1, pl.len() + 1).alias("address_id"))
        .with_columns(pl.col("_addr_hash").cast(pl.Int64))
    )

    addr_df = (
        meta_with_hash.select(["school_id", "school_year", "_addr_hash"])
        .join(
            addresses.select(["_addr_hash", "address_id"]),
            on="_addr_hash",
            how="left",
        )
        .unique()
        .with_columns(pl.col("_addr_hash").cast(pl.Int64))
    )

    return _AddressDimension(meta_with_hash=meta_with_hash, address_df=addr_df)


def extract_dataframes() -> ExtractedFrames:
    """Load reference data, enrich geo dimensions, and return named tables.

    Returns:
        ExtractedFrames: PSGC, enrollment, geo, levels, and address tables.
    """

    paths = _resolve_source_paths()

    # Reference data
    psgc_df = set_psgc(f=paths.psgc_file)

    # Enrollment metadata and levels
    school_year_meta, enroll_df, levels_df = unpack_enroll_data(
        enrolment_folder=paths.enroll_dir
    )

    # PSGC matching pipeline
    rprint("[blue]Matching schools to PSGC...[/blue]")
    meta_psgc = match_psgc_schools(
        psgc_df=psgc_df,
        school_location_df=school_year_meta,
    )

    # Address dimension
    dimension = _build_address_dimension(meta_psgc=meta_psgc)

    # Geo enrichment
    rprint("[blue]Setting coordinates...[/blue]")
    geo_df = set_coordinates(geo_file=paths.geo_file, meta_df=dimension.meta_with_hash)
    geo_df = geo_df.with_columns(pl.col("_addr_hash").cast(pl.Int64))

    return ExtractedFrames(
        psgc=psgc_df,
        enrollment=enroll_df,
        geo=geo_df,
        levels=levels_df,
        address=dimension.address_df,
    )
