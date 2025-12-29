"""Address dimension builder that produces canonical hashes for locations."""

import hashlib
from dataclasses import dataclass

import polars as pl

from ..plugin import BaseExtractor, ExtractionContext, ExtractionResult

ADDR_KEY_COLS = [
    "psgc_region_id",
    "psgc_provhuc_id",
    "psgc_muni_id",
    "psgc_brgy_id",
]


@dataclass(frozen=True)
class AddressDimension:
    meta_with_hash: pl.DataFrame
    address_df: pl.DataFrame


def build_address_dimension(meta_psgc: pl.DataFrame) -> AddressDimension:
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

    return AddressDimension(meta_with_hash=meta_with_hash, address_df=addr_df)


class AddressDimensionExtractor(BaseExtractor):
    """Create canonical address dimension hashes for geo enrichment."""

    name = "address"
    depends_on = ["meta_psgc"]
    outputs = ["address", "meta_with_hash"]

    def extract(
        self,
        context: ExtractionContext,
        dependencies: dict[str, pl.DataFrame],
    ) -> ExtractionResult:
        del context
        dimension = build_address_dimension(dependencies["meta_psgc"])
        return ExtractionResult(
            tables={
                "meta_with_hash": dimension.meta_with_hash,
                "address": dimension.address_df,
            }
        )
