from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence, Type, Union

import polars as pl


@dataclass(frozen=True)
class ColumnDef:
    name: str
    dtype: Union[Type[pl.DataType], pl.DataType]
    nullable: bool = True
    description: str = ""


@dataclass(frozen=True)
class TableSchema:
    """Defines the shape of a logical table produced by the pipeline."""

    name: str
    columns: Sequence[ColumnDef]
    primary_key: list[str] | None = None

    def to_polars_schema(self) -> dict[str, pl.DataType]:
        return {col.name: col.dtype for col in self.columns}

    def validate(self, df: pl.DataFrame) -> list[str]:
        errors: list[str] = []
        existing = set(df.columns)
        defined = {col.name for col in self.columns}

        missing = defined - existing
        if missing:
            errors.append(
                f"[{self.name}] missing columns: {', '.join(sorted(missing))}"
            )

        for col in self.columns:
            if col.name not in existing:
                continue
            if not col.nullable and df[col.name].null_count() > 0:
                errors.append(f"[{self.name}] column '{col.name}' contains nulls")

        return errors


PSGC_SCHEMA = TableSchema(
    name="psgc",
    primary_key=["id"],
    columns=[
        ColumnDef("id", pl.Utf8, nullable=False),
        ColumnDef("name", pl.Utf8),
        ColumnDef("cc", pl.Utf8),
        ColumnDef("geo", pl.Utf8),
        ColumnDef("old_names", pl.Utf8),
        ColumnDef("city_class", pl.Utf8),
        ColumnDef("income_class", pl.Utf8),
        ColumnDef("urban_rural", pl.Utf8),
        ColumnDef("2024_pop", pl.Utf8),
        ColumnDef("status", pl.Utf8),
    ],
)

ENROLLMENT_SCHEMA = TableSchema(
    name="enrollment",
    primary_key=["school_year", "school_id", "grade", "sex", "strand"],
    columns=[
        ColumnDef("school_year", pl.Utf8, nullable=False),
        ColumnDef("school_id", pl.Utf8, nullable=False),
        ColumnDef("grade", pl.Utf8),
        ColumnDef("sex", pl.Utf8),
        ColumnDef("strand", pl.Utf8),
        ColumnDef("num_students", pl.Int64),
    ],
)

SCHOOL_YEAR_META_SCHEMA = TableSchema(
    name="school_year_meta",
    primary_key=["school_year", "school_id"],
    columns=[
        ColumnDef("school_year", pl.Utf8, nullable=False),
        ColumnDef("school_id", pl.Utf8, nullable=False),
        ColumnDef("school_name", pl.Utf8),
        ColumnDef("sector", pl.Utf8),
        ColumnDef("school_management", pl.Utf8),
        ColumnDef("annex_status", pl.Utf8),
        ColumnDef("region", pl.Utf8),
        ColumnDef("province", pl.Utf8),
        ColumnDef("municipality", pl.Utf8),
        ColumnDef("barangay", pl.Utf8),
        ColumnDef("street_address", pl.Utf8),
        ColumnDef("legislative_district", pl.Utf8),
        ColumnDef("division", pl.Utf8),
        ColumnDef("school_district", pl.Utf8),
    ],
)

SCHOOL_LEVEL_SCHEMA = TableSchema(
    name="school_levels",
    primary_key=["school_id", "school_year", "level"],
    columns=[
        ColumnDef("school_id", pl.Utf8, nullable=False),
        ColumnDef("school_year", pl.Utf8, nullable=False),
        ColumnDef("level", pl.Utf8, nullable=False),
        ColumnDef("offered", pl.Boolean),
    ],
)

META_PSGC_SCHEMA = TableSchema(
    name="meta_psgc",
    primary_key=["school_id", "school_year"],
    columns=[
        ColumnDef("school_year", pl.Utf8, nullable=False),
        ColumnDef("school_id", pl.Utf8, nullable=False),
        ColumnDef("psgc_region_id", pl.Utf8),
        ColumnDef("psgc_provhuc_id", pl.Utf8),
        ColumnDef("psgc_muni_id", pl.Utf8),
        ColumnDef("psgc_brgy_id", pl.Utf8),
        ColumnDef("division", pl.Utf8),
        ColumnDef("division_id", pl.Utf8),
        ColumnDef("school_name", pl.Utf8),
        ColumnDef("province", pl.Utf8),
        ColumnDef("municipality", pl.Utf8),
        ColumnDef("barangay", pl.Utf8),
    ],
)

ADDRESS_SCHEMA = TableSchema(
    name="address",
    primary_key=["address_id"],
    columns=[
        ColumnDef("school_id", pl.Utf8, nullable=False),
        ColumnDef("school_year", pl.Utf8, nullable=False),
        ColumnDef("_addr_hash", pl.Int64, nullable=False),
        ColumnDef("address_id", pl.Int64, nullable=False),
    ],
)

GEO_SCHEMA = TableSchema(
    name="geo",
    primary_key=["school_id", "school_year"],
    columns=[
        ColumnDef("school_id", pl.Utf8, nullable=False),
        ColumnDef("school_year", pl.Utf8, nullable=False),
        ColumnDef("_addr_hash", pl.Int64),
        ColumnDef("address_id", pl.Int64),
        ColumnDef("longitude", pl.Float64),
        ColumnDef("latitude", pl.Float64),
        ColumnDef("psgc_region_id", pl.Utf8),
        ColumnDef("psgc_provhuc_id", pl.Utf8),
        ColumnDef("psgc_muni_id", pl.Utf8),
        ColumnDef("psgc_brgy_id", pl.Utf8),
    ],
)

REGION_NAMES_SCHEMA = TableSchema(
    name="region_names",
    primary_key=["psgc_region_id", "location"],
    columns=[
        ColumnDef("psgc_region_id", pl.Utf8, nullable=False),
        ColumnDef("roman", pl.Utf8),
        ColumnDef("location", pl.Utf8, nullable=False),
        ColumnDef("common", pl.Utf8),
        ColumnDef("other", pl.Utf8),
    ],
)

TEACHERS_SCHEMA = TableSchema(
    name="teachers",
    primary_key=["school_year", "school_id", "level", "position"],
    columns=[
        ColumnDef("school_year", pl.Utf8, nullable=False),
        ColumnDef("school_id", pl.Utf8, nullable=False),
        ColumnDef("level", pl.Utf8, nullable=False),
        ColumnDef("position", pl.Utf8, nullable=False),
        ColumnDef("num", pl.Int64),
    ],
)

DROPOUTS_SCHEMA = TableSchema(
    name="dropouts",
    primary_key=[
        "school_year",
        "school_id",
        "grade",
        "sex",
        "strand",
        "source_file",
        "source_row",
    ],
    columns=[
        ColumnDef("school_year", pl.Utf8, nullable=False),
        ColumnDef("school_id", pl.Utf8, nullable=False),
        ColumnDef("grade", pl.Utf8, nullable=False),
        ColumnDef("strand", pl.Utf8),
        ColumnDef("sex", pl.Utf8, nullable=False),
        ColumnDef("num_dropouts", pl.Int64),
        ColumnDef("source_file", pl.Utf8, nullable=False),
        ColumnDef("source_row", pl.Int64, nullable=False),
        ColumnDef("ingested_at", pl.Datetime),
    ],
)

SCHEMAS = {
    "psgc": PSGC_SCHEMA,
    "enrollment": ENROLLMENT_SCHEMA,
    "school_year_meta": SCHOOL_YEAR_META_SCHEMA,
    "school_levels": SCHOOL_LEVEL_SCHEMA,
    "meta_psgc": META_PSGC_SCHEMA,
    "address": ADDRESS_SCHEMA,
    "geo": GEO_SCHEMA,
    "region_names": REGION_NAMES_SCHEMA,
    "teachers": TEACHERS_SCHEMA,
    "dropouts": DROPOUTS_SCHEMA,
}
