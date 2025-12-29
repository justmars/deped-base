# Geodata Extractor

## Purpose

Attach longitude/latitude coordinates to the school-year frames and associate them with the canonical address IDs so analysts can map the `geos` table.

## Source

- Input: `meta_with_hash` previously produced by `AddressDimensionExtractor`.
- Coordinate file: CSV from `GEO_FILE`, containing `id` (school_id), `longitude`, `latitude`.

## Transform highlights

- The extractor keeps the enriched metadata, joins coordinates via `school_id`, and ensures `_addr_hash` is cast to `Int64`.
- It also joins the `address_id` column by matching on `school_id`, `school_year`, and `_addr_hash`, so the geography fact table retains both spatial and normalized address data.

## Output tables

- `geo`: final geography fact table that feeds into the `geos` SQLite table.

## Schema

Defined as `SCHEMAS["geo"]`, requiring PSGC IDs, `_addr_hash`, `address_id`, and coordinate columns.

## Related docs

- Supports the mapping in `docs/schema.md` where `geos` is described as a school-year geography fact table.
