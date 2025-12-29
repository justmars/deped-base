# Address Dimension Extractor

## Purpose

Creates canonical `_addr_hash` values and an address bridge table so schools share normalized location identities across years.

## Source

- Input: `meta_psgc` DataFrame enriched with PSGC codes.

## Transform highlights

- `_hash_row` combines the PSGC-level IDs (`psgc_region_id`, `psgc_provhuc_id`, `psgc_muni_id`, `psgc_brgy_id`) into a deterministic string, then hashes it to `_addr_hash`.
- The deduplicated hash table gets sequential `address_id`s so an address dimension can be reused across school years.
- The extractor emits both the hashed metadata (`meta_with_hash`) and the `address` bridge table.

## Output tables

- `meta_with_hash`: intermediate frame carrying `_addr_hash`, used by the geo extractor.
- `address`: table linking each school-year to `_addr_hash` and `address_id`.

## Schema

`SCHEMAS["address"]` enforces the presence of `_addr_hash` and `address_id`, while `meta_with_hash` currently feeds the `geo` schema.

## Related docs

- Ensures the normalized identities behind `geos` and `addr` tables in the SQLite output; no extra doc yet.
