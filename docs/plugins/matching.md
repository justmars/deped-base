# Matching Extractor

## Purpose

Join the latest school metadata (`school_year_meta`) with the PSGC reference, resolving all hierarchical codes (region → province/HUC → municipality → barangay) plus division lookups.

## Source

- Inputs: `psgc` DataFrame from `PsgcExtractor` and `school_year_meta`.
- Downstream corrections: normalization helpers from `src/foundation/transforms`.

## Transform highlights

- `attach_psgc_region_codes`, `attach_psgc_provhuc_codes`, `attach_psgc_muni_id`, and `attach_psgc_brgy_id` sequentially enrich the metadata with PSGC codes.
- Post-match, the metadata is cleaned (division lookups, manual barangay corrections, MAGUINDANAO splits) via transforms such as `fill_missing_psgc`, `reorganize_school_geo_df`, and `get_divisions`.
- Outputs include division/jurisdiction IDs to support joins with the address dimension.

## Output tables

- `meta_psgc`: metadata enriched with PSGC geocodes and extra columns like `division_id`.

## Schema

Captured by `SCHEMAS["meta_psgc"]`, which expects `school_id`, `school_year`, and the full set of PSGC identifiers.

## Related docs

- [`docs/brgy_names.md`](../brgy_names.md) explores unmatched barangays and how the matching logic surfaces them for audits.
