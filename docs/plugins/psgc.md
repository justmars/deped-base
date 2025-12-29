# PSGC Extractor

## Purpose

Load the official PSGC Excel worksheet (`PSGC` sheet), normalize IDs/names, and provide the canonical geography reference that downstream plugins join into.

## Source

- File: configured via `PSGC_FILE` in the `.env`
- Sheet: `PSGC`, columns mapped to `id`, `name`, `geo`, `city_class`, `income_class`, `urban_rural`, `status`.

## Transform highlights

- Selects only the columns needed by downstream matchers and pads the PSGC code to a 10-digit string.
- Cleans `income_class` and `city_class` by stripping stars/blank names.
- Normalizes `name` for provinces by preferring `old_names` when available and replacing `"-"` with null on string columns to keep missing explicit.

## Output tables

- `psgc`: raw reference data that is both written to the SQLite `psgc` table and consumed by matching extractors.

## Schema

Described in `src/foundation/schema.py` via `PSGC_SCHEMA`; `id` is non-nullable and the table includes classification columns such as `geo`, `city_class`, and `income_class`.

## Related docs

- [`docs/brgy_names.md`](../brgy_names.md) shows how missing barangay matches are surfaced from `psgc` vs. matched metadata.
