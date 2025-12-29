# Region Names Extractor

## Purpose

Load the curated region alias sheet so every canonical PSGC region ID is linked to its roman numeral, location name, and common shorthand before other extractors run.

- File: `data/regions.yml`
- Keys: `psgc_region_id`, `roman`, `location`, `common`, and optional `other` aliases (use the full 10-digit PSGC region code so the field can act as a foreign key).
## Source

- Default file: `data/regions.yml`.
- Override via `REGION_NAMES_FILE` environment variable to point to another YAML file with `psgc_region_id`, `roman`, `location`, `common`, and optional `other` aliases (use the full 10-digit PSGC region code so the field can act as a foreign key).
- File: `data/regions.yml`
- Keys: `psgc_region_id`, `roman`, `location`, `common`, and optional `other` aliases.

## Transform highlights

- The plugin simply reads the YAML file, ensures the values are strings, and emits them as a tidy table.
- It depends on `PsgcExtractor` so the region catalog is built once the PSGC codes are available.

## Output tables

- `region_names`: every alias pointing to a PSGC region ID. Treat `location` as the human-readable label so downstream matchers can join whichever variant they encounter.

## Schema

See `src/foundation/schema.py` (`REGION_NAMES_SCHEMA`). The table enforces `psgc_region_id` and `location` (both non-null) and optionally keeps roman/common/other aliases.

- `docs/plugins.md` and `docs/plugins/address.md` explain how these aliases help the matching pipeline stay resilient when the enrollment data uses shorthand region labels.
- Use `PluginPipeline.get_output_table` when you want to persist this optional table (see `cli build` for an example).
