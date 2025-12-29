# PSGC Pipeline

This site collects the knowledge needed to generate the `cli build` dataset (enrollment facts, geography, PSGC reference) and to extend it with new data through plugins.

## Initial ingestion flow

1. `cli prep` seeds the database (`DB_FILE`) with lookup tables such as `school_sizes` and `school_grades`.
2. `cli build` executes `PluginPipeline`, which discovers the extractors listed in `docs/plugins.md`:
   - `PsgcExtractor`: loads and normalizes the PSGC workbook.
   - `EnrollmentExtractor`: melts each `ENROLL_DIR` CSV, sanitizes `num_students`, and emits the metadata plus levels/facts.
   - `PsgcMatchingExtractor`: joins the metadata to PSGC hierarchies and resolves divisions.
   - `AddressDimensionExtractor`: hashes PSGC combos into `_addr_hash`/`address_id` pairs.
   - `GeoExtractor`: attaches coordinates and `address_id` to produce the final geography frame.
3. Each extractor logs `[green]Validated schema` when its tables pass contract checks in `src/foundation/schema.py`, and `cli build` persists the resulting DataFrames (`psgc`, `enroll`, `geos`, `addr`, etc.) via the legacy loaders.

## Extending with more data

- Add new plugin docs in `docs/plugins/*.md` following the existing uniform structure (purpose, source, transforms, outputs, schema, related docs).
- Ensure every new table has a schema entry in `src/foundation/schema.py`.
- Add fixtures/tests in `tests/` to cover the extractor logic and confirm the pipeline discovers it in `PluginPipeline`.
- Run `python -m pytest tests/test_extract_dataframes.py tests/test_cli.py` after `cli prep`/`cli build` to guard regression.

## Supplemental references

- [`docs/enrolment_origin.md`](enrolment_origin.md) explains the wide-format enrollment CSVs and the strategy for cleanly parsing `<grade>[_<strand>]_sex`.
- [`docs/brgy_names.md`](brgy_names.md) shows how the matching pipeline surfaces unmatched barangays for manual review.
To dig into a specific extractor, see the individual pages under `docs/plugins/`.
