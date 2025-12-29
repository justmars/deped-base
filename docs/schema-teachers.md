# Teacher Schema

This document explains the schema contract that backs the `teachers` fact table so you know what the plugin expects and what downstream consumers get.

## Purpose

The `teachers` table stores per-school, per-year headcounts for instructional roles. It keeps the same `school_year` values that power the enrollment tables so the data can be joined by time without reinventing school-year identifiers.

## Columns

- `school_year` (TEXT, required): Derived from the workbook filename (e.g., `2022-2023`). This column is used during ingestion and eventually replaced with `school_year_id` via `common.bulk_update` after the plugin writes to SQLite.
- `school_id` (TEXT, required): Normalized LIS/BEIS identifiers. The extractor reads both columns, falls back from LIS to BEIS when necessary, and coerces the result into nullable integers for stability.
- `level` (TEXT, required): Hard-coded tags (`es`, `jhs`, `shs`) based on the worksheet being processed.
- `position` (TEXT, required): Melted column headers that describe the role (e.g., `sped/sned teacher i`). These values become the basis for the `teacher_positions` lookup table.
- `num` (Nullable INTEGER): Headcount values after dropping zeros and parsing with `Int64`.

## Pipeline behavior

1. `src/foundation/plugins/hr.py` discovers every workbook under `SourcePaths.hr_dir` (`HR_DIR`, default `data/hr`), unpivots the level sheets, and returns the combined Polars frame for validation.
2. `schema.py` exposes `TEACHERS_SCHEMA` and registers it in `SCHEMAS` so `PluginPipeline` validates the extractor output before any writes happen.
3. `cli build` writes the validated `teachers` frame to SQLite, converts `school_year` into a foreign key (`school_year_id`), and materializes `teacher_positions` for easier analytics.

## Extending the schema

- If the HR workbooks add new columns that should be stored (e.g., `remarks` or `tenure`), extend `TEACHERS_SCHEMA` in `schema.py` and emit the new column from `src/foundation/plugins/hr.py`.
- When new school-year formats arrive, add a matching `SheetConfig` to `YEAR_SHEET_CONFIGS`. Without a config the extractor will raise an error so the file layout stays explicit.
- Add a regression test under `tests/test_plugins_hr.py` that writes a representative workbook to a temporary `HR_DIR` to keep the contract guarded.
