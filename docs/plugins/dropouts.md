# Dropouts Plugin

The `DropoutsExtractor` loads the Excel workbooks published for each school year and returns a tidy `dropouts` fact table that records dropout counts by `[school_year, school_id, grade, sex, strand]`. The extractor also records provenance (`source_file`, `source_row`, `ingested_at`) so analysts can trace every rejection.

## Source files

- Name each workbook `<year>-dropouts.xlsx` (e.g., `2022-2023-dropouts.xlsx`, `2023-2024-dropouts.xlsx`). The extractor expects the files to live under `DROPOUT_DIR` (default `data/dropout`).
- The loader knows about three schemas:
  - 2022-2023: simple headers with a `School ID` column plus grade/strand/sex combinations.
  - 2023-2024: merged headers that span two rows before flattening into the raw column names.
  - 2024-2025: snake_case headers (`dropout_k_male`, `dropout_g1_female`, …) that align with the most recent naming convention.
- Each configuration declares the sheet, the column ranges, and the parser to apply. Missing files raise `FileNotFoundError` so the pipeline fails fast when a release is incomplete.

## Schema contract

- **Table**: `dropouts`
- **Primary key**: `school_year`, `school_id`, `grade`, `sex`, `strand`, `source_file`, `source_row`
- **Columns**
  - `school_year` (`text`, required) – derived from the workbook filename.
  - `school_id` (`text`, required) – LIS/BEIS identifier normalized as text.
  - `grade` (`text`, required) – canonical grade label (`kinder`, `g1`, `g11`, etc.).
  - `strand` (`text`, optional) – populated for SHS columns that list a strand.
  - `sex` (`text`, required) – `m` or `f`.
  - `num_dropouts` (`integer`) – sanitized dropout count (zeros and invalid cells are dropped before this column is populated).
  - `source_file` (`text`, required) – workbook filename.
  - `source_row` (`integer`, required) – Excel row that supplied the raw value.
  - `ingested_at` (`datetime`) – UTC timestamp when the plugin ran.

The pipeline validates the frame against `SCHEMAS["dropouts"]` before any downstream writes. Duplicate rows are collapsed (keeping the latest ingestion per `(school_year, school_id, grade, strand, sex)` tuple), and invalid parses are logged with a sample of offending `raw_col` values.

## Environment configuration

- `DROPOUT_DIR`: folder containing the dropout workbooks. Defaults to `data/dropout`.

The extractor reads `context.paths.dropout_dir` so the path can be configured just like the other data sources.

## Observability

- Each file logs how many rows were melted, how many valid rows were emitted, and how many were rejected.
- Metrics exposed via `ExtractionResult.metrics` include `dropouts_files`, `dropouts_rows_melted`, `dropouts_valid_rows`, `dropouts_invalid_rows`, and `dropouts_duplicates_removed`.
- Invalid rows keep a short sample so operators can tweak parsers when the Excel layout changes.

## Extending the extractor

1. Add a new entry to `DROP_OUT_CONFIGS` describing the sheet name, column ranges, and header layout for the new workbook.
2. Extend `_parser_for_schema` with a parser function (e.g., `_parse_dropout_2025`) that returns `(grade, strand, sex)` tuples normalized to the canonical labels.
3. Drop the workbook into `DROPOUT_DIR` and rerun `cli build`. The extractor will detect the new file, log QA metrics, and emit additional rows for `dropouts`.

The ingestion run stores provenance for every row (`source_file`, `source_row`, `ingested_at`), so corrected workbooks can be replayed safely without losing traceability.
