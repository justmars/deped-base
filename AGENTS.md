# Pipeline Goal

- Ingest CSV/Excel (and similar) into a normalized SQLite database using **Polars**
- Prioritize reproducibility, idempotency, and auditability

## Architecture

- Keep ingestion **append-safe** and **restartable**
- Separate concerns:
  - extract (read files)
  - transform (clean/normalize)
  - load (write to SQLite)
- No schema changes unless required by the data contract

## Data Contract

- Define per-source:
  - expected columns + dtypes
  - primary/natural keys
  - required vs optional fields
  - allowed value sets (where applicable)
- Fail fast on contract breaks; log offending rows + reason

## Transform Rules

- Normalize names: snake_case columns, trimmed strings, consistent nulls
- Parse types explicitly (dates, ints, categoricals); avoid inference-only
- Deduplicate by key; prefer latest by `updated_at` (if present)
- Keep raw provenance fields (e.g., `source_file`, `source_row`, `ingested_at`)

## Load Rules (SQLite)

- Use transactions per batch; ensure atomic writes
- Use `UPSERT` for dimension tables; append or upsert facts by key
- Create/verify indexes for join keys and query hot paths
- Record ingestion run metadata (run id, counts, duration, checksum)

## Performance

- Scan lazily where possible; avoid materializing wide frames early
- Use chunked/batched inserts; avoid row-by-row writes
- Validate and cast before write to reduce SQLite type churn

## Observability

- Log counts at each stage: read, filtered, rejected, inserted, updated
- Emit a small QA report: null rates, duplicates removed, top invalid categories
- Keep sample of rejected rows for debugging

## Project Hygiene

- Update README/docs when:
  - schema changes
  - new sources added
  - contract/transform rules change
- Keep pipeline entrypoints small; extract reusable transforms/utilities
