# Extraction Rules

Documenting the nuanced behaviors built into the Foundation extractor pipeline so that future contributors know exactly what transforms happen before data hits SQLite.

## PSGC loader

- Columns such as `2024_pop` and `status` are explicitly cast to `Int64`/`Utf8`, eliminating dtype warnings even when Polars cannot infer input types.
- When a PSGC row provides `old_names`, the loader prefers those values for the `name` column (e.g., DepEd province names stick to the historical label requested by the dataset) before reverting to `Name`.

## Enrollment parsing

- Each enrollment CSV is melted from wide to long form via `melt_enrollment_csv`.
- The `num_students` column is normalized using `sanitize_num_students`: commas/spaces are stripped, non-digit strings map to `NULL`, and the dtype is forced to `Int64`. This ensures comparisons (e.g., dropping zeros) never mix types.
- Invalid counts are logged per CSV, including a sample of rejected values, so the ingestion audit trail surfaces bad data without failing the run.
- Grade/strand/sex tokens are parsed with `split_grade_strand_sex`, which handles the new `sshs_acad`/`sshs_techpro` strand names alongside legacy two-part and three-part encodings.

## Metadata and matching

- Location cleaning driven by `data/fixes.yml` enforces canonical municipalities, province splits (e.g., Maguindanao north/south), and NIR substitutions before PSGC matching happens.
- `match_psgc_schools` progresses through region → province/HUC → municipality → barangay attaches, letting each plugin express its dependencies so the pipeline can reorder or replace steps without breaking the contract.
- Final metadata rows are reshuffled via `transforms/reorder.py` so PSGC identifiers sit next to their human-readable parents and are stored as strings.

## Provenance and governance

- The pipeline emits log messages for each major stage (source detection, melting, matching, address hashing) and keeps provenance fields (`source_file`, `_addr_hash`, etc.) so downstream loaders can tie data back to the raw asset.
- Validation rules (e.g., `sanitize_num_students`, schema validations) are centralized under `plugins/meta.py` to keep extractors idempotent and audit-ready.

This documentation supplements `README.md` and `docs/enrolment_origin.md` by capturing implementation-specific behaviors that are easy to miss without reading the code.
