# Foundation

This project combines:

1. [Project Bukas](https://www.deped.gov.ph/machine-ready-files/) enrollment CSVs;
2. [Philippine Standard Geographic Code (PSGC)](https://psa.gov.ph/classification/psgc);
3. Manually curated geocoordinates.

The result is a time-aware, PSGC-anchored warehouse of enrollment facts, school-year metadata, geography, and canonical addresses. `cli build` creates the database that feeds downstream school-observation analytics, scorecards, mapping workloads, and insight tooling.

> [!IMPORTANT]
> The longitude / latitude file is presently generated through a third-party repository. This should be integrated here in the future.

## Architecture highlights

- `src/foundation/pipeline.py` orchestrates the extract → transform → load flow while still returning the legacy `ExtractedFrames`.
- `src/foundation/plugins/` now exposes a plug-and-play extractor system: each `BaseExtractor` declares `depends_on`/`outputs`, the runtime enforces `SCHEMAS`, and discovery is automatic via filesystem scanning (no registration required).
- `src/foundation/transforms/` houses reusable cleanup helpers (location fixes, name normalization, reorder utilities) so extractors stay focused.
- `src/foundation/loaders/` handles SQLite writes (enrollment tables, lookup wiring) off the critical extraction path so tests can mock ingestion easily.
- `src/foundation/schema.py` centralizes every table contract; plugin outputs that do not match raise `PipelineExecutionError` before any data hits SQLite.

## Extraction rules

`docs/extraction_rules.md` details the PSGC naming fixes, enrollment melting heuristics, `num_students` sanitization, invalid-row logging, matching dependencies, and address hashing that underlies the pipeline. Keep it synchronized whenever new transforms or validation rules are introduced.

## Development

```sh
uv sync --all-extras # creates the foundation package, defined in pyproject.toml
source .venv/bin/activate # enter the virtual environment
```

## Run

Rename `env.example` to `.env` (it configures `DB_FILE`, `ENROLL_DIR`, `PSGC_FILE`, etc.).

```sh
zensical serve # show docs
cli            # show available commands
cli prep       # creates the database and seeds reference tables
cli build      # runs all extractors, validates schemas, and writes the tables
```

## Extensions & docs

- `docs/plugins.md` explains the plugin workflow and how to add new extractors (e.g., teacher data) without touching the orchestration.
- `docs/plugins/*.md` contains per-extractor reference pages (PSGC loader, enrollment melt, matching, address dimension, geodata, and HR teacher counts) so you can trace each table from source to SQLite.
- `docs/enrolment_origin.md` and `docs/brgy_names.md` provide deeper context for the enrollment source files and PSGC matching edge cases, respectively.
