# Foundation

This project integrates data from these sources to generate a single sqlite file:

1. [Project Bukas](https://www.deped.gov.ph/machine-ready-files/) enrollment datasets;
2. [Philippine Standard Geographic Code (PSGC)](https://psa.gov.ph/classification/psgc) using the address fields from enrollment datasets;
3. Additional geospatial metadata from manually-curated longitude / latitude values.

> [!IMPORTANT]
> The longitude / latitude file is presently generated through a third-party repository. This should be integrated here in the future.

The resulting database is a time-aware, PSGC-anchored education data warehouse that separates enrollment facts, school-year metadata, geographic identity, and address normalization—designed for accurate analytics, mapping, and policy use.

## Project layout

- `src/foundation/pipeline.py`: orchestrates extract → transform → load and returns the typed `ExtractedFrames`.
- `src/foundation/plugins/`: houses each extractor and matching helper (PSGC, enrollment metadata, geodata, address matching) so new sources can be added via plugins.
- `src/foundation/transforms/`: shared cleanup utilities (location fixes, school-name normalization, reorder helpers) isolated for reuse.
- `src/foundation/loaders/`: loader helpers (currently the enrollment table wiring) keep database writes separate from extraction concerns.

## Extraction rules

Detailed notes about the extraction behavior live in `docs/extraction_rules.md`, covering PSGC name normalization, enrollment melting/`num_students` cleanup, logging of invalid rows, and the dependent matching flow. Keep this file in sync whenever the pipeline adds or refactors a rule.

## Development

```sh
uv sync --all-extras # will create the foundation package, see pyproject.toml
source .venv/bin/activate # enter virtual environment
```

## Run

Rename env.example to `.env` (contains the name of the `DB_FILE`, set to `deped.db`).

```sh
cli # show the different commands
cli prep # deped.db created w/ some generic tables
cli build # populates deped.db from /data files
```

## Docs

```sh
zensical serve
```
