# Foundation

This project integrates data from these sources to generate a single sqlite file:

1. [Project Bukas](https://www.deped.gov.ph/machine-ready-files/) enrollment datasets;
2. [Philippine Standard Geographic Code (PSGC)](https://psa.gov.ph/classification/psgc) using the address fields from enrollment datasets;
3. Additional geospatial metadata from manually-curated longitude / latitude values.

> [!IMPORTANT]
> The longitude / latitude file is presently generated through a third-party repository. This should be integrated here in the future.

The resulting database is a time-aware, PSGC-anchored education data warehouse that separates enrollment facts, school-year metadata, geographic identity, and address normalization—designed for accurate analytics, mapping, and policy use.

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
