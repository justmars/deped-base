# Extractor Plugin Guide

This project builds the shared SQLite dataset through two CLI commands:

1. `cli prep` seeds the database (`DB_FILE`) with reference tables such as `school_sizes`, `school_grades`, and `school_epochs`.
2. `cli build` runs the new `PluginPipeline`, which discovers every `BaseExtractor`, enforces `SCHEMAS`, and persists the legacy tables (`enroll`, `geos`, `addr`, `psgc`, etc.).

Each extractor now participates in that pipeline as a discrete plugin: it declares its inputs via `depends_on`, lists its outputs, uses shared transforms, and returns a Polars DataFrame map. The pipeline validates each table immediately so any contract mismatch is caught before `sqlite_utils` writes the data.

## Initial ingestion pipeline

| Step | Plugin | Description | Document |
| --- | --- | --- | --- |
| 1 | `PsgcExtractor` | Loads official PSGC Excel, normalizes IDs/names, and provides the master geography table. | [PSGC extractor](/docs/plugins/psgc.md) |
| 2 | `RegionNamesExtractor` | Loads the curated region alias table so every PSGC region ID has roman/location/common aliases available. | [Region names extractor](/docs/plugins/regions.md) |
| 3 | `EnrollmentExtractor` | Melts yearly enrollment CSVs, sanitizes counts, and emits metadata + fact tables. | [Enrollment extractor](/docs/plugins/enrollment.md) |
| 4 | `PsgcMatchingExtractor` | Matches school metadata to PSGC codes (regions → provinces → barangays). | [Matching extractor](/docs/plugins/matching.md) |
| 5 | `AddressDimensionExtractor` | Builds canonical `_addr_hash` + address bridge table. | [Address extractor](/docs/plugins/address.md) |
| 6 | `GeoExtractor` | Joins coordinates plus address IDs to enrich the geography fact table. | [Geodata extractor](/docs/plugins/geodata.md) |

Any new extractor that follows this contract plugs into `cli build` automatically; no further orchestration edits are required. The pipeline log will print `[green]Validated schema[/green] <table>` once every table passes validation.

## Adding more datasets

1. Add a schema entry for each new logical table in `src/foundation/schema.py` so contracts remain centralized.
2. Create a plugin module under `src/foundation/plugins/` with a `BaseExtractor` subclass (`name`, `outputs`, `depends_on`), reusing shared transforms when possible.
3. Have the extractor run only its extract-transform logic and return `ExtractionResult(tables={...})`. Avoid writing to SQLite directly; let `cli build` consume the tables post-validation.
4. Extend regression coverage by adding targeted tests (`tests/test_plugins_<name>.py`) and rerun the suite (`python -m pytest tests/test_extract_dataframes.py tests/test_cli.py`).

### Example: Teacher headcount plugin

The existing HR helpers already demonstrate how to normalize companion datasets per school year:

```python
from pathlib import Path

import pandas as pd
from sqlite_utils import Database

from hr.helpers import load_teacher_file
from src.foundation.common import env
from src.foundation.plugin import BaseExtractor, ExtractionContext, ExtractionResult


class TeachersExtractor(BaseExtractor):
    name = "teachers"
    outputs = ["teachers"]
    depends_on = ["school_years"]

    def extract(
        self,
        context: ExtractionContext,
        dependencies: dict[str, object],
    ) -> ExtractionResult:
        teacher_dir = Path(env.path("HR_DIR"))
        db = Database(env.path("DB_FILE"))

        frames = [
            load_teacher_file(f=file, db=db)
            for file in sorted(teacher_dir.glob("*.xlsx"))
        ]
        combined = pd.concat(frames, ignore_index=True)
        return ExtractionResult(tables={"teachers": combined})
```

Supply the schema in `SCHEMAS`. See concrete steps on the schema definition that matches the teacher extractor and how the pipeline validates it:

When you add teacher/headcount data via a plugin, the pipeline validates every table against `SCHEMAS` before it writes to SQLite. This ensures the extractor cannot introduce schema drift or accidentally omit required columns.

## What to do

1. **Declare the intended table name and columns** in `src/foundation/schema.py`. Example entry:

   ```python
   TEACHERS_SCHEMA = TableSchema(
       name="teachers",
       primary_key=["school_year_id", "school_id", "level", "position"],
       columns=[
           ColumnDef("school_year_id", pl.Int64, nullable=False),
           ColumnDef("school_id", pl.Utf8, nullable=False),
           ColumnDef("level", pl.Utf8, nullable=False),
           ColumnDef("position", pl.Utf8, nullable=False),
           ColumnDef("num", pl.Int64),
       ],
   )
   ```

   Then include it in the `SCHEMAS` dict (e.g., `SCHEMAS["teachers"] = TEACHERS_SCHEMA`).

2. **Emit the matching columns** from your plugin (`ExtractionResult.tables`). Missing columns or unexpected `null`s will raise `PipelineExecutionError` with a short sample, so match the `TableSchema` exactly.

3. **Normalize `school_year_id`** using the existing `school_years` lookup instead of inventing new references. This keeps the teacher facts aligned with the rest of the warehouse.

4. **Add documentation links** (e.g., update `docs/index.md` or `docs/plugins.md`) so future contributors know the schema expectations for teacher data.

Once the schema is defined, your `TeachersExtractor` is auto-discovered by `PluginRegistry`. No module registration is necessary—the `SCHEMAS` entry plus the `ExtractionResult` columns are all the pipeline needs to keep contracts safe.

## Source data files

- `/data/generic.yml` seeds the generic lookup tables (`school_sizes`, `school_levels`, etc.) via `cli prep`.
- `/data/regions.yml` now holds the canonical region alias catalog that `RegionNamesExtractor` loads after the PSGC table exists.
- Keep these files in the repository so the pipeline and tests can reuse them, and update the YAML whenever new aliases or reference values are needed.

## Related reference documents

- [`docs/enrolment_origin.md`](./enrolment_origin.md) explains the wide-format enrollment CSVs that feed the enrollment extractor.
- [`docs/brgy_names.md`](./brgy_names.md) shows how missing barangay matches surface during PSGC matching and how the metadata can be interrogated for corrections.
