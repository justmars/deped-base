# Teacher Headcount Plugin

This extractor taps the HR workbook bundle to provide a `teachers` fact table that mirrors the per-school, per-year headcounts delivered to analysts.

## Source files

- Name each file using the school-year range (e.g., `2022-2023-teachers.xlsx`, `2023-2024-teachers.xlsx`). The plugin scans `HR_DIR` (default `data/hr`) for `*.xlsx` workbooks and consumes every matching file the moment `cli build` runs.
- Each workbook currently exposes three sheets. Early years call them `ES DB`, `JHS DB`, and `SHS DB`, while newer workbooks may simply be named `ES`, `JHS`, and `SHS`. The extractor hardcodes the layout for each year-range in `YEAR_SHEET_CONFIGS`, so introducing a new workbook requires adding a matching `SheetConfig`.
- Every sheet must expose the LIS/BEIS school identifiers (`Lis School ID` / `Beis School ID`) plus the level-specific headcount columns. The scraper renames the columns to lowercase, melts the tabular header rows into `position` labels, drops zeros, and finally casts the counts to nullable integers.

## Schema contract

- Table: `teachers`
- Primary key: `school_year`, `school_id`, `level`, `position`
- Columns:
  - `school_year` (text, derived from the filename)
  - `school_id` (normalized LIS/BEIS identifier)
  - `level` (`es`, `jhs`, `shs` depending on the worksheet)
  - `position` (column header describing the role)
  - `num` (nullable integer headcount)

The extractor emits a `polars.DataFrame` that is immediately validated against `SCHEMAS["teachers"]` before any writes happen. Downstream (`cli build`) inserts the rows into `sqlite_utils` and uses `bulk_update` to convert `school_year` into a foreign key (`school_year_id`), just like the enrollment table.

## Environment configuration

- `HR_DIR`: folder containing the HR workbooks. Defaults to `data/hr`. Document this variable in `.env`/`env.example` and in this doc.
- Because the module uses pandas to read Excel, make sure the runtime environment satisfies the pinned version (`pandas>=2.2.3`, `openpyxl`, `fastexcel`).

## Extending the extractor

1. Drop new workbooks into `HR_DIR` with the correct year range in their filename.
2. If the sheet layout changes (different sheet names, column ranges, headers), update `YEAR_SHEET_CONFIGS` inside `src/foundation/plugins/hr.py` with the new `SheetConfig`.
3. Add a regression test (see `tests/test_plugins_hr.py`) that writes a representative workbook to a temporary `HR_DIR` and runs `PluginPipeline.execute()` to confirm the `teachers` table is emitted.

### Ordering notes

The extractor now declares `depends_on = ["psgc"]`, so the pipeline ensures the official geography table is built before any HR tables are emitted. This keeps the documented ingestion order (PSGC → matching → address/geo → HR) intact and prevents the HR plugin from running before its upstream reference tables exist.

## Sample excerpt

```python
class TeachersExtractor(BaseExtractor):
    name = "teachers"
    outputs = ["teachers"]

    def extract(self, context, dependencies):
        files = sorted(context.paths.hr_dir.glob("*.xlsx"))
        frames = [ _read_teacher_file(file) for file in files ]
        df = pd.concat(frames, ignore_index=True)
        return ExtractionResult(tables={"teachers": pl.from_pandas(df)})

```

This keeps the extractor focused on transformation while the pipeline (1) validates the schema, (2) wires lookups, and (3) lets the SQLite loaders handle the actual write.
