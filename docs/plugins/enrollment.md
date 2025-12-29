# Enrollment Extractor

## Purpose

Reads the Project Bukas enrollment CSVs (one per school year), melts them into a long fact table, extracts metadata, and emits the constructed lookup tables (`school_year_meta`, `enrollment`, `school_levels`).

## Source

- Folder: configured via `ENROLL_DIR`
- Files: named `enrollment_<year_range>.csv`, covering `2017-2018` through `2024-2025`.
- Format: wide enrollment columns following `<grade>[_<strand>]_sex`.

## Transform highlights

- Files are melted through `melt_enrollment_csv`, which uses `sanitize_num_students` to clean comma/literal noise and drop zero/invalid entries.
- Metadata columns are cleaned via `clean_meta_location_names` and `clean_school_name`.
- Offer-level data (`offers_es`, etc.) is pivoted to `school_levels`.

## Output tables

- `school_year_meta`: metadata per school-year, later matched to PSGC.
- `enrollment`: fact table with columns (`school_year`, `school_id`, `grade`, `sex`, `strand`, `num_students`).
- `school_levels`: helper table marking which levels a school offered.

## Schema

See `SCHEMAS` definitions `ENROLLMENT_SCHEMA`, `SCHOOL_YEAR_META_SCHEMA`, and `SCHOOL_LEVEL_SCHEMA`.

## Related docs

- [`docs/enrolment_origin.md`](../enrolment_origin.md) explains the wide-format files and normalization/pivot logic required to keep counts ints.
