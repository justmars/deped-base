# Origin

- Each enrollment CSV file represents one school year of submitted enrollment data.
- Although the exact number of columns may vary per year, all files follow the same general structure.

## School-Year Structure of Each CSV File

Each CSV contains one row per school, with school-level identifiers such as:

1. school_id
2. school_name
3. region_code / division_code (if present)
4. year
5. location descriptors

Following these identifiers, the file contains a large number of wide-format enrollment columns.

## Wide Format Enrollment Columns

Enrollment counts are encoded in column names using the pattern:

```sh
Grade[_Strand]_Sex
```

Example | Meaning
-- | --
`g1_male` | Grade 1, Male
`g11_gas_female` | Grade 11, GAS strand, Female
`g12_tvl_male` | Grade 12, TVL strand, Male

This format is consistent across historical school years from 2017 to 2024.

## Special Case for SY 2025–2026: New Strands with Underscores

Starting in School Year 2025–2026, DepEd introduced new Senior High School strands:

1. `sshs_acad`
2. `sshs_techpro`

Unlike previous strands, these contain underscores.
This introduces a parsing challenge, because the enrollment pipeline previously assumed that Grade, Strand, and Sex could be split reliably using `_`.

If split naïvely on _, these become 4 tokens: `["G12", "sshs", "acad", "Male"]`

This breaks earlier parsing logic, which expected only 2 parts (Grade_Sex) or 3 parts (Grade_Strand_Sex).

## Count Normalization

The wide enrollment columns occasionally include formatting such as comma separators, stray whitespace, or even empty/invalid strings. The pipeline now sanitizes each `num_students` value by removing commas/trim spaces and casting digit-only strings to integers before filtering (`normalize_num_students`), so every stored count is a clean `Int64` and malformed cells become `NULL`. This guarantees numeric comparisons (e.g., dropping zeros) never see string types, keeping the fact table deterministic.

Invalid rows are logged per file with their count and a small sample, so auditors can review rejected entries without rerunning the transformation manually.
