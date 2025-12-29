# 📦 SQLite Database Export – Schema & Table Descriptions

This document describes the structure and purpose of the SQLite database produced by the data extraction and build pipeline.

## Overview

The database separates data into:

- **Fact tables** – measurable quantities (e.g., enrollment counts)
- **Dimension tables** – descriptive attributes (e.g., grades, strands, addresses)
- **Reference tables** – authoritative code lists (e.g., PSGC, school years)

This structure ensures:

- Historical correctness
- Efficient querying
- Referential integrity

## 1. `school_years`

### Purpose

Canonical reference table for school years.

### Source

Derived from distinct values in the enrollment dataset.

### Key Columns

- `id` (PRIMARY KEY)
- `school_year` (e.g., `"2023-2024"`)

### Notes

- Used as a foreign key by other tables.
- Normalizes time across the database.

## 2. `school_levels`

### Purpose

Tracks which education levels (ES, JHS, SHS) a school offered in a given school year.

### Source

Derived from enrollment metadata (`levels_df`).

### Key Columns

- `school_id`
- `school_year_id` (FK → `school_years.id`)
- `offers_es`
- `offers_jhs`
- `offers_shs`

### Notes

- Allows analysis of program availability over time.
- Decoupled from enrollment counts.

## 3. `enroll` (Core Fact Table)

### Purpose

Stores student enrollment counts.

### Source

Processed enrollment files.

### Key Columns

- `school_id`
- `school_year_id` (FK → `school_years.id`)
- `grade_id` (FK → `school_grades.id`)
- `strand_id` (FK → `school_strands.id`, nullable)
- `sex`
- `num_students`

### Notes

- Primary table for analytics and reporting.
- All participation and trend metrics are derived here.
- `num_students` is sanitized by removing formatting (commas/whitespace) and casting to `Int64`, so invalid or empty counts become `NULL` before load.

## 4. Enrollment Dimension Tables

### 4.1 `school_grades`

#### Purpose

Reference table for grade levels.

#### Key Columns

- `id`
- `label`
- `rank`
- `ks`

### 4.2 `school_strands`

#### Purpose

Reference table for senior high school strands.

#### Key Columns

- `id`
- `strand`

## 5. `geos` (Base / School-Year Geography Table)

### Purpose

Represents the geographic and administrative identity of a school **per school year**.

### Source

Enrollment metadata enriched with PSGC matching and coordinates.

### Key Columns

- `school_id`
- `school_year`
- `school_name`
- `region`
- `province`
- `municipality`
- `barangay`
- `psgc_region_id`
- `psgc_provhuc_id`
- `psgc_muni_id`
- `psgc_brgy_id`
- `longitude`
- `latitude`
- `_addr_hash`

### Notes

- Time-aware: schools may move, close, or reopen.
- Used for mapping, spatial analysis, and regional aggregation.

## 6. `addr` (Address Bridge Table)

### Purpose

Links each school and school year to a canonical address identity.

### Source

Derived from PSGC-resolved metadata.

### Key Columns

- `school_id`
- `school_year`
- `_addr_hash`
- `address_id`

### Notes

- Implements address normalization.
- Prevents duplication of address data.
- Supports many-to-one and one-to-many address relationships over time.

## 7. `psgc` (Official Geographic Reference)

### Purpose

Authoritative list of Philippine administrative units from the PSA.

### Source

PSA PSGC dataset.

### Key Columns

- `id` (PSGC code)
- `name`
- `geo` (Region / Province / City / Barangay)
- Classification attributes (e.g., urban/rural, income class)

### Notes

- Single source of truth for geographic codes.
- Referenced by the `geo` table via foreign keys.

## Foreign Key Relationships (Logical)

```text
school_years
   ↑
   ├── school_levels.school_year_id
   └── enroll.school_year_id

school_grades
   ↑
   └── enroll.grade_id

school_strands
   ↑
   └── enroll.strand_id

psgc
   ↑
   └── geo.psgc_*_id
