# PSGC

## PSGC Data

1. Downloaded from the PSA website as Oct. 13, 2025
2. See PSGC file = 2025-10-13-psgc.xlsx

## Project Bukas Enrollment Data

1. Includes S.Y. `2017-2018` to `2024-2025`
2. See `/data/enroll` folder.
3. These are sourced from Project Bukas' [machine-ready files](https://www.deped.gov.ph/machine-ready-files/)

## YAML-based Location Fixes

1. `/data/fixes.yml` contains rule-based corrections to DepEd school metadata, it is loaded by `clean_meta_location_names()`
2. The latest (official) enrollment school year data is only from 2024-2025
3. The fix maps region, province, and municipality fields to

## Geocoordinates

1. Longitude and latitude data need to be sourced separately.
2. At present, this is done through a manually-generated .csv file consisting of the school id, longitude, latitude coordinates.

## Orchestration

```python
from src.base import unpack_enroll_data, set_coordinates, set_psgc
from src.match_psgc_schools import match_psgc_schools

# Process enrolment files from Project Bukas to create latest school metadata and historical enrollment dataframes
enrolment_folder = Path("data/enroll")
school_df, enroll_df = unpack_enroll_data(enrolment_folder=enrolment_folder)

# Load cleaned PSGC data from the Philippine Statistics Authority (PSA)
psgc_file = Path("data/2025-10-13-psgc.xlsx")
psgc_df = set_psgc(f=psgc_file)

# Match school_df (Project Bukas) with psgc_df (PSA)
school_geo_df = match_psgc_schools(psgc_df=psgc_df, school_location_df=school_df)

# Add longitude / latitude, detect outliers based on psgc code
geo_file = Path("data/2025-12-02-geo.csv")
df = set_coordinates(geo_file=geo_file, meta_df=school_geo_df)
```

## Notebook Evaluation

``` py
# List down all regions
df[df["geo"] == "Reg"]

# List down all provinces
df[df["geo"] == "Prov"]

# Detect provinces within a specific region
df[(df["geo"] == "Prov") & (df["id"].str.startswith("19"))]

# Determine no. of provinces lacking a PSGC municipality code
muni = df[df["psgc_muni_id"].isna()]
muni.groupby(["region", "province"]).size()

# Determine no. of barangays lacking a PSGC barangay code
brgy = df[df["psgc_brgy_id"].isna()]
brgy.groupby(["region", "province", "municipality"]).size()

# Detect provinces based on search string partials
df[df["province"].str.contains("Maguindanao", case=False, na=False)]

# Detect provincial outliers
x = df[df["province_outlier"]]
x.groupby(["region","province"]).size()
```
