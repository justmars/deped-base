# Dated Barangay Names

```py
brgy = df[df["psgc_brgy_id"].isna()]

missing_brgy_counts = (
    brgy
    .groupby(["psgc_muni_id","barangay"])
    .size()
    .reset_index(name="count")
    .sort_values("count", ascending=False)
) # List of municipalities, barangays w/o psgc codes
```

The reason for the absence is that some barangays, as written, cannot be found in the psgc list, e.g. `MUZON` in municipality '0301420000' cannot be matched. If viewing the municipality from `psgc_df`:

```py
text = "muzon"
muni = "0301420000"
psgc_df[
  (psgc_df["geo"] == "Bgy") &
  (psgc_df["id"].str.startswith(muni[:7])) &
  ((psgc_df["name"].str.contains(text, case=False)) | (psgc_df["old_names"].str.contains(text, case=False)))
]
```

This reveals `Muzon Proper`, `Muzon East`, `Muzon South`, and `Muzon West`.
