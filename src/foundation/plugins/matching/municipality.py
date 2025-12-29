import re
from collections.abc import Iterable

import polars as pl

from ...common import console, normalize_geo_name


def _allowed_prefixes_from_provhuc(provhuc_value) -> set[str]:
    """
    Return allowed PSGC id prefixes derived from a provhuc value.

    Rules (PSGC ids are 10 digits):
      - If provhuc is missing/NaN -> return empty set.
      - Count trailing zeros in the 10-digit string:
          * trailing_zeros >= 5 -> only allow 5-digit prefix (province-level)
          * 3 <= trailing_zeros < 5 -> allow 5-digit and 7-digit prefixes
          * trailing_zeros < 3  -> allow 5-digit and 7-digit prefixes (full id present)
      - The returned prefixes are strings (5-digit and/or 7-digit).

    Examples:
      - "1374000000" -> trailing_zeros = 5 -> returns {"13740"}
      - "1374040000" -> trailing_zeros = 4 -> returns {"13740", "1374040"}
      - "1374040123" -> trailing_zeros = 0 -> returns {"13740", "1374040"}

    Args:
        provhuc_value: str | int | float | None

    Returns:
        set of prefix strings (e.g., {"13740", "1374040"})
    """
    prefixes: set[str] = set()

    # handle missing
    if provhuc_value is None or (
        isinstance(provhuc_value, float) and provhuc_value != provhuc_value  # NaN check
    ):
        return prefixes

    # normalize to 10-digit string (strip spaces)
    s = str(provhuc_value).strip()
    # if user passed a shorter canonical prefix (rare), pad left with zeros? better to left-justify:
    # but we expect PSGC-style numeric strings; try to extract digits only
    digits = re.sub(r"\D", "", s)
    if not digits:
        return prefixes

    # ensure at most 10 digits; if shorter, left-pad with zeros to 10 to preserve positions
    digits = digits.zfill(10)[-10:]

    # count trailing zeros
    m = re.search(r"(0+)$", digits)
    trailing_zeros = len(m.group(1)) if m else 0

    # first-five always useful if we have at least 5 digits
    if len(digits) >= 5:
        prefixes.add(digits[:5])

    # decide whether to include 7-digit prefix
    if trailing_zeros < 5:
        # include 7-digit prefix when there is municipal-level specificity
        if len(digits) >= 7:
            prefixes.add(digits[:7])

    return prefixes


def attach_psgc_muni_id(meta: pl.DataFrame, psgc: pl.DataFrame) -> pl.DataFrame:
    """
    Populate meta['psgc_muni_id'] by matching municipality names against PSGC City/Mun rows
    filtered by allowed prefixes derived from meta['psgc_provhuc_id'].

    Behavior:
      - For each school row, derive allowed prefixes from its psgc_provhuc_id:
          prefixes = { first5, first7 } (when available)
      - Candidate PSGC municipality rows = psgc rows with geo in {"City", "Mun"}
        and id starting with any of the allowed prefixes.
        If no prefixes (psgc_provhuc_id missing) then candidates = all City/Mun rows.
      - Normalize candidate PSGC names with normalize_geo_name() and match exactly
        against normalized(meta["municipality"]).
      - If a match is found, set meta["psgc_muni_id"] to the matched PSGC row's full id.
      - If multiple candidates match the same normalized name, the first exact match is used.
      - No aliasing, no SubMun logic, no HUC→prov conversions here — only prefix-filtered City/Mun names.

    Args:
        meta: DataFrame containing at least columns:
              - "municipality"
              - "psgc_provhuc_id" (may be NaN)
        psgc: PSGC master DataFrame containing at least:
              - "id"   (10-digit PSGC string/number)
              - "name"
              - "geo"  (City/Mun/SubMun/Prov/etc.)

    Returns:
        A copy of meta with an added column "psgc_muni_id" (full PSGC id or None) and columns reordered
        so that 'municipality' and 'psgc_muni_id' are adjacent.
    """
    if "psgc_provhuc_id" not in meta.columns:
        raise Exception("Missing dependency.")
    console.log("[cyan]Attaching PSGC municipality codes...[/cyan]")

    # Normalize meta municipality for matching
    meta = meta.with_columns(
        normalized_municipality=pl.col("municipality").map_elements(
            normalize_geo_name, return_dtype=pl.Utf8
        )
    )

    # Prepare PSGC submun/city/mun candidate table and normalized names
    citymun_df = psgc.filter(
        pl.col("geo").is_in(["SubMun", "City", "Mun"])
    ).with_columns(
        psgc_id_str=pl.col("id").cast(pl.Utf8),
        normalized_name=pl.col("name").map_elements(
            normalize_geo_name, return_dtype=pl.Utf8
        ),
    )

    # Build a mapping: normalized_name -> list of full ids (to handle duplicates safely)
    candidates_by_name: dict[str, list[str]] = {}
    for row in citymun_df.iter_rows(named=True):
        nm = row["normalized_name"]
        if nm not in candidates_by_name:
            candidates_by_name[nm] = []
        candidates_by_name[nm].append(row["psgc_id_str"])

    # For faster prefix filtering, build an index of id -> normalized_name
    id_to_name = dict(
        zip(
            citymun_df["psgc_id_str"].to_list(), citymun_df["normalized_name"].to_list()
        )
    )

    # Helper to gather candidate ids for a given set of prefixes
    def _candidate_ids_for_prefixes(prefixes: Iterable[str]) -> set[str]:
        if not prefixes:
            # no prefix restriction -> all city/mun ids
            return set(id_to_name.keys())
        out = set()
        for pid in id_to_name.keys():
            for pref in prefixes:
                if pid.startswith(pref):
                    out.add(pid)
                    break
        return out

    # Cache prefix->candidate_ids to avoid repeated scanning
    prefix_cache: dict[tuple[str, ...], set[str]] = {}

    # Build list of muni_ids by row
    def get_muni_id(provhuc_val, norm_muni):
        prefixes = _allowed_prefixes_from_provhuc(provhuc_val)
        key = tuple(sorted(prefixes))
        if key in prefix_cache:
            candidate_ids = prefix_cache[key]
        else:
            candidate_ids = _candidate_ids_for_prefixes(prefixes)
            prefix_cache[key] = candidate_ids

        # Quick path: if normalized name not present among any city/mun candidates, None
        found_id = None
        if not prefixes:
            ids_for_name = candidates_by_name.get(norm_muni, [])
            found_id = ids_for_name[0] if ids_for_name else None
        else:
            for cid in candidate_ids:
                if id_to_name.get(cid) == norm_muni:
                    found_id = cid
                    break
        return found_id

    # Apply to all rows
    meta = meta.with_columns(
        psgc_muni_id=pl.struct(
            ["psgc_provhuc_id", "normalized_municipality"]
        ).map_elements(
            lambda x: get_muni_id(x["psgc_provhuc_id"], x["normalized_municipality"]),
            return_dtype=pl.Utf8,
        )
    )

    return meta
