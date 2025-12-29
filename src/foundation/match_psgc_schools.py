import polars as pl

from .apply_fixes import fill_missing_psgc
from .extract_brgy import apply_barangay_corrections, attach_psgc_brgy_id
from .extract_muni import attach_psgc_muni_id
from .extract_province import attach_psgc_provhuc_codes
from .extract_region import attach_psgc_region_codes
from .normalize import get_divisions
from .reorder_columns import reorganize_school_geo_df


def match_psgc_schools(
    psgc_df: pl.DataFrame, school_location_df: pl.DataFrame
) -> pl.DataFrame:
    """
    Attach complete PSGC geographic codes (region, province/HUC, municipality,
    barangay) to a school metadata DataFrame.

    This function assumes that the caller has already prepared the school
    metadata DataFrame. It performs only the geographic matching steps
    using the official PSGC dataset.

    Workflow:
        1. Load and normalize PSGC reference data.
        2. Attach PSGC region codes to each school.
        3. Attach PSGC province/HUC/SubMunicipality codes.
        4. Attach PSGC municipality/city codes.
        5. Attach PSGC barangay codes.
        6. Return a unified geocoded DataFrame.

    The function applies all relevant PSGC logic:
        * Region aliasing + normalization
        * Province/HUC precedence rules
        * SubMunicipality (SubMun) matching
        * NCR-specific logic (HUC-as-province)
        * Special-case overrides (e.g., SGA, City of Isabela)
        * BARMM region corrections
        * Maguindanao del Norte / del Sur split
        * Municipality and barangay normalization rules

    Args:
        psgc_df (pl.DataFrame):
            A DataFrame based on the PSGC Excel source file (e.g. "data/2025-10-13-psgc.xlsx").
            The file must contain the PSGC master sheet with columns such as:
            `id`, `name`, `geo`, `city_class`, `income_class`, etc.

        school_location_df (pl.DataFrame):
            A DataFrame containing cleaned school location metadata.
            Expected fields include (at minimum):
                - region
                - province
                - municipality
                - barangay
                - school_id
                - school_name
            Any additional metadata columns are preserved.

    Returns:
        pl.DataFrame:
            A DataFrame identical to `school_location_df` but enriched with:

                * psgc_region_id
                * psgc_provhuc_id
                * psgc_muni_id
                * psgc_brgy_id

            These fields represent the official PSGC geographic codes at all
            levels of hierarchy (region → province/HUC → municipality → barangay).
    """
    # PSGC region matching
    reg_df = attach_psgc_region_codes(meta=school_location_df, psgc=psgc_df)

    # PSGC province / HUC / SubMun matching
    prov_df = attach_psgc_provhuc_codes(meta=reg_df, psgc=psgc_df)

    # PSGC municipality matching
    muni_df = attach_psgc_muni_id(meta=prov_df, psgc=psgc_df)

    # PSGC barangay matching
    brgy_df = attach_psgc_brgy_id(meta=muni_df, psgc=psgc_df)

    # Manual corrections
    manually_corrected_df = apply_barangay_corrections(meta=brgy_df, psgc=psgc_df)

    df = fill_missing_psgc(meta_df=manually_corrected_df, psgc_df=psgc_df)

    df = df.with_columns(
        province=pl.col("province").map_elements(
            lambda x: x.title() if x else x, return_dtype=pl.Utf8
        )
    )

    division_lookup = get_divisions(df)

    df = df.join(
        division_lookup.select(["psgc_region_id", "division", "division_id"]),
        on=["psgc_region_id", "division"],
        how="left",
    )

    # Reordered
    reordered_df = reorganize_school_geo_df(df=df)

    return reordered_df
