import pandas as pd
from rich import print as rprint

from .common import env
from .extract_geodata import set_coordinates
from .extract_meta import unpack_enroll_data
from .extract_psgc import set_psgc
from .match_psgc_schools import match_psgc_schools


def extract_dataframes() -> tuple[
    pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame
]:
    enroll_dir = env.path("ENROLL_DIR")
    rprint(f"Detected: {enroll_dir=}")

    psgc_file = env.path("PSGC_FILE")
    rprint(f"Detected: {psgc_file=}")

    geo_file = env.path("GEO_FILE")
    rprint(f"Detected: {geo_file=}")

    # Load cleaned PSGC data from the Philippine Statistics Authority
    psgc_df = set_psgc(f=env.path("PSGC_FILE"))

    # Process enrolment files from Project Bukas
    school_df, enroll_df, levels_df = unpack_enroll_data(enrolment_folder=enroll_dir)

    # Match school meta df from enrolment files with psgc df from the PSA
    meta_df = match_psgc_schools(psgc_df=psgc_df, school_location_df=school_df)

    # Clean annex status field
    meta_df["annex_status"] = meta_df["annex_status"].str.strip().str.lower()

    # Add longitude / latitude coordinates, determine outliers
    geo_df = set_coordinates(geo_file=geo_file, meta_df=meta_df)

    return (psgc_df, enroll_df, geo_df, levels_df)
