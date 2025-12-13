from pathlib import Path

import click
import yaml
from rich import print as rprint
from sqlite_utils import Database

from .common import add_to, bulk_update, env, prep_table
from .extract_dataframes import extract_dataframes
from .extract_enrollment import set_enrollment_tables


@click.group()
def remake():
    """Generate the initial school database"""
    pass


@remake.command("prep")
def prep():
    """Assumes generics.yml file, to populate target db."""
    src = env.path("GENERIC_FILE")
    target = env.path("DB_FILE")
    rprint(f"Detected: {src=}; (re)-building {target=}")

    db = Database(target, recreate=True, use_counts_table=True)
    db.enable_wal()

    rprint(f"Processing {src=} for target db.")
    generic = Path(src)
    text = generic.read_text()
    data = yaml.safe_load(text)

    prep_table(db=db, table_name="school_sizes", values=data["school_sizes"])
    prep_table(db=db, table_name="school_grades", values=data["school_grades"])
    prep_table(db=db, table_name="school_epochs", values=data["school_epochs"])

    db.close()


@remake.command("build")
def build():
    """Populates the target database file with contents from /data."""
    # extract data sources
    psgc_df, enroll_df, geo_df, levels_df = extract_dataframes()

    # add main tables
    target = env.path("DB_FILE")
    skl = env.str("MAIN_TABLE")
    db = Database(target, use_counts_table=True)
    db.enable_wal()

    # the school years table will be created based on the enroll dataframe
    db = add_to(
        db=db,
        df=enroll_df[["school_year"]].drop_duplicates().dropna(subset=["school_year"]),
        table_name="school_years",
    )
    db = add_to(db=db, df=levels_df, table_name="school_levels")

    # the levels table contains school year, this can be replaced
    bulk_update(
        db=db,
        tbl_name="school_levels",
        target_col="school_year",
        dependency_tbl="school_years",
        fk_col="school_year_id",
        source_col="school_year",
    )

    db = add_to(db=db, df=enroll_df, table_name="enroll")

    # the enroll table just added contains school year, this can also be replaced
    bulk_update(
        db=db,
        tbl_name="enroll",
        target_col="school_year",
        dependency_tbl="school_years",
        fk_col="school_year_id",
        source_col="school_year",
    )

    # create foreign key references for strand and grades
    db = set_enrollment_tables(db=db, df=enroll_df, src_table="enroll")

    # add the geo dataframe as the base table
    db = add_to(db=db, df=geo_df.rename(columns={"school_id": "id"}), table_name=skl)

    # connect the enroll table to the base table
    db["enroll"].add_foreign_key(  # type: ignore
        column="school_id",
        other_table=skl,
        other_column="id",
    )

    # create the psgc table
    db = add_to(db=db, df=psgc_df, table_name="psgc")

    # add foreign keys from the base table to psgc
    cols = ["psgc_region_id", "psgc_provhuc_id", "psgc_muni_id", "psgc_brgy_id"]
    for col in cols:
        db[skl].add_foreign_key(col, "psgc", "id")  # type: ignore

    db.close()


if __name__ == "__main__":
    remake()
