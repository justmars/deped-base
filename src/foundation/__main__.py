from pathlib import Path

import click
import polars as pl
import yaml
from rich.console import Console
from sqlite_utils import Database

from .common import add_to, bulk_update, env, prep_table
from .loaders.enrollment import set_enrollment_tables
from .pipeline import (
    ExtractedFrames,
    PipelineOutput,
    PluginPipeline,
    frames_from_pipeline_output,
)

console = Console()


@click.group()
def remake():
    """Generate the initial school database"""
    pass


@remake.command("prep")
def prep():
    """Assumes generics.yml file, to populate target db."""
    src = env.path("GENERIC_FILE")
    if not src.exists():
        raise FileNotFoundError(f"Generic file {src=} does not exist.")

    # database will be created (or remade, if already existing) here
    target = env.path("DB_FILE")
    db = Database(target, recreate=True, use_counts_table=True)
    db.enable_wal()

    with console.status(
        f"[bold magenta]Preparing database[/bold magenta] {target.name}\n",
        spinner="dots",
    ):
        console.log(
            f"[bold]Using generic file:[/bold] {src}; [red]rebuilding[/red] {target}"
        )
        generic = Path(src)
        text = generic.read_text()
        data = yaml.safe_load(text)

        prep_table(db=db, table_name="school_sizes", values=data["school_sizes"])
        prep_table(db=db, table_name="school_grades", values=data["school_grades"])
        prep_table(db=db, table_name="school_epochs", values=data["school_epochs"])
        console.log(f"[green]✓ Built reference tables from {generic.name}[/green]")
    db.close()


@remake.command("build")
def build():
    """Populates the target database file with contents from /data."""
    target = _resolve_db_target()
    geo = env.str("GEOS_TABLE")
    console.log(f"[blue]Populating[/blue]: {target}; [red]main table[/red] {geo}")

    db = _open_wal_database(target)
    try:
        pipeline = PluginPipeline()
        console.log(f"[blue]Discovered extractors:[/blue] {len(pipeline.plugins)}")
        order = ", ".join(plugin.name for plugin in pipeline.execution_order)
        console.log(f"[blue]Execution order:[/blue] {order}")
        output = pipeline.execute()
        data = frames_from_pipeline_output(output)
        db = _load_lookup_tables(
            db=db, enrollment_df=data.enrollment, levels_df=data.levels
        )
        db = _load_enrollment_tables(db=db, enrollment_df=data.enrollment)
        db = _load_geography_tables(db=db, data=data, geo_table=geo)
        region_names = pipeline.get_output_table(output, "region_names")
        if region_names is not None:
            db = add_to(db=db, df=region_names, table_name="region_names")
        teachers = pipeline.get_output_table(output, "teachers")
        if teachers is not None:
            db = _load_teacher_tables(db=db, teachers_df=teachers)
    finally:
        db.close()


def _resolve_db_target() -> Path:
    """Return the configured database file path, raising if missing.

    Returns:
        Path: Path to the DB file defined in the env.
    """

    target = env.path("DB_FILE")
    if not target.exists():
        raise FileNotFoundError(f"Target database file {target=} does not exist.")
    return target


def _open_wal_database(target: Path) -> Database:
    """Open the target SQLite database with WAL enabled.

    Args:
        target (Path): Path to the existing SQLite file.

    Returns:
        Database: `sqlite_utils.Database` opened with WAL mode.
    """

    db = Database(target, use_counts_table=True)
    db.enable_wal()
    return db


def _load_lookup_tables(
    db: Database, enrollment_df: pl.DataFrame, levels_df: pl.DataFrame
) -> Database:
    """Load the school year and level lookup tables.

    Args:
        db (Database): Open SQLite database connection.
        enrollment_df (pl.DataFrame): Full enrollment facts.
        levels_df (pl.DataFrame): Derived level metadata.

    Returns:
        Database: Updated database after inserting lookup data.
    """
    db = add_to(
        db=db,
        df=enrollment_df[["school_year"]].unique().drop_nulls(subset=["school_year"]),
        table_name="school_years",
    )
    db = add_to(db=db, df=levels_df, table_name="school_levels")

    bulk_update(
        db=db,
        tbl_name="school_levels",
        target_col="school_year",
        dependency_tbl="school_years",
        fk_col="school_year_id",
        source_col="school_year",
    )
    return db


def _load_enrollment_tables(db: Database, enrollment_df: pl.DataFrame) -> Database:
    """Insert enrollment facts and resolve year foreign keys.

    Args:
        db (Database): Open SQLite database connection.
        enrollment_df (pl.DataFrame): Enrollment fact frame.

    Returns:
        Database: Database after inserting enrollment data.
    """
    db = add_to(db=db, df=enrollment_df, table_name="enroll")

    bulk_update(
        db=db,
        tbl_name="enroll",
        target_col="school_year",
        dependency_tbl="school_years",
        fk_col="school_year_id",
        source_col="school_year",
    )

    return set_enrollment_tables(db=db, df=enrollment_df, src_table="enroll")


def _load_geography_tables(
    db: Database, data: ExtractedFrames, geo_table: str
) -> Database:
    """Persist geography, address, and PSGC tables.

    Args:
        db (Database): Open SQLite database connection.
        data (ExtractedFrames): Named frames from extraction.
        geo_table (str): Name of the `geos` table.

    Returns:
        Database: Database after geography tables are stored.
    """
    db = add_to(db=db, df=data.geo, table_name=geo_table)
    db = add_to(db=db, df=data.address, table_name="addr")
    db = add_to(db=db, df=data.psgc, table_name="psgc")

    _attach_psgc_foreign_keys(db=db, geo_table=geo_table)
    return db


def _load_teacher_tables(db: Database, teachers_df: pl.DataFrame) -> Database:
    """Insert teacher headcounts and derive helper tables."""

    if teachers_df.height == 0:
        return db

    db = add_to(db=db, df=teachers_df, table_name="teachers")
    bulk_update(
        db=db,
        tbl_name="teachers",
        target_col="school_year",
        dependency_tbl="school_years",
        fk_col="school_year_id",
        source_col="school_year",
    )

    db["teachers"].extract(  # type: ignore
        columns="position",
        table="teacher_positions",
        fk_column="teacher_position_id",
    )

    return db


def _attach_psgc_foreign_keys(db: Database, geo_table: str) -> None:
    """Add PSGC foreign key constraints for the geography table.

    Args:
        db (Database): Open connection.
        geo_table (str): Name of the geography fact table.
    """
    cols = ["psgc_region_id", "psgc_provhuc_id", "psgc_muni_id", "psgc_brgy_id"]
    for col in cols:
        db[geo_table].add_foreign_key(col, "psgc", "id")  # type: ignore


if __name__ == "__main__":
    remake()
