import polars as pl
from sqlite_utils import Database

from foundation.common import add_to, bulk_update


def set_school_strand(db: Database, df: pl.DataFrame, src_table: str):
    db = add_to(
        db=db,
        df=df[["strand"]].unique().drop_nulls(subset=["strand"]),
        table_name="school_strands",
    )
    bulk_update(
        db=db,
        tbl_name=src_table,
        target_col="strand",
        dependency_tbl="school_strands",
        fk_col="strand_id",
        source_col="strand",
    )
    return db


def set_enrollment_tables(db: Database, df: pl.DataFrame, src_table: str):
    if not db[src_table].exists():
        raise Exception(f"Dependency table {src_table=} missing ")

    if db["school_grades"].exists():
        bulk_update(
            db=db,
            tbl_name=src_table,
            target_col="grade",
            dependency_tbl="school_grades",
            fk_col="grade_id",
            source_col="label",
        )

    db = set_school_strand(db=db, df=df, src_table=src_table)

    return db
