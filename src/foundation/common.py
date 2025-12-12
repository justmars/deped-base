import re
import sqlite3
import time
import unicodedata
from pathlib import Path
from typing import Any

import pandas as pd
import yaml
from environs import Env
from rich import print as rprint
from rich.console import Console
from rich.progress import Progress
from rich.syntax import Syntax
from sqlite_utils import Database

env = Env()
env.read_env()


def prep_table(db: Database, table_name: str, values: list[dict]):
    rprint(f"Adding [green]{table_name=}[/green]...")
    db[table_name].drop(ignore=True)
    db[table_name].insert_all(values, pk="id")  # type: ignore


def prettify_sql(file: Path, display: bool = False) -> str:
    console = Console()
    if not file.parent == Path("sql"):
        raise FileNotFoundError("File needs to be under /data/sql")
    sql = file.read_text()
    if display:
        syntax = Syntax(sql, "sql")
        console.print(syntax)
    return sql


def run_sql_file(conn: Any, file: Path, prefix_expr: str | None = None):
    """Run the contents of the `*.sql` file using the `prefix_expr` as the first
    part of the sql script.

    Args:
        conn (Any): The database connection
        file (Path): The *.sql file
        prefix_expr (str | None, optional): Usually `create view...`. Defaults to None.

    Raises:
        Exception: No connection found.
    """
    if not isinstance(conn, sqlite3.Connection):
        raise Exception("Could not get connection.")
    cur = conn.cursor()
    sql = file.read_text()
    if prefix_expr:
        sql = "\n".join((prefix_expr, sql))
    cur.execute(sql)
    conn.commit()


def add_to(db: Database, df: pd.DataFrame, table_name: str) -> Database:
    """Add a `table_name` to the target database `db` sourced from the given dataframe `df`. Presumes
    that the dataframe is already ready for insertion."""
    tbl = db[table_name]
    rows = df.to_dict(orient="records")
    rprint(f"Insert {table_name=} values from [green]{len(rows)=}[/green]")
    tbl.insert_all(rows, pk="id", replace=True)  # type: ignore
    return db


# --------------------------------------------------------
# Load YAML only once (cached)
# --------------------------------------------------------
def load_fixes() -> dict:
    yaml_path = Path(__file__).parent.parent.parent / "data" / "fixes.yml"
    with open(yaml_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


FIXES = load_fixes()
PSGC_REGION_MAP = FIXES["region_psgc_map"]


def normalize_region_name(name: str) -> str:
    if not isinstance(name, str):
        return ""
    name = name.lower().strip()

    # remove content inside parentheses
    name = re.sub(r"\(.*?\)", "", name)

    # remove punctuation and multiple spaces
    name = re.sub(r"[^a-z0-9 ]+", " ", name)
    name = re.sub(r"\s+", " ", name)

    return name.strip()


def convert_trailing_roman(text: str) -> str:
    """
    Convert a trailing Roman numeral (I–X) at the end of a string
    to its Arabic equivalent.
    """
    if not isinstance(text, str):
        return text

    # Roman → Arabic mapping (1–10)
    roman_map = {
        "I": "1",
        "II": "2",
        "III": "3",
        "IV": "4",
        "V": "5",
        "VI": "6",
        "VII": "7",
        "VIII": "8",
        "IX": "9",
        "X": "10",
    }

    # Regex: capture ending Roman numeral with optional punctuation/space
    match = re.search(r"\b(I|II|III|IV|V|VI|VII|VIII|IX|X)\b\.?$", text, re.IGNORECASE)
    if not match:
        return text

    roman = match.group(1).upper()
    arabic = roman_map[roman]

    # Replace only the ending numeral
    return re.sub(r"\b" + match.group(1) + r"\b\.?$", arabic, text, flags=re.IGNORECASE)


def normalize_geo_name(name: str) -> str:
    """
    Normalize a geographic name for consistent matching.
    """
    if not isinstance(name, str) or not name.strip():
        return ""

    name = name.lower().strip()
    name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("utf-8")

    # Remove parenthesis text
    name = re.sub(r"\(.*?\)", "", name).strip()

    # Remove punctuation (periods, commas)
    name = re.sub(r"[\.,]", "", name)

    abbrev_map = {
        r"\bsanto\b": "santo",
        r"\bsto\b": "santo",
        r"\bsta\b": "santa",
    }
    for pattern, replacement in abbrev_map.items():
        name = re.sub(pattern, replacement, name)

    if name.startswith("city of "):
        name = name[len("city of ") :].strip() + " city"

    return re.sub(r"\s+", " ", name).strip()


def bulk_update(
    db: Database,
    tbl_name: str,
    target_col: str,
    dependency_tbl: str,
    fk_col: str,
    source_col: str = "label",
):
    """Replace text-based column values with corresponding foreign key IDs.

    This function transforms a text-based categorical column into a foreign-key column
    by matching each value with its corresponding `id` from a lookup (dependency) table.
    It then renames the column and enforces a foreign key constraint for data integrity.
    A timer is displayed to track execution duration.

    Args:
        db (Database):
            A `sqlite_utils.Database` connection to the target SQLite database.
        tbl_name (str):
            The name of the main table to be updated (e.g., `"bld_crla"`).
        target_col (str):
            The column in the main table that currently holds text labels to be converted.
        dependency_tbl (str):
            The name of the lookup table containing `id`–`label` mappings (e.g., `"scores_crla"`).
        fk_col (str):
            The new column name to apply after conversion, indicating its foreign key nature.
        source_col (str, optional):
            The name of the label column in the lookup table used for matching.
            Defaults to `"label"`.

    Behavior:
        1. Validates that both the main and dependency tables exist and contain the necessary columns.
        2. Updates the target column by replacing text labels with corresponding foreign key IDs.
        3. Renames the column to reflect its foreign key role.
        4. Adds a foreign key constraint for referential integrity.

    Notes:
        - Only non-null values are updated.
        - Label matching is case-sensitive unless normalized beforehand.
        - Works best when lookup tables use unique labels.

    Example:
        ```python
        bulk_update(
            db=mydb,
            tbl_name="bld_crla",
            target_col="reading_level",
            dependency_tbl="scores_crla",
            fk_col="reading_level_crla_id"
        )
        ```

    Raises:
        Exception: If the target or dependency table does not exist,
                   or if the target column is missing.

    """
    console = Console()
    start_time = time.perf_counter()

    rprint(
        f"[bold cyan]Updating[/bold cyan] [yellow]{tbl_name}[/yellow].{target_col} "
        f"→ [green]{fk_col}[/green] using [magenta]{dependency_tbl}.{source_col}[/magenta]"
    )

    # --- Validation ---
    if not db[dependency_tbl].exists():
        raise Exception(f"Missing dependency table: {dependency_tbl}")
    if target_col not in db[tbl_name].columns_dict.keys():
        raise Exception(f"Column not found: {target_col} in {tbl_name}")

    with Progress() as progress:
        task = progress.add_task(f"Processing {tbl_name}", total=None)

        # Step 1: Update text values to lookup IDs
        db.executescript(f"""--sql
            UPDATE {tbl_name}
            SET {target_col} = (
                SELECT id
                FROM {dependency_tbl}
                WHERE {dependency_tbl}.{source_col} = {tbl_name}.{target_col}
            )
            WHERE {target_col} IS NOT NULL;
        """)

        # Step 2: Rename column
        db[tbl_name].transform(rename={target_col: fk_col})  # type: ignore

        # Step 3: Add FK constraint
        db[tbl_name].add_foreign_key(fk_col, dependency_tbl, "id")  # type: ignore

        progress.update(task, completed=1)
        progress.stop()

    elapsed = time.perf_counter() - start_time
    console.print(f"[green]✓ Completed in {elapsed:.2f} seconds[/green]")
