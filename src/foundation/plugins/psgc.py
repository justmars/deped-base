from pathlib import Path

import polars as pl

from ..plugin import BaseExtractor, ExtractionContext, ExtractionResult


def set_psgc(f: Path) -> pl.DataFrame:
    """Load and clean PSGC Excel data using Polars."""
    print(f"Initializing PSGC data from {f=}")

    # Read Excel file using Polars (uses calamine backend)
    df = pl.read_excel(
        source=f,
        sheet_name="PSGC",
    )

    # Select and rename columns (A:I,K = 0-8 and 10)
    columns = df.columns
    selected_cols = (
        list(range(9)) + [10] if len(columns) > 10 else list(range(len(columns)))
    )
    df = df.select([df.columns[i] for i in selected_cols])

    new_columns = [
        "id",
        "name",
        "cc",
        "geo",
        "old_names",
        "city_class",
        "income_class",
        "urban_rural",
        "2024_pop",
        "status",
    ]
    df = df.rename({old: new for old, new in zip(df.columns, new_columns)})

    # Format ID column as zero-padded 10-char string
    df = df.with_columns(
        pl.col("id").cast(pl.Int64).cast(pl.Utf8).str.pad_start(10, "0")
    )

    # Prefer old_names for provinces when present
    df = df.with_columns(
        pl.when((pl.col("geo") == "Prov") & (pl.col("old_names").is_not_null()))
        .then(pl.col("old_names"))
        .otherwise(pl.col("name"))
        .alias("name")
    )

    # Replace "-" with null (only on string columns)
    string_cols = [col for col in df.columns if df[col].dtype == pl.Utf8]
    df = df.with_columns(
        [
            pl.when(pl.col(col) == "-").then(None).otherwise(pl.col(col)).alias(col)
            for col in string_cols
        ]
    )

    # Clean income_class: fill nulls with "" and remove "*"
    df = df.with_columns(
        pl.col("income_class")
        .fill_null("")
        .cast(pl.Utf8)
        .str.replace_all(r"\*", "")
        .alias("income_class")
    )

    # Safe fill for city_class: fill nulls with ""
    df = df.with_columns(
        pl.col("city_class").fill_null("").cast(pl.Utf8).alias("city_class")
    )

    return df


class PsgcExtractor(BaseExtractor):
    """PSGC loader that conforms to the extractor interface."""

    name = "psgc"
    outputs = ["psgc"]
    schema_name = "psgc"

    def extract(
        self,
        context: ExtractionContext,
        dependencies: dict[str, pl.DataFrame],
    ) -> ExtractionResult:
        del dependencies
        df = set_psgc(f=context.paths.psgc_file)
        return ExtractionResult(tables={"psgc": df})
