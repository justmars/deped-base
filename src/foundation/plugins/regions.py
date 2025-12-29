from pathlib import Path

import polars as pl
import yaml

from ..plugin import BaseExtractor, ExtractionContext, ExtractionResult


def _load_region_aliases(regions_file: Path) -> list[dict[str, str]]:
    if not regions_file.exists():
        raise FileNotFoundError(f"Missing regions file: {regions_file}")
    payload = yaml.safe_load(regions_file.read_text())
    return payload.get("region_names", [])


class RegionNamesExtractor(BaseExtractor):
    """Load canonical region aliases to help match region labels."""

    name = "region_names"
    depends_on = ["psgc"]
    outputs = ["region_names"]
    schema_name = "region_names"

    def extract(
        self,
        context: ExtractionContext,
        dependencies: dict[str, pl.DataFrame],
    ) -> ExtractionResult:
        regions_file = context.paths.region_names_file
        entries = _load_region_aliases(regions_file)
        if not entries:
            raise ValueError("No region aliases found in regions.yml")
        df = pl.DataFrame(entries or [])
        # ensure columns exist before casting
        for col in ("psgc_region_id", "roman", "location", "common", "other"):
            if col not in df.columns:
                df = df.with_columns(pl.lit(None).alias(col))
            df = df.with_columns(pl.col(col).cast(pl.Utf8))
        return ExtractionResult(tables={"region_names": df})
