import polars as pl

from src.foundation.pipeline import PluginPipeline


class TestRegionNamesPlugin:
    def test_region_names_available(self, test_env):
        pipeline = PluginPipeline()
        output = pipeline.execute()

        assert "region_names" in output.tables
        df: pl.DataFrame = output.tables["region_names"]

        assert df.height > 0
        assert set(df["psgc_region_id"].to_list()) >= {"01", "13"}
        assert "location" in df.columns
        assert "common" in df.columns
