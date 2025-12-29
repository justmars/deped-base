import polars as pl

from src.foundation.pipeline import PluginPipeline


class TestHrPlugin:
    def test_teachers_table(self, test_env):
        pipeline = PluginPipeline()
        output = pipeline.execute()

        assert "teachers" in output.tables
        df: pl.DataFrame = output.tables["teachers"]

        assert df.height > 0
        assert set(df["school_year"].to_list()) == {"2022-2023"}
        assert set(df["level"].to_list()).issuperset({"es", "jhs", "shs"})
        assert "position" in df.columns
        assert "num" in df.columns
