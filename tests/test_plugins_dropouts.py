import polars as pl

from src.foundation.pipeline import PluginPipeline


class TestDropoutsPlugin:
    def test_dropouts_table_produces_rows(self, test_env):
        pipeline = PluginPipeline()
        output = pipeline.execute()

        assert "dropouts" in output.tables
        df: pl.DataFrame = output.tables["dropouts"]

        assert df.height > 0
        assert set(df.columns) >= {
            "school_year",
            "school_id",
            "grade",
            "sex",
            "num_dropouts",
            "source_file",
            "source_row",
            "ingested_at",
        }

        metrics = output.metrics
        assert metrics["dropouts_files"] == 3
        assert metrics["dropouts_invalid_rows"] >= 0
