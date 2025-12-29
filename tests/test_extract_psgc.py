import polars as pl
import pytest

from src.foundation.extract_psgc import set_psgc


class TestPSGCExtraction:
    def test_set_psgc(self, sample_psgc_xlsx):
        """Test PSGC data loading and cleaning."""
        df = set_psgc(sample_psgc_xlsx)

        # Check that it's a polars DataFrame
        assert isinstance(df, pl.DataFrame)

        # Check expected columns
        expected_columns = [
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
        assert list(df.columns) == expected_columns

        # Check that we have data
        assert df.height > 0
