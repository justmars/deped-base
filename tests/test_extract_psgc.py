import pandas as pd
import pytest

from src.foundation.extract_psgc import convert_to_int, format_id_column, set_psgc


class TestPSGCExtraction:
    def test_format_id_column(self):
        """Test PSGC ID formatting."""
        assert format_id_column(123) == "0000000123"
        assert format_id_column("123") == "0000000123"
        assert format_id_column("0123456789") == "0123456789"

    def test_convert_to_int(self):
        """Test numeric conversion."""
        assert convert_to_int("123") == 123
        assert convert_to_int("123.0") == 123
        assert convert_to_int("  123  ") == 123
        assert pd.isna(convert_to_int("abc"))
        assert pd.isna(convert_to_int(""))

    def test_set_psgc(self, sample_psgc_xlsx):
        """Test PSGC data loading and cleaning."""
        df = set_psgc(sample_psgc_xlsx)

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

        # Check data types and content
        assert df["id"].dtype == "object"  # string IDs
        assert df["cc"].dtype in ["float64", "int64"]  # correspondence codes
        assert df["2024_pop"].dtype in ["float64", "int64"]  # population

        # Check specific values
        region_row = df[df["geo"] == "Reg"].iloc[0]
        assert region_row["id"] == "0100000000"
        assert region_row["name"] == "Region I"
        assert region_row["cc"] == 1.0
