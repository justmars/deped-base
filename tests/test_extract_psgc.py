import polars as pl
import pytest

from src.foundation.plugins.psgc import set_psgc


class TestPSGCExtraction:
    def test_set_psgc(self, sample_psgc_xlsx):
        """Test PSGC data loading and cleaning."""
        df = set_psgc(sample_psgc_xlsx)

        # Check that it's a polars DataFrame
        assert isinstance(df, pl.DataFrame)

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
        assert df.height > 0

    def test_old_name_preference(self, tmp_path):
        """Ensure province rows prefer `old_names` when present."""
        data = {
            "10-digit PSGC": [130000000, 130010000],
            "Name": ["Region X", "New Province"],
            "Correspondence Code": [1.0, 130.0],
            "Geographic Level": ["Reg", "Prov"],
            "Old names": ["", "Old Province"],
            "City Class": ["", ""],
            "Income\nClassification (DOF DO No. 074.2024)": ["", ""],
            "Urban / Rural\n(based on 2020 CPH)": ["", ""],
            "2024 Population": [1000000.0, 200000.0],
            "Unnamed: 9": ["", ""],
            "Status": ["", ""],
        }
        df = pl.DataFrame(data)
        path = tmp_path / "custom_psgc.xlsx"
        df.write_excel(path, worksheet="PSGC")

        result = set_psgc(path)
        province_row = result.filter(pl.col("geo") == "Prov")
        assert province_row["name"][0] == "Old Province"
