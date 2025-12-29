import polars as pl

from src.foundation.extract_dataframes import extract_dataframes


class TestDataFrameExtraction:
    def test_extract_dataframes(self, test_env):
        """Test the main dataframe extraction function."""
        from src.foundation.common import env

        # This should work with our test environment
        psgc_df, enroll_df, geo_df, levels_df, addr_df = extract_dataframes()

        # Check that all dataframes are returned
        assert isinstance(psgc_df, pl.DataFrame)
        assert isinstance(enroll_df, pl.DataFrame)
        assert isinstance(geo_df, pl.DataFrame)
        assert isinstance(levels_df, pl.DataFrame)
        assert isinstance(addr_df, pl.DataFrame)

        # Check that they have rows
        assert psgc_df.height > 0
        assert enroll_df.height > 0
        assert geo_df.height > 0
        assert levels_df.height > 0
        assert addr_df.height > 0

        # Check expected columns
        assert "id" in psgc_df.columns
        assert "name" in psgc_df.columns

        assert "school_year" in enroll_df.columns
        assert "school_id" in enroll_df.columns
        assert "grade" in enroll_df.columns
        assert "sex" in enroll_df.columns
        assert "num_students" in enroll_df.columns

        assert "school_id" in geo_df.columns
        assert "longitude" in geo_df.columns
        assert "latitude" in geo_df.columns

        assert "school_year" in levels_df.columns
        assert "school_id" in levels_df.columns

        assert "school_id" in addr_df.columns
        assert "address_id" in addr_df.columns
