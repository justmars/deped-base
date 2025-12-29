import pandas as pd

from src.foundation.extract_dataframes import extract_dataframes


class TestDataFrameExtraction:
    def test_extract_dataframes(self, test_env):
        """Test the main dataframe extraction function."""
        from src.foundation.common import env

        # This should work with our test environment
        psgc_df, enroll_df, geo_df, levels_df, addr_df = extract_dataframes()

        # Check that all dataframes are returned
        assert not psgc_df.empty
        assert not enroll_df.empty
        assert not geo_df.empty
        assert not levels_df.empty
        assert not addr_df.empty

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
