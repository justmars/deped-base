import pandas as pd

from src.foundation.extract_geodata import set_coordinates


class TestGeoDataExtraction:
    def test_set_coordinates(self, sample_geo_csv):
        """Test attaching coordinates to school metadata."""
        # Create sample meta_df
        meta_df = pd.DataFrame(
            {
                "school_id": [100001, 100002, 100004],  # Note: 100004 not in geo file
                "school_name": ["School A", "School B", "School D"],
                "region": ["Region I", "Region I", "Region I"],
            }
        )

        result = set_coordinates(sample_geo_csv, meta_df)

        # Check that coordinates were added
        assert "longitude" in result.columns
        assert "latitude" in result.columns

        # Check specific values
        school_100001 = result[result["school_id"] == 100001].iloc[0]
        assert school_100001["longitude"] == 120.5678
        assert school_100001["latitude"] == 18.1234

        # Check that missing school has NaN coordinates
        school_100004 = result[result["school_id"] == 100004].iloc[0]
        assert pd.isna(school_100004["longitude"])
        assert pd.isna(school_100004["latitude"])
