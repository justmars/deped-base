import polars as pl

from src.foundation.plugins.geodata import set_coordinates


class TestGeoDataExtraction:
    def test_set_coordinates(self, sample_geo_csv):
        """Test attaching coordinates to school metadata."""
        # Create sample meta_df
        meta_df = pl.DataFrame(
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
        school_100001 = result.filter(pl.col("school_id") == 100001).row(0, named=True)
        assert school_100001["longitude"] == 120.5678
        assert school_100001["latitude"] == 18.1234

        # Check that missing school has None coordinates
        school_100004 = result.filter(pl.col("school_id") == 100004).row(0, named=True)
        assert school_100004["longitude"] is None
        assert school_100004["latitude"] is None
