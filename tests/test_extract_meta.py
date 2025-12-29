import pandas as pd
import pytest

from src.foundation.extract_meta import (
    extract_grade_sex_columns,
    extract_school_year,
    split_grade_strand_sex,
    unpack_enroll_data,
)


class TestMetaExtraction:
    def test_extract_school_year(self):
        """Test school year extraction from filename."""
        assert extract_school_year("enrollment_2023-2024.csv") == "2023-2024"
        assert extract_school_year("data_2017-2018_final.csv") == "2017-2018"

        with pytest.raises(ValueError):
            extract_school_year("no_year_here.csv")

    def test_extract_grade_sex_columns(self):
        """Test identification of enrollment columns."""
        df = pd.DataFrame(
            {
                "school_id": [1, 2],
                "school_name": ["A", "B"],
                "kinder_male": [10, 20],
                "kinder_female": [15, 25],
                "g1_male": [12, 22],
            }
        )

        id_vars = ["school_id", "school_name"]
        grade_cols = extract_grade_sex_columns(df, id_vars)

        expected = ["kinder_male", "kinder_female", "g1_male"]
        assert grade_cols == expected

    def test_split_grade_strand_sex(self):
        """Test splitting grade/strand/sex columns."""
        series = pd.Series(
            [
                "kinder_male",
                "kinder_female",
                "g11_stem_male",
                "g11_stem_female",
                "g12_abm_male",
            ]
        )

        result = split_grade_strand_sex(series)

        assert len(result) == 5
        assert result.iloc[0]["grade"] == "kinder"
        assert result.iloc[0]["sex"] == "male"
        assert pd.isna(result.iloc[0]["strand"])

        assert result.iloc[2]["grade"] == "g11"
        assert result.iloc[2]["strand"] == "stem"
        assert result.iloc[2]["sex"] == "male"

    def test_unpack_enroll_data(self, test_env):
        """Test unpacking enrollment data from directory."""
        from src.foundation.common import env

        school_year_meta, enroll_df, levels_df = unpack_enroll_data(
            env.path("ENROLL_DIR")
        )

        # Check that we have data
        assert not school_year_meta.empty
        assert not enroll_df.empty
        assert not levels_df.empty

        # Check expected columns in enroll_df
        expected_enroll_cols = [
            "school_year",
            "school_id",
            "grade",
            "sex",
            "num_students",
        ]
        assert all(col in enroll_df.columns for col in expected_enroll_cols)

        # Check that school_year is correctly extracted
        assert enroll_df["school_year"].iloc[0] == "2023-2024"
