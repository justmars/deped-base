import polars as pl
import pytest

from src.foundation.plugins.meta import (
    _log_invalid_num_student_values,
    extract_grade_sex_columns,
    extract_school_year,
    sanitize_num_students,
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
        df = pl.DataFrame(
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
        series = pl.Series(
            [
                "kinder_male",
                "kinder_female",
                "g11_stem_male",
                "g11_stem_female",
                "g12_abm_male",
            ]
        )

        result = split_grade_strand_sex(series)

        # Result is a DataFrame with 5 rows
        assert isinstance(result, pl.DataFrame)
        assert result.height == 5

        # Check specific rows
        assert result["grade"][0] == "kinder"
        assert result["sex"][0] == "male"
        assert result["strand"][0] is None

        assert result["grade"][2] == "g11"
        assert result["strand"][2] == "stem"
        assert result["sex"][2] == "male"

    def test_sanitize_num_students(self):
        """Ensure enrollment counts are parsed into integers."""
        df = pl.DataFrame(
            {
                "num_students": ["1,200", "  50 ", "abc", None, "0"],
            }
        )
        normalized = df.with_columns(
            sanitize_num_students(pl.col("num_students")).alias("num_students")
        )
        assert normalized["num_students"].to_list() == [1200, 50, None, None, 0]
        assert normalized["num_students"].dtype == pl.Int64

    def test_log_invalid_num_student_values(self, capsys):
        """Ensure invalid rows produce a log entry with samples."""
        df = pl.DataFrame(
            {
                "__raw_num_students": ["100", "not-numeric", ""],
                "num_students": [100, None, None],
            }
        )
        _log_invalid_num_student_values(df, "test normalization")
        captured = capsys.readouterr()
        assert (
            "Dropped 2 invalid num_students rows during test normalization."
            in captured.out
        )
        assert "Sample values" in captured.out

    def test_unpack_enroll_data(self, test_env):
        """Test unpacking enrollment data from directory."""
        from src.foundation.common import env

        school_year_meta, enroll_df, levels_df = unpack_enroll_data(
            env.path("ENROLL_DIR")
        )

        # Check that we have data
        assert isinstance(school_year_meta, pl.DataFrame)
        assert isinstance(enroll_df, pl.DataFrame)
        assert isinstance(levels_df, pl.DataFrame)

        assert school_year_meta.height > 0
        assert enroll_df.height > 0
        assert levels_df.height > 0

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
        assert enroll_df["school_year"][0] == "2023-2024"
