from pathlib import Path

import polars as pl

from src.foundation.plugins.meta import melt_enrollment_csv


def _write_sample_enrollment(path: Path):
    data = {
        "school_id": [1, 2, 3],
        "school_name": ["A", "B", "C"],
        "region": ["Region I", "Region I", "Region I"],
        "province": ["Test Province", "Test Province", "Test Province"],
        "municipality": ["Test Muni", "Test Muni", "Test Muni"],
        "barangay": ["Barangay A", "Barangay B", "Barangay C"],
        "street_address": ["Addr", "Addr", "Addr"],
        "legislative_district": ["1st District"] * 3,
        "division": ["Division 1"] * 3,
        "school_district": ["District 1"] * 3,
        "sector": ["Public"] * 3,
        "school_management": ["DepEd"] * 3,
        "annex_status": ["Standalone School"] * 3,
        "offers_es": [True, True, True],
        "offers_jhs": [False, False, False],
        "offers_shs": [False, False, False],
        "kinder_male": ["1,000", "500", "abc"],
        "kinder_female": ["1,100", "600", "2"],
    }
    df = pl.DataFrame(data)
    df.write_csv(path)


def test_melt_enrollment_csv_logs_invalid(tmp_path, capsys):
    path = tmp_path / "enrollment_2025-2026.csv"
    _write_sample_enrollment(path)

    df = melt_enrollment_csv(path)

    assert df["num_students"].dtype == pl.Int64
    assert df.height > 0

    captured = capsys.readouterr()
    assert "Dropped 1 invalid num_students rows" in captured.out
