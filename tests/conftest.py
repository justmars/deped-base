import os
import pathlib
import tempfile
from pathlib import Path

import polars as pl
import pytest
import yaml
from openpyxl import Workbook


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def sample_enrollment_csv(temp_dir):
    """Create a sample enrollment CSV file."""
    data = {
        "school_id": [100001, 100002, 100003],
        "school_name": ["Test School 1", "Test School 2", "Test School 3"],
        "region": ["Region I", "Region I", "Region I"],
        "division": ["Test Division", "Test Division", "Test Division"],
        "province": ["TEST PROVINCE", "TEST PROVINCE", "TEST PROVINCE"],
        "school_district": ["District 1", "District 1", "District 1"],
        "legislative_district": ["1st District", "1st District", "1st District"],
        "municipality": ["TEST MUNI", "TEST MUNI", "TEST MUNI"],
        "barangay": ["Barangay 1", "Barangay 2", "Barangay 3"],
        "street_address": ["Address 1", "Address 2", "Address 3"],
        "sector": ["Public", "Public", "Public"],
        "school_management": ["DepEd", "DepEd", "DepEd"],
        "annex_status": ["Standalone School", "Standalone School", "Standalone School"],
        "offers_es": [True, True, True],
        "offers_jhs": [False, False, False],
        "offers_shs": [False, False, False],
        "kinder_male": [4, 26, 8],
        "kinder_female": [5, 25, 10],
        "g1_male": [3, 33, 11],
        "g1_female": [2, 25, 10],
        "g2_male": [5, 18, 11],
        "g2_female": [7, 29, 4],
        "g3_male": [5, 23, 13],
        "g3_female": [1, 32, 4],
        "g4_male": [2, 30, 8],
        "g4_female": [2, 23, 7],
        "g5_male": [2, 34, 12],
        "g5_female": [9, 35, 14],
        "g6_male": [2, 27, 9],
        "g6_female": [9, 42, 7],
        "g11_stem_male": [1, 5, 2],
        "g11_stem_female": [2, 3, 1],
        "g12_abm_male": [0, 2, 1],
        "g12_abm_female": [1, 1, 0],
    }
    df = pl.DataFrame(data)
    file_path = temp_dir / "enrollment_2023-2024.csv"
    df.write_csv(file_path)
    return file_path


@pytest.fixture
def sample_psgc_xlsx(temp_dir):
    """Create a sample PSGC XLSX file."""
    data = {
        "10-digit PSGC": [100000000, 128000000, 128010000, 128010010],
        "Name": ["Region I", "Ilocos Norte", "Bacarra", "Libtong"],
        "Correspondence Code": [1.0, 128.0, 12801.0, 12801001.0],
        "Geographic Level": ["Reg", "Prov", "Mun", "Bgy"],
        "Old names": ["", "", "", ""],
        "City Class": ["", "", "", ""],
        "Income\nClassification (DOF DO No. 074.2024)": ["", "", "", ""],
        "Urban / Rural\n(based on 2020 CPH)": ["", "", "", ""],
        "2024 Population": [5000000.0, 600000.0, 35000.0, 2000.0],
        "Unnamed: 9": ["", "", "", ""],
        "Status": ["", "", "", ""],
    }
    df = pl.DataFrame(data)
    file_path = temp_dir / "psgc.xlsx"
    df.write_excel(file_path, worksheet="PSGC")
    return file_path


@pytest.fixture
def sample_geo_csv(temp_dir):
    """Create a sample geo coordinates CSV file."""
    data = {
        "id": [100001, 100002, 100003],
        "school_name": ["School A", "School B", "School C"],
        "latitude": [18.1234, 18.2345, 18.3456],
        "longitude": [120.5678, 120.6789, 120.7890],
    }
    df = pl.DataFrame(data)
    file_path = temp_dir / "geo.csv"
    df.write_csv(file_path)
    return file_path


@pytest.fixture
def sample_generic_yml(temp_dir):
    """Create a sample generic.yml file."""
    data = {
        "school_sizes": [
            {
                "id": 1,
                "label": "Extra Small",
                "shorthand": "xs",
                "minimum": 1,
                "maximum": 100,
            },
            {
                "id": 2,
                "label": "Small",
                "shorthand": "sm",
                "minimum": 101,
                "maximum": 500,
            },
        ],
        "school_grades": [
            {"label": "kinder", "key_stage": 1},
            {"label": "g1", "key_stage": 1},
            {"label": "g2", "key_stage": 1},
        ],
        "school_epochs": [
            {"id": 1, "label": "bosy"},
            {"id": 2, "label": "mosy"},
        ],
    }
    file_path = temp_dir / "generic.yml"
    with open(file_path, "w") as f:
        yaml.dump(data, f)
    return file_path


@pytest.fixture
def sample_fixes_yml(temp_dir):
    """Create a sample fixes.yml file."""
    data = {
        "region_psgc_map": {
            "region i": "region i",
            "test region": "test region",
        },
        "provincial_muni_fixes": [
            {
                "province": "test province",
                "municipality": "test muni",
                "corrected": "Test Muni",
            },
        ],
    }
    file_path = temp_dir / "fixes.yml"
    with open(file_path, "w") as f:
        yaml.dump(data, f)
    return file_path


@pytest.fixture
def sample_hr_xlsx(temp_dir):
    """Create a sample teacher workbook for tests."""

    hr_dir = temp_dir / "hr"
    hr_dir.mkdir()

    columns = (
        ["col1", "col2", "col3", "Lis School ID", "Beis School ID"]
        + [f"filler_{i}" for i in range(6, 19)]
        + [f"teacher_role_{i}" for i in range(1, 13)]
    )
    rows: list[dict[str, int | None]] = []
    for idx, sid in enumerate((400001, 400002), start=1):
        row = {col: None for col in columns}
        row["Lis School ID"] = sid
        row["Beis School ID"] = sid + 5000
        for i in range(1, 13):
            row[f"teacher_role_{i}"] = (idx * i) + 3
        rows.append(row)

    df = pl.DataFrame(rows, schema=columns)

    workbook_path = hr_dir / "2022-2023-teachers.xlsx"
    workbook = Workbook()
    workbook.remove(workbook.active)

    header_offset = 5
    for sheet in ("ES DB", "JHS DB", "SHS DB"):
        ws = workbook.create_sheet(sheet)
        for _ in range(header_offset):
            ws.append([None] * len(columns))
        ws.append(columns)
        for record in df.to_dicts():
            ws.append([record[col] for col in columns])

    workbook.save(workbook_path)
    return hr_dir


@pytest.fixture
def test_env(
    temp_dir,
    sample_enrollment_csv,
    sample_psgc_xlsx,
    sample_geo_csv,
    sample_generic_yml,
    sample_fixes_yml,
    sample_hr_xlsx,
):
    """Set up test environment variables."""
    # Create enroll directory and move CSV there
    enroll_dir = temp_dir / "enroll"
    enroll_dir.mkdir()
    sample_enrollment_csv.rename(enroll_dir / sample_enrollment_csv.name)

    # Set environment variables
    os.environ["DB_FILE"] = str(temp_dir / "test.db")
    os.environ["GEOS_TABLE"] = "geos"
    os.environ["GENERIC_FILE"] = str(sample_generic_yml)
    os.environ["ENROLL_DIR"] = str(enroll_dir)
    os.environ["FIXES_FILE"] = str(sample_fixes_yml)
    os.environ["GEO_FILE"] = str(sample_geo_csv)
    os.environ["PSGC_FILE"] = str(sample_psgc_xlsx)
    os.environ["HR_DIR"] = str(sample_hr_xlsx)

    yield temp_dir
