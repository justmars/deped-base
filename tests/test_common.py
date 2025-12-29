import polars as pl
import pytest
from sqlite_utils import Database

from src.foundation.common import (
    add_to,
    bulk_update,
    convert_trailing_roman,
    normalize_geo_name,
    normalize_region_name,
    prep_table,
)


class TestCommonFunctions:
    def test_normalize_region_name(self):
        """Test region name normalization."""
        assert normalize_region_name("Region I (Ilocos Region)") == "region i"
        assert (
            normalize_region_name("NATIONAL CAPITAL REGION")
            == "national capital region"
        )
        assert normalize_region_name("Region XIII") == "region xiii"

    def test_convert_trailing_roman(self):
        """Test Roman numeral conversion."""
        assert convert_trailing_roman("District I") == "District 1"
        assert convert_trailing_roman("Region II") == "Region 2"
        assert convert_trailing_roman("Division X.") == "Division 10"
        assert convert_trailing_roman("No roman here") == "No roman here"

    def test_normalize_geo_name(self):
        """Test geographic name normalization."""
        assert normalize_geo_name("Sto. Tomas") == "santo tomas"
        assert normalize_geo_name("City of Manila") == "manila city"
        assert normalize_geo_name("Quezon City (2nd District)") == "quezon city"

    def test_prep_table(self, tmp_path):
        """Test table preparation."""
        db_path = tmp_path / "test.db"
        db = Database(db_path)

        try:
            values = [
                {"id": 1, "label": "Test 1"},
                {"id": 2, "label": "Test 2"},
            ]

            prep_table(db, "test_table", values)

            # Check table was created and populated
            assert db["test_table"].exists()
            rows = list(db["test_table"].rows)
            assert len(rows) == 2
            assert rows[0]["label"] == "Test 1"
        finally:
            db.conn.close()

    def test_add_to(self, tmp_path):
        """Test adding dataframe to database."""
        db_path = tmp_path / "test.db"
        db = Database(db_path)

        try:
            df = pl.DataFrame(
                {
                    "id": [1, 2, 3],
                    "name": ["Alice", "Bob", "Charlie"],
                    "value": [10, 20, 30],
                }
            )

            result_db = add_to(db, df, "test_table")

            assert result_db["test_table"].exists()
            rows = list(result_db["test_table"].rows)
            assert len(rows) == 3
            assert rows[1]["name"] == "Bob"
        finally:
            db.conn.close()

    def test_bulk_update(self, tmp_path):
        """Test bulk update with foreign keys."""
        db_path = tmp_path / "test.db"
        db = Database(db_path)

        try:
            # Create dependency table
            db["categories"].insert_all(
                [
                    {"id": 1, "label": "Category A"},
                    {"id": 2, "label": "Category B"},
                ],
                pk="id",
            )

            # Create main table
            db["items"].insert_all(
                [
                    {"id": 1, "name": "Item 1", "category": "Category A"},
                    {"id": 2, "name": "Item 2", "category": "Category B"},
                    {"id": 3, "name": "Item 3", "category": "Category A"},
                ],
                pk="id",
            )

            # Perform bulk update
            bulk_update(
                db=db,
                tbl_name="items",
                target_col="category",
                dependency_tbl="categories",
                fk_col="category_id",
            )

            # Check results
            rows = list(db["items"].rows)
            assert rows[0]["category_id"] == "1"  # sqlite returns strings
            assert rows[1]["category_id"] == "2"

            # Check foreign key was added
            assert "category_id" in db["items"].columns_dict
            # Note: sqlite-utils foreign key checking might not be directly testable here
        finally:
            db.conn.close()
