import os
import subprocess
import sys
from pathlib import Path

import pytest


class TestCLI:
    def test_cli_prep_command(self, test_env):
        """Test the 'cli prep' command."""
        # Run the prep command
        result = subprocess.run(
            [sys.executable, "-m", "src.foundation", "prep"],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent,
        )

        assert result.returncode == 0
        assert "Using" in result.stdout
        assert "rebuilding" in result.stdout

        # Check that database was created
        db_path = Path(os.environ["DB_FILE"])
        assert db_path.exists()

        # Check that tables were created (we can check this by trying to connect)
        import sqlite3

        conn = sqlite3.connect(db_path)
        try:
            cursor = conn.cursor()

            # Check that expected tables exist
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [row[0] for row in cursor.fetchall()]

            expected_tables = ["school_sizes", "school_grades", "school_epochs"]
            for table in expected_tables:
                assert table in tables
        finally:
            conn.close()

    def test_cli_build_command(self, test_env):
        """Test the 'cli build' command."""
        # First run prep to set up the database
        subprocess.run(
            [sys.executable, "-m", "src.foundation", "prep"],
            capture_output=True,
            cwd=Path(__file__).parent.parent,
        )

        # Then run build
        result = subprocess.run(
            [sys.executable, "-m", "src.foundation", "build"],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent,
        )

        assert result.returncode == 0
        assert "Populating" in result.stdout
        assert "main" in result.stdout and "table" in result.stdout

        # Check that main tables were created
        import sqlite3

        db_path = Path(os.environ["DB_FILE"])
        conn = sqlite3.connect(db_path)
        try:
            cursor = conn.cursor()

            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [row[0] for row in cursor.fetchall()]

            expected_tables = [
                "school_sizes",
                "school_grades",
                "school_epochs",  # from prep
                "school_years",
                "enroll",
                "geos",
                "addr",
                "psgc",  # from build
            ]
            for table in expected_tables:
                assert table in tables
        finally:
            conn.close()
