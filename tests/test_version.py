import os
import pathlib
import tomllib

import src.foundation as x

# Manually Define Env Variables in Test
os.environ["DB_FILE"] = "test.db"
os.environ["GENERIC_FILE"] = "generics.yml"


def test_version():
    path = pathlib.Path("pyproject.toml")
    data = tomllib.loads(path.read_text())
    version = data["project"]["version"]
    assert version == x.__version__
