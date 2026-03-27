import importlib.util
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch

sqlalchemy = types.ModuleType("sqlalchemy")
sqlalchemy.create_engine = lambda *args, **kwargs: None
sqlalchemy.text = lambda value: value
sqlalchemy_engine = types.ModuleType("sqlalchemy.engine")
sqlalchemy_engine.Engine = object
sys.modules.setdefault("sqlalchemy", sqlalchemy)
sys.modules.setdefault("sqlalchemy.engine", sqlalchemy_engine)

MODULE_PATH = Path("/home/fillip/projec/cursorskills/.cursor/skills/source-system-analyser/scripts/volume_projection/collector.py")
SPEC = importlib.util.spec_from_file_location("volume_projection_collector", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(MODULE)


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class VolumeProjectionCollectorTests(unittest.TestCase):
    def test_run_collect_requires_db_analysis_config(self):
        with patch.object(
            MODULE,
            "load_config",
            return_value={"error": "db_analysis_config_required", "instruction": "Ask the user"},
        ):
            result = MODULE.run_collect(object(), schema="public")

        self.assertEqual(result["error"], "db_analysis_config_required")

    def test_list_tables_filters_excluded_tables(self):
        class _Conn:
            def execute(self, stmt, params=None):
                return _FakeResult([("customers",), ("orders",)])

        tables = MODULE._list_tables(
            _Conn(),
            "postgresql",
            "public",
            config={"exclude_schemas": [], "exclude_tables": ["orders"], "max_row_limit": None},
        )

        self.assertEqual(tables, ["customers"])


if __name__ == "__main__":
    unittest.main()
