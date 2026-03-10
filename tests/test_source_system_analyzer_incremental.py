import importlib.util
import sys
import types
import unittest
from pathlib import Path

sqlalchemy = types.ModuleType("sqlalchemy")
sqlalchemy.create_engine = lambda *args, **kwargs: None
sqlalchemy.inspect = lambda *args, **kwargs: None
sqlalchemy.text = lambda value: value
sqlalchemy.MetaData = object
sqlalchemy.Table = object
sqlalchemy.select = lambda *args, **kwargs: None
sqlalchemy_engine = types.ModuleType("sqlalchemy.engine")
sqlalchemy_engine.Engine = object
sqlalchemy_exc = types.ModuleType("sqlalchemy.exc")
sqlalchemy_exc.SAWarning = Warning
sys.modules.setdefault("sqlalchemy", sqlalchemy)
sys.modules.setdefault("sqlalchemy.engine", sqlalchemy_engine)
sys.modules.setdefault("sqlalchemy.exc", sqlalchemy_exc)

MODULE_PATH = Path("/home/fillip/projec/cursorskills/.cursor/skills/source-system-analyser/scripts/source_system_analyzer.py")
SPEC = importlib.util.spec_from_file_location("source_system_analyzer", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(MODULE)


class SourceSystemAnalyzerIncrementalTests(unittest.TestCase):
    def test_detect_incremental_columns_prefers_update_watermark(self):
        columns = [
            {"name": "customer_id", "type": "bigint", "is_incremental": True},
            {"name": "created_at", "type": "datetime2", "is_incremental": False},
            {"name": "updated_at", "type": "datetime2", "is_incremental": False},
        ]

        result = MODULE.detect_incremental_columns(columns, ["customer_id"])

        self.assertEqual(result, ["updated_at"])

    def test_detect_incremental_columns_allows_modified_variants(self):
        columns = [
            {"name": "id", "type": "bigint", "is_incremental": True},
            {"name": "last_modified_on", "type": "timestamp", "is_incremental": False},
        ]

        result = MODULE.detect_incremental_columns(columns, ["id"])

        self.assertEqual(result, ["last_modified_on"])


if __name__ == "__main__":
    unittest.main()
