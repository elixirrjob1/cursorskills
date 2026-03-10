import sys
import types
import unittest

sqlalchemy = types.ModuleType("sqlalchemy")
sqlalchemy.MetaData = object
sqlalchemy.Table = object
sqlalchemy.func = types.SimpleNamespace(count=lambda *args, **kwargs: None)
sqlalchemy.inspect = lambda *args, **kwargs: None
sqlalchemy.select = lambda *args, **kwargs: None
sqlalchemy.text = lambda value: value
sqlalchemy_engine = types.ModuleType("sqlalchemy.engine")
sqlalchemy_engine.Engine = object
sys.modules.setdefault("sqlalchemy", sqlalchemy)
sys.modules.setdefault("sqlalchemy.engine", sqlalchemy_engine)

from api import db


class IncrementalInferenceTests(unittest.TestCase):
    def test_prefers_update_timestamps_over_primary_key_and_created_at(self):
        columns = [
            {"name": "customer_id", "type": "bigint"},
            {"name": "created_at", "type": "datetime2"},
            {"name": "updated_at", "type": "datetime2"},
        ]

        self.assertEqual(db._infer_incremental_columns(columns, ["customer_id"]), ["updated_at"])

    def test_returns_empty_when_no_update_style_watermark_exists(self):
        columns = [
            {"name": "customer_id", "type": "bigint"},
            {"name": "created_at", "type": "datetime2"},
        ]

        self.assertEqual(db._infer_incremental_columns(columns, ["customer_id"]), [])


if __name__ == "__main__":
    unittest.main()
