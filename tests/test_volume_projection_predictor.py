import importlib.util
import sys
import types
import unittest
from pathlib import Path

sqlalchemy = types.ModuleType("sqlalchemy")
sqlalchemy.create_engine = lambda *args, **kwargs: None
sqlalchemy.text = lambda value: value
sqlalchemy_engine = types.ModuleType("sqlalchemy.engine")
sqlalchemy_engine.Engine = object
sys.modules.setdefault("sqlalchemy", sqlalchemy)
sys.modules.setdefault("sqlalchemy.engine", sqlalchemy_engine)

MODULE_PATH = Path("/home/fillip/projec/cursorskills/.cursor/skills/source-system-analyser/scripts/volume_projection/predictor.py")
SPEC = importlib.util.spec_from_file_location("volume_projection_predictor", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(MODULE)


class _FakeResult:
    def __init__(self, *, one=None, all_rows=None):
        self._one = one
        self._all = all_rows or []

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._all


class _FakeConnection:
    def __init__(self, growth_rows):
        self._growth_rows = growth_rows

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, stmt, params=None):
        sql = str(stmt)
        if "FROM prediction.collection_runs" in sql:
            return _FakeResult(one=(7, "2026-03-10T12:00:00Z"))
        if "FROM prediction.database_snapshots" in sql:
            return _FakeResult(one=None)
        if "FROM prediction.table_size_snapshots" in sql:
            return _FakeResult(
                all_rows=[
                    ("customers", "public", 10, 100.0, 1000, 800, 200, 1.0, 12, 3, 1),
                ]
            )
        if "FROM prediction.growth_history" in sql:
            return _FakeResult(all_rows=self._growth_rows)
        raise AssertionError(f"Unexpected SQL: {sql}")


class _FakeEngine:
    def __init__(self, growth_rows):
        self.dialect = types.SimpleNamespace(name="postgresql")
        self._growth_rows = growth_rows

    def connect(self):
        return _FakeConnection(self._growth_rows)


class VolumeProjectionPredictorTests(unittest.TestCase):
    def test_build_projection_report_uses_1y_2y_5y_horizons(self):
        engine = _FakeEngine(
            [
                ("2025-01-01", 2, 10),
                ("2025-02-01", 4, 14),
            ]
        )

        report = MODULE.build_projection_report(engine)
        projections = report["tables"][0]["projections"]

        self.assertEqual(projections["1_year"]["estimated_rows"], 46)
        self.assertEqual(projections["2_year"]["estimated_rows"], 82)
        self.assertEqual(projections["5_year"]["estimated_rows"], 190)
        self.assertIn("projected_1_year_size_bytes", report["summary"])
        self.assertIn("projected_2_year_size_bytes", report["summary"])
        self.assertIn("projected_5_year_size_bytes", report["summary"])

    def test_build_projection_report_falls_back_to_current_row_count_without_history(self):
        engine = _FakeEngine([])

        report = MODULE.build_projection_report(engine)
        projections = report["tables"][0]["projections"]

        self.assertEqual(projections["1_year"]["estimated_rows"], 10)
        self.assertEqual(projections["2_year"]["estimated_rows"], 10)
        self.assertEqual(projections["5_year"]["estimated_rows"], 10)


if __name__ == "__main__":
    unittest.main()
