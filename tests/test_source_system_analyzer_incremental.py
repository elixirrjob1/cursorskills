import importlib.util
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch

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
    def test_projection_lookup_maps_new_horizons(self):
        report = {
            "tables": [
                {
                    "schema": "public",
                    "table": "customers",
                    "projections": {
                        "1_year": {"estimated_rows": 15},
                        "2_year": {"estimated_rows": 20},
                        "5_year": {"estimated_rows": 35},
                    },
                }
            ]
        }

        with patch.object(MODULE.predictor, "build_projection_report", return_value=report):
            result = MODULE._projection_lookup(object())

        self.assertEqual(
            result[("public", "customers")],
            {
                "row_count_projection_1y": 15,
                "row_count_projection_2y": 20,
                "row_count_projection_5y": 35,
            },
        )

    def test_projection_lookup_returns_empty_when_report_unavailable(self):
        with patch.object(MODULE.predictor, "build_projection_report", return_value={"error": "No successful collection run found. Run collector first."}):
            result = MODULE._projection_lookup(object())

        self.assertEqual(result, {})

    def test_collect_projection_inputs_runs_setup_and_collect(self):
        fake_engine = object()

        with patch.object(MODULE.collector, "run_setup") as run_setup, \
             patch.object(MODULE.collector, "run_collect") as run_collect:
            MODULE._collect_projection_inputs(fake_engine, "dbo")

        run_setup.assert_called_once_with(fake_engine)
        run_collect.assert_called_once_with(fake_engine, schema="dbo")

    def test_build_source_system_document_uses_direct_history_projection_fallback(self):
        fake_engine = types.SimpleNamespace(dialect=types.SimpleNamespace(name="postgresql"))

        with patch.object(MODULE, "get_engine", return_value=fake_engine), \
             patch.object(MODULE, "get_adapter", return_value=None), \
             patch.object(
                 MODULE,
                 "fetch_schema_metadata",
                 return_value={
                     "tables": ["customers"],
                     "columns": {"customers": [{"name": "customer_id", "type": "integer", "nullable": False, "is_incremental": False}]},
                     "primary_keys": {"customers": ["customer_id"]},
                     "foreign_keys": {"customers": []},
                 },
             ), \
             patch.object(MODULE, "parse_connection_info", return_value={"driver": "postgresql"}), \
             patch.object(MODULE, "fetch_database_timezone", return_value="UTC"), \
             patch.object(MODULE, "fetch_row_counts", return_value={"customers": 10}), \
             patch.object(MODULE, "_collect_projection_inputs"), \
             patch.object(MODULE, "_projection_lookup", return_value={}), \
             patch.object(MODULE, "_direct_history_projection_lookup", return_value={("public", "customers"): {"row_count_projection_1y": 18, "row_count_projection_2y": 26, "row_count_projection_5y": 50}}), \
             patch.object(MODULE, "fetch_sample_rows", return_value=(["customer_id"], [])), \
             patch.object(MODULE, "detect_partition_columns", return_value=([], "exact")), \
             patch.object(MODULE, "detect_incremental_columns", return_value=[]), \
             patch.object(MODULE, "fetch_column_statistics", return_value={}), \
             patch.object(MODULE, "detect_join_candidates", return_value=[]), \
             patch.object(MODULE, "_apply_concept_classification", return_value={"concepts": []}):
            result = MODULE.build_source_system_document("postgresql://user:pass@localhost:5432/demo", schema="public")

        self.assertEqual(result["tables"][0]["row_count_projection_1y"], 18)
        self.assertEqual(result["tables"][0]["row_count_projection_2y"], 26)
        self.assertEqual(result["tables"][0]["row_count_projection_5y"], 50)

    def test_build_source_system_document_prefers_direct_history_projection_fields(self):
        fake_engine = types.SimpleNamespace(dialect=types.SimpleNamespace(name="postgresql"))

        with patch.object(MODULE, "get_engine", return_value=fake_engine), \
             patch.object(MODULE, "get_adapter", return_value=None), \
             patch.object(
                 MODULE,
                 "fetch_schema_metadata",
                 return_value={
                     "tables": ["customers"],
                     "columns": {"customers": [{"name": "customer_id", "type": "integer", "nullable": False, "is_incremental": False}]},
                     "primary_keys": {"customers": ["customer_id"]},
                     "foreign_keys": {"customers": []},
                 },
             ), \
             patch.object(MODULE, "parse_connection_info", return_value={"driver": "postgresql"}), \
             patch.object(MODULE, "fetch_database_timezone", return_value="UTC"), \
             patch.object(MODULE, "fetch_row_counts", return_value={"customers": 10}), \
             patch.object(MODULE, "_collect_projection_inputs"), \
             patch.object(MODULE, "_projection_lookup", return_value={("public", "customers"): {"row_count_projection_1y": 15, "row_count_projection_2y": 20, "row_count_projection_5y": 35}}), \
             patch.object(MODULE, "_direct_history_projection_lookup", return_value={("public", "customers"): {"row_count_projection_1y": 99, "row_count_projection_2y": 99, "row_count_projection_5y": 99}}), \
             patch.object(MODULE, "fetch_sample_rows", return_value=(["customer_id"], [])), \
             patch.object(MODULE, "detect_partition_columns", return_value=([], "exact")), \
             patch.object(MODULE, "detect_incremental_columns", return_value=[]), \
             patch.object(MODULE, "fetch_column_statistics", return_value={}), \
             patch.object(MODULE, "detect_join_candidates", return_value=[]), \
             patch.object(MODULE, "_apply_concept_classification", return_value={"concepts": []}):
            result = MODULE.build_source_system_document("postgresql://user:pass@localhost:5432/demo", schema="public")

        self.assertEqual(len(result["tables"]), 1)
        self.assertEqual(result["tables"][0]["row_count"], 10)
        self.assertEqual(result["tables"][0]["row_count_projection_1y"], 99)
        self.assertEqual(result["tables"][0]["row_count_projection_2y"], 99)
        self.assertEqual(result["tables"][0]["row_count_projection_5y"], 99)

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

    def test_check_timestamp_ordering_adds_finding_when_constraint_missing(self):
        tables = [
            {
                "table": "customers",
                "columns": [
                    {"name": "created_at", "type": "timestamp", "nullable": False},
                    {"name": "updated_at", "type": "timestamp", "nullable": True},
                ],
            }
        ]

        result = MODULE.check_timestamp_ordering(tables, {"customers": []})

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["check"], "timestamp_ordering")
        self.assertEqual(result[0]["severity"], "info")
        self.assertIn("updated_at IS NULL OR created_at <= updated_at", result[0]["recommendation"])

    def test_check_timestamp_ordering_skips_when_constraint_exists(self):
        tables = [
            {
                "table": "customers",
                "columns": [
                    {"name": "created_at", "type": "timestamp", "nullable": False},
                    {"name": "updated_at", "type": "timestamp", "nullable": False},
                ],
            }
        ]
        check_constraints = {
            "customers": [
                {
                    "column": None,
                    "constraint_name": "ck_customers_created_updated",
                    "check_clause": "([created_at] <= [updated_at])",
                }
            ]
        }

        result = MODULE.check_timestamp_ordering(tables, check_constraints)

        self.assertEqual(result, [])


if __name__ == "__main__":
    unittest.main()
