import importlib.util
import tempfile
import sys
import types
import unittest
from contextlib import ExitStack
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
    def test_system_description_config_round_trip(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "source-system-description.json"

            MODULE._save_system_description_config(
                "Retail ERP and order management platform.",
                config_path=str(config_path),
            )
            loaded = MODULE._load_system_description_config(str(config_path))

        self.assertEqual(
            loaded["system_description"],
            "Retail ERP and order management platform.",
        )
        self.assertEqual(loaded["config_path"], str(config_path))

    def test_build_source_system_document_requires_db_analysis_config(self):
        result = MODULE.build_source_system_document(
            "postgresql://user:pass@localhost:5432/demo",
            schema="public",
            config={"error": "db_analysis_config_required", "instruction": "Ask the user"},
        )

        self.assertEqual(result["error"], "db_analysis_config_required")

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
            result = MODULE._projection_lookup(object(), {"exclude_schemas": [], "exclude_tables": [], "max_row_limit": None})

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
            result = MODULE._projection_lookup(object(), {"exclude_schemas": [], "exclude_tables": [], "max_row_limit": None})

        self.assertEqual(result, {})

    def test_collect_projection_inputs_runs_setup_and_collect(self):
        fake_engine = object()
        config = {"exclude_schemas": [], "exclude_tables": [], "max_row_limit": None}

        with patch.object(MODULE.collector, "run_setup") as run_setup, \
             patch.object(MODULE.collector, "run_collect") as run_collect:
            MODULE._collect_projection_inputs(fake_engine, "dbo", config)

        run_setup.assert_called_once_with(fake_engine)
        run_collect.assert_called_once_with(fake_engine, schema="dbo", config=config)

    def test_fetch_schema_metadata_skips_excluded_tables(self):
        class _FakeInspector:
            def get_schema_names(self):
                return ["public"]

            def get_table_names(self, schema=None):
                return ["customers", "orders"]

            def get_columns(self, table_name, schema=None):
                return [{"name": "id", "type": "integer", "nullable": False}]

            def get_pk_constraint(self, table_name, schema=None):
                return {"constrained_columns": ["id"]}

            def get_foreign_keys(self, table_name, schema=None):
                return []

        with patch.object(MODULE, "inspect", return_value=_FakeInspector()):
            result = MODULE.fetch_schema_metadata(
                object(),
                schema="public",
                config={"exclude_schemas": [], "exclude_tables": ["orders"], "max_row_limit": None},
            )

        self.assertEqual(result["tables"], ["customers"])

    def test_build_source_system_document_caps_sample_row_limits(self):
        fake_engine = types.SimpleNamespace(dialect=types.SimpleNamespace(name="postgresql"))
        fake_adapter = types.SimpleNamespace(
            resolve_default_schema=lambda engine: "public",
            fetch_table_descriptions=lambda engine, schema: {},
            fetch_column_descriptions=lambda engine, schema: {},
            fetch_check_constraints=lambda engine, schema: {},
            fetch_enum_columns=lambda engine, schema: {},
            fetch_unique_constraints=lambda engine, schema: {},
            detect_cdc_enabled=lambda engine, table_name, schema: False,
        )
        sample_limits = []
        format_limits = []

        def fake_fetch_sample_rows(engine, table, limit, schema=None, adapter=None):
            sample_limits.append(limit)
            return (["customer_id"], [])

        def fake_check_format_inconsistency(engine, tables, schema, sample_size=200, adapter=None):
            format_limits.append(sample_size)
            return []

        with ExitStack() as stack:
            stack.enter_context(patch.object(MODULE, "get_engine", return_value=fake_engine))
            stack.enter_context(patch.object(MODULE, "get_adapter", return_value=fake_adapter))
            stack.enter_context(
                patch.object(
                    MODULE,
                    "fetch_schema_metadata",
                    return_value={
                        "tables": ["customers"],
                        "columns": {"customers": [{"name": "customer_id", "type": "integer", "nullable": False, "is_incremental": False}]},
                        "primary_keys": {"customers": ["customer_id"]},
                        "foreign_keys": {"customers": []},
                    },
                )
            )
            stack.enter_context(patch.object(MODULE, "parse_connection_info", return_value={"driver": "postgresql"}))
            stack.enter_context(patch.object(MODULE, "fetch_database_timezone", return_value="UTC"))
            stack.enter_context(patch.object(MODULE, "fetch_row_counts", return_value={"customers": 10}))
            stack.enter_context(patch.object(MODULE, "_collect_projection_inputs"))
            stack.enter_context(patch.object(MODULE, "_projection_lookup", return_value={}))
            stack.enter_context(patch.object(MODULE, "_direct_history_projection_lookup", return_value={}))
            stack.enter_context(patch.object(MODULE, "fetch_sample_rows", side_effect=fake_fetch_sample_rows))
            stack.enter_context(patch.object(MODULE, "detect_partition_columns", return_value=([], "exact")))
            stack.enter_context(patch.object(MODULE, "detect_incremental_columns", return_value=[]))
            stack.enter_context(patch.object(MODULE, "fetch_column_statistics", return_value={}))
            stack.enter_context(patch.object(MODULE, "detect_join_candidates", return_value=[]))
            stack.enter_context(patch.object(MODULE, "_apply_concept_classification", return_value={"concepts": []}))
            stack.enter_context(patch.object(MODULE, "check_controlled_value_candidates", return_value=[]))
            stack.enter_context(patch.object(MODULE, "check_nullable_but_never_null", return_value=[]))
            stack.enter_context(patch.object(MODULE, "check_missing_primary_keys", return_value=[]))
            stack.enter_context(patch.object(MODULE, "check_missing_foreign_keys", return_value=[]))
            stack.enter_context(patch.object(MODULE, "check_format_inconsistency", side_effect=fake_check_format_inconsistency))
            stack.enter_context(patch.object(MODULE, "check_range_violations", return_value=[]))
            stack.enter_context(patch.object(MODULE, "check_timestamp_ordering", return_value=[]))
            stack.enter_context(patch.object(MODULE, "check_delete_management", return_value=[]))
            stack.enter_context(patch.object(MODULE, "check_late_arriving_data", return_value=[]))
            stack.enter_context(patch.object(MODULE, "check_timezone", return_value=[]))
            stack.enter_context(patch.object(MODULE, "check_unit_consistency", return_value=[]))
            MODULE.build_source_system_document(
                "postgresql://user:pass@localhost:5432/demo",
                schema="public",
                config={"exclude_schemas": [], "exclude_tables": [], "max_row_limit": 3},
            )

        self.assertEqual(sample_limits, [3])
        self.assertEqual(format_limits, [3])

    def test_build_source_system_document_includes_applied_db_analysis_config(self):
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
             patch.object(MODULE, "_direct_history_projection_lookup", return_value={}), \
             patch.object(MODULE, "fetch_sample_rows", return_value=(["customer_id"], [])), \
             patch.object(MODULE, "detect_partition_columns", return_value=([], "exact")), \
             patch.object(MODULE, "detect_incremental_columns", return_value=[]), \
             patch.object(MODULE, "fetch_column_statistics", return_value={}), \
             patch.object(MODULE, "detect_join_candidates", return_value=[]), \
             patch.object(MODULE, "_apply_concept_classification", return_value={"concepts": []}):
            result = MODULE.build_source_system_document(
                "postgresql://user:pass@localhost:5432/demo",
                schema="public",
                config={"exclude_schemas": ["audit"], "exclude_tables": ["logs"], "max_row_limit": 25},
            )

        self.assertEqual(
            result["source_system_context"]["db_analysis_config"],
            {
                "exclude_schemas": ["audit"],
                "exclude_tables": ["logs"],
                "max_row_limit": 25,
            },
        )

    def test_build_source_system_document_generates_missing_table_and_column_descriptions(self):
        fake_engine = types.SimpleNamespace(dialect=types.SimpleNamespace(name="postgresql"))

        with patch.object(MODULE, "get_engine", return_value=fake_engine), \
             patch.object(MODULE, "get_adapter", return_value=None), \
             patch.object(
                 MODULE,
                 "fetch_schema_metadata",
                 return_value={
                     "tables": ["customers"],
                     "columns": {
                         "customers": [
                             {"name": "customer_id", "type": "integer", "nullable": False, "is_incremental": False},
                             {"name": "email", "type": "text", "nullable": True, "is_incremental": False},
                         ]
                     },
                     "primary_keys": {"customers": ["customer_id"]},
                     "foreign_keys": {"customers": []},
                 },
             ), \
             patch.object(MODULE, "parse_connection_info", return_value={"driver": "postgresql"}), \
             patch.object(MODULE, "fetch_database_timezone", return_value="UTC"), \
             patch.object(MODULE, "fetch_row_counts", return_value={"customers": 10}), \
             patch.object(MODULE, "_collect_projection_inputs"), \
             patch.object(MODULE, "_projection_lookup", return_value={}), \
             patch.object(MODULE, "_direct_history_projection_lookup", return_value={}), \
             patch.object(MODULE, "fetch_sample_rows", return_value=(["customer_id", "email"], [])), \
             patch.object(MODULE, "detect_partition_columns", return_value=([], "exact")), \
             patch.object(MODULE, "detect_incremental_columns", return_value=[]), \
             patch.object(MODULE, "fetch_column_statistics", return_value={}), \
             patch.object(MODULE, "detect_join_candidates", return_value=[]), \
             patch.object(MODULE, "_apply_concept_classification", return_value={"concepts": []}):
            result = MODULE.build_source_system_document(
                "postgresql://user:pass@localhost:5432/demo",
                schema="public",
                config={"exclude_schemas": [], "exclude_tables": [], "max_row_limit": None},
            )

        table = result["tables"][0]
        self.assertEqual(table["table_description"], "Stores customer records including email.")
        self.assertEqual(table["columns"][0]["column_description"], "Primary key for the customer record.")
        self.assertEqual(table["columns"][1]["column_description"], "Contact value for the customer record.")
        self.assertEqual(table["glossary_terms"], [])
        self.assertEqual(table["columns"][0]["glossary_terms"], [])
        self.assertEqual(table["columns"][1]["glossary_terms"], [])

    def test_build_source_system_document_includes_openmetadata_glossary_terms(self):
        fake_engine = types.SimpleNamespace(
            dialect=types.SimpleNamespace(name="postgresql"),
            url=types.SimpleNamespace(host="localhost", port=5432, database="demo"),
        )

        with patch.object(MODULE, "get_engine", return_value=fake_engine), \
             patch.object(MODULE, "get_adapter", return_value=None), \
             patch.object(
                 MODULE,
                 "fetch_schema_metadata",
                 return_value={
                     "tables": ["customers"],
                     "columns": {
                         "customers": [
                             {"name": "customer_id", "type": "integer", "nullable": False, "is_incremental": False},
                             {"name": "email", "type": "text", "nullable": True, "is_incremental": False},
                         ]
                     },
                     "primary_keys": {"customers": ["customer_id"]},
                     "foreign_keys": {"customers": []},
                 },
             ), \
             patch.object(MODULE, "parse_connection_info", return_value={"driver": "postgresql", "database": "demo"}), \
             patch.object(MODULE, "fetch_database_timezone", return_value="UTC"), \
             patch.object(MODULE, "fetch_row_counts", return_value={"customers": 10}), \
             patch.object(
                 MODULE,
                 "_fetch_openmetadata_glossary_assignments",
                 return_value=(
                     {
                         "customers": {
                             "glossary_terms": ["Retail.Customer"],
                             "column_glossary_terms": {
                                 "customer_id": ["Retail.Customer.Identifier"],
                                 "email": ["Retail.Customer.Email"],
                             },
                         }
                     },
                     {
                         "enabled": True,
                         "configured": True,
                         "database_service_name": "svc",
                         "schema": "public",
                         "match_strategy": "",
                         "matched_tables": 1,
                         "unmatched_tables": 0,
                         "error": "",
                     },
                 ),
             ), \
             patch.object(MODULE, "_collect_projection_inputs"), \
             patch.object(MODULE, "_projection_lookup", return_value={}), \
             patch.object(MODULE, "_direct_history_projection_lookup", return_value={}), \
             patch.object(MODULE, "fetch_sample_rows", return_value=(["customer_id", "email"], [])), \
             patch.object(MODULE, "detect_partition_columns", return_value=([], "exact")), \
             patch.object(MODULE, "detect_incremental_columns", return_value=[]), \
             patch.object(MODULE, "fetch_column_statistics", return_value={}), \
             patch.object(MODULE, "detect_join_candidates", return_value=[]), \
             patch.object(MODULE, "_apply_concept_classification", return_value={"concepts": []}):
            result = MODULE.build_source_system_document(
                "postgresql://user:pass@localhost:5432/demo",
                schema="public",
                config={"exclude_schemas": [], "exclude_tables": [], "max_row_limit": None},
            )

        table = result["tables"][0]
        self.assertEqual(table["glossary_terms"], ["Retail.Customer"])
        self.assertEqual(table["columns"][0]["glossary_terms"], ["Retail.Customer.Identifier"])
        self.assertEqual(table["columns"][1]["glossary_terms"], ["Retail.Customer.Email"])
        self.assertEqual(
            result["source_system_context"]["openmetadata_enrichment"],
            {
                "enabled": True,
                "configured": True,
                "database_service_name": "svc",
                "schema": "public",
                "match_strategy": "",
                "matched_tables": 1,
                "unmatched_tables": 0,
                "error": "",
            },
        )

    @patch.dict("os.environ", {"OPENMETADATA_BASE_URL": "http://example:8585", "OPENMETADATA_EMAIL": "admin@example.com", "OPENMETADATA_PASSWORD": "secret"}, clear=False)
    @patch.object(MODULE, "_openmetadata_request")
    def test_fetch_openmetadata_glossary_assignments_matches_by_table_and_column_name(self, mock_request):
        mock_request.return_value = {
            "data": [
                {
                    "name": "customers",
                    "fullyQualifiedName": "snowflake_fivetran.demo.dbo.customers",
                    "databaseSchema": {"fullyQualifiedName": "snowflake_fivetran.demo.dbo"},
                    "tags": [{"tagFQN": "Retail.Customer", "source": "Glossary"}],
                    "columns": [
                        {"name": "customer_id", "tags": [{"tagFQN": "Retail.Customer.Identifier", "source": "Glossary"}]},
                        {"name": "email", "tags": [{"tagFQN": "Retail.Customer.Email", "source": "Glossary"}]},
                    ],
                }
            ]
        }

        assignments, status = MODULE._fetch_openmetadata_glossary_assignments(
            database_name="demo",
            schema_name="dbo",
            table_names=["customers"],
        )

        self.assertEqual(assignments["customers"]["glossary_terms"], ["Retail.Customer"])
        self.assertEqual(
            assignments["customers"]["column_glossary_terms"],
            {
                "customer_id": ["Retail.Customer.Identifier"],
                "email": ["Retail.Customer.Email"],
            },
        )
        self.assertEqual(status["match_strategy"], "table_name")
        self.assertEqual(status["matched_tables"], 1)
        self.assertEqual(status["unmatched_tables"], 0)

    @patch.dict("os.environ", {"OPENMETADATA_BASE_URL": "http://example:8585", "OPENMETADATA_EMAIL": "admin@example.com", "OPENMETADATA_PASSWORD": "secret"}, clear=False)
    @patch.object(MODULE, "_openmetadata_request")
    def test_fetch_openmetadata_classification_catalog_normalizes_scope_and_options(self, mock_request):
        def side_effect(method, endpoint, params=None):
            if endpoint == "classifications":
                return {
                    "data": [
                        {"name": "PII", "fullyQualifiedName": "PII", "provider": "system", "mutuallyExclusive": True, "description": "Personally identifiable information."},
                        {"name": "Tier", "fullyQualifiedName": "Tier", "provider": "system", "mutuallyExclusive": True, "description": "Business criticality tiers."},
                    ]
                }
            if endpoint == "tags":
                return {
                    "data": [
                        {
                            "name": "Sensitive",
                            "fullyQualifiedName": "PII.Sensitive",
                            "classification": {"name": "PII"},
                            "description": "Sensitive personal data.",
                            "recognizers": [{"target": "column_name"}],
                        },
                        {
                            "name": "NonSensitive",
                            "fullyQualifiedName": "PII.NonSensitive",
                            "classification": {"name": "PII"},
                            "description": "Non-sensitive personal data.",
                            "recognizers": [{"target": "content"}],
                        },
                        {
                            "name": "Tier1",
                            "fullyQualifiedName": "Tier.Tier1",
                            "classification": {"name": "Tier"},
                            "description": "Highest business criticality.",
                            "recognizers": [],
                        },
                    ]
                }
            raise AssertionError(f"Unexpected endpoint {endpoint}")

        mock_request.side_effect = side_effect
        catalog = MODULE.fetch_openmetadata_classification_catalog()

        self.assertEqual(
            catalog,
            [
                {
                    "name": "PII",
                    "provider": "system",
                    "description": "Personally identifiable information.",
                    "mutually_exclusive": True,
                    "allowed_on": ["table", "column"],
                    "options": [
                        {"name": "Sensitive", "fqn": "PII.Sensitive", "description": "Sensitive personal data."},
                        {"name": "NonSensitive", "fqn": "PII.NonSensitive", "description": "Non-sensitive personal data."},
                    ],
                },
                {
                    "name": "Tier",
                    "provider": "system",
                    "description": "Business criticality tiers.",
                    "mutually_exclusive": True,
                    "allowed_on": ["table", "column"],
                    "options": [
                        {"name": "Tier1", "fqn": "Tier.Tier1", "description": "Highest business criticality."},
                    ],
                },
            ],
        )

    def test_build_source_system_document_preserves_existing_source_descriptions(self):
        fake_engine = types.SimpleNamespace(dialect=types.SimpleNamespace(name="postgresql"))
        fake_adapter = types.SimpleNamespace(
            resolve_default_schema=lambda engine: "public",
            fetch_table_descriptions=lambda engine, schema: {"customers": "Source table comment"},
            fetch_column_descriptions=lambda engine, schema: {"customers": {"email": "Source column comment"}},
            fetch_check_constraints=lambda engine, schema: {},
            fetch_enum_columns=lambda engine, schema: {},
            fetch_unique_constraints=lambda engine, schema: {},
            detect_cdc_enabled=lambda engine, table_name, schema: False,
            fetch_database_timezone=lambda engine: "UTC",
        )

        with ExitStack() as stack:
            stack.enter_context(patch.object(MODULE, "get_engine", return_value=fake_engine))
            stack.enter_context(patch.object(MODULE, "get_adapter", return_value=fake_adapter))
            stack.enter_context(
                patch.object(
                    MODULE,
                    "fetch_schema_metadata",
                    return_value={
                        "tables": ["customers"],
                        "columns": {"customers": [{"name": "email", "type": "text", "nullable": True, "is_incremental": False}]},
                        "primary_keys": {"customers": []},
                        "foreign_keys": {"customers": []},
                    },
                )
            )
            stack.enter_context(patch.object(MODULE, "parse_connection_info", return_value={"driver": "postgresql"}))
            stack.enter_context(patch.object(MODULE, "fetch_database_timezone", return_value="UTC"))
            stack.enter_context(patch.object(MODULE, "fetch_row_counts", return_value={"customers": 10}))
            stack.enter_context(patch.object(MODULE, "_collect_projection_inputs"))
            stack.enter_context(patch.object(MODULE, "_projection_lookup", return_value={}))
            stack.enter_context(patch.object(MODULE, "_direct_history_projection_lookup", return_value={}))
            stack.enter_context(patch.object(MODULE, "fetch_sample_rows", return_value=(["email"], [])))
            stack.enter_context(patch.object(MODULE, "detect_partition_columns", return_value=([], "exact")))
            stack.enter_context(patch.object(MODULE, "detect_incremental_columns", return_value=[]))
            stack.enter_context(patch.object(MODULE, "fetch_column_statistics", return_value={}))
            stack.enter_context(patch.object(MODULE, "detect_join_candidates", return_value=[]))
            stack.enter_context(patch.object(MODULE, "_apply_concept_classification", return_value={"concepts": []}))
            stack.enter_context(patch.object(MODULE, "check_controlled_value_candidates", return_value=[]))
            stack.enter_context(patch.object(MODULE, "check_nullable_but_never_null", return_value=[]))
            stack.enter_context(patch.object(MODULE, "check_missing_primary_keys", return_value=[]))
            stack.enter_context(patch.object(MODULE, "check_missing_foreign_keys", return_value=[]))
            stack.enter_context(patch.object(MODULE, "check_format_inconsistency", return_value=[]))
            stack.enter_context(patch.object(MODULE, "check_range_violations", return_value=[]))
            stack.enter_context(patch.object(MODULE, "check_timestamp_ordering", return_value=[]))
            stack.enter_context(patch.object(MODULE, "check_delete_management", return_value=[]))
            stack.enter_context(patch.object(MODULE, "check_late_arriving_data", return_value=[]))
            stack.enter_context(patch.object(MODULE, "check_timezone", return_value=[]))
            stack.enter_context(patch.object(MODULE, "check_unit_consistency", return_value=[]))
            result = MODULE.build_source_system_document(
                "postgresql://user:pass@localhost:5432/demo",
                schema="public",
                config={"exclude_schemas": [], "exclude_tables": [], "max_row_limit": None},
            )

        table = result["tables"][0]
        self.assertEqual(table["table_description"], "Source table comment")
        self.assertEqual(table["columns"][0]["column_description"], "Source column comment")

    def test_build_source_system_document_includes_openmetadata_classification_tags(self):
        fake_engine = types.SimpleNamespace(dialect=types.SimpleNamespace(name="postgresql"))
        fake_adapter = types.SimpleNamespace(
            resolve_default_schema=lambda engine: "public",
            fetch_table_descriptions=lambda engine, schema: {},
            fetch_column_descriptions=lambda engine, schema: {},
            fetch_check_constraints=lambda engine, schema: {},
            fetch_enum_columns=lambda engine, schema: {},
            fetch_unique_constraints=lambda engine, schema: {},
            detect_cdc_enabled=lambda engine, table_name, schema: False,
            fetch_database_timezone=lambda engine: "UTC",
        )

        with ExitStack() as stack:
            stack.enter_context(patch.object(MODULE, "get_engine", return_value=fake_engine))
            stack.enter_context(patch.object(MODULE, "get_adapter", return_value=fake_adapter))
            stack.enter_context(
                patch.object(
                    MODULE,
                    "fetch_schema_metadata",
                    return_value={
                        "tables": ["customers"],
                        "columns": {"customers": [{"name": "email", "type": "text", "nullable": True, "is_incremental": False}]},
                        "primary_keys": {"customers": []},
                        "foreign_keys": {"customers": []},
                    },
                )
            )
            stack.enter_context(patch.object(MODULE, "parse_connection_info", return_value={"driver": "postgresql", "database": "demo"}))
            stack.enter_context(patch.object(MODULE, "fetch_database_timezone", return_value="UTC"))
            stack.enter_context(patch.object(MODULE, "fetch_row_counts", return_value={"customers": 10}))
            stack.enter_context(
                patch.object(
                    MODULE,
                    "fetch_openmetadata_classification_catalog",
                    return_value=[
                        {
                            "name": "PII",
                            "provider": "system",
                            "description": "Personally identifiable information.",
                            "mutually_exclusive": True,
                            "allowed_on": ["table", "column"],
                            "options": [{"name": "Sensitive", "fqn": "PII.Sensitive", "description": "Sensitive personal data."}],
                        }
                    ],
                )
            )
            stack.enter_context(
                patch.object(
                    MODULE,
                    "_fetch_openmetadata_glossary_assignments",
                    return_value=(
                        {
                            "customers": {
                                "glossary_terms": ["Retail.Customer"],
                                "classification_tags": ["Tier.Tier1"],
                                "column_glossary_terms": {"email": ["Retail.Customer.Email"]},
                                "column_classification_tags": {"email": ["PII.Sensitive"]},
                            }
                        },
                        {
                            "enabled": True,
                            "configured": True,
                            "database_service_name": "svc",
                            "schema": "public",
                            "match_strategy": "",
                            "matched_tables": 1,
                            "unmatched_tables": 0,
                            "error": "",
                        },
                    ),
                )
            )
            stack.enter_context(patch.object(MODULE, "_collect_projection_inputs"))
            stack.enter_context(patch.object(MODULE, "_projection_lookup", return_value={}))
            stack.enter_context(patch.object(MODULE, "_direct_history_projection_lookup", return_value={}))
            stack.enter_context(patch.object(MODULE, "fetch_sample_rows", return_value=(["email"], [])))
            stack.enter_context(patch.object(MODULE, "detect_partition_columns", return_value=([], "exact")))
            stack.enter_context(patch.object(MODULE, "detect_incremental_columns", return_value=[]))
            stack.enter_context(patch.object(MODULE, "fetch_column_statistics", return_value={}))
            stack.enter_context(patch.object(MODULE, "detect_join_candidates", return_value=[]))
            stack.enter_context(patch.object(MODULE, "_apply_concept_classification", return_value={"concepts": []}))
            stack.enter_context(patch.object(MODULE, "check_controlled_value_candidates", return_value=[]))
            stack.enter_context(patch.object(MODULE, "check_nullable_but_never_null", return_value=[]))
            stack.enter_context(patch.object(MODULE, "check_missing_primary_keys", return_value=[]))
            stack.enter_context(patch.object(MODULE, "check_missing_foreign_keys", return_value=[]))
            stack.enter_context(patch.object(MODULE, "check_format_inconsistency", return_value=[]))
            stack.enter_context(patch.object(MODULE, "check_range_violations", return_value=[]))
            stack.enter_context(patch.object(MODULE, "check_timestamp_ordering", return_value=[]))
            stack.enter_context(patch.object(MODULE, "check_delete_management", return_value=[]))
            stack.enter_context(patch.object(MODULE, "check_late_arriving_data", return_value=[]))
            stack.enter_context(patch.object(MODULE, "check_timezone", return_value=[]))
            stack.enter_context(patch.object(MODULE, "check_unit_consistency", return_value=[]))
            result = MODULE.build_source_system_document(
                "postgresql://user:pass@localhost:5432/demo",
                schema="public",
                config={"exclude_schemas": [], "exclude_tables": [], "max_row_limit": None},
            )

        self.assertEqual(
            result["metadata"]["openmetadata_classifications"],
            [
                {
                    "name": "PII",
                    "provider": "system",
                    "description": "Personally identifiable information.",
                    "mutually_exclusive": True,
                    "allowed_on": ["table", "column"],
                    "options": [{"name": "Sensitive", "fqn": "PII.Sensitive", "description": "Sensitive personal data."}],
                }
            ],
        )
        table = result["tables"][0]
        self.assertEqual(table["classification_tags"], ["Tier.Tier1"])
        self.assertEqual(table["columns"][0]["classification_tags"], ["PII.Sensitive"])

    def test_build_source_system_document_uses_llm_for_missing_classification_tags(self):
        fake_engine = types.SimpleNamespace(dialect=types.SimpleNamespace(name="postgresql"))

        class _FakeGenerator:
            def is_available(self):
                return True

            def generate_column_description(self, **kwargs):
                return "Customer email address."

            def generate_table_description(self, **kwargs):
                return "Customer records."

        with ExitStack() as stack:
            stack.enter_context(patch.object(MODULE, "get_engine", return_value=fake_engine))
            stack.enter_context(patch.object(MODULE, "get_adapter", return_value=None))
            stack.enter_context(
                patch.object(
                    MODULE,
                    "fetch_schema_metadata",
                    return_value={
                        "tables": ["customers"],
                        "columns": {"customers": [{"name": "email", "type": "text", "nullable": True, "is_incremental": False}]},
                        "primary_keys": {"customers": []},
                        "foreign_keys": {"customers": []},
                    },
                )
            )
            stack.enter_context(patch.object(MODULE, "parse_connection_info", return_value={"driver": "postgresql", "database": "demo"}))
            stack.enter_context(patch.object(MODULE, "fetch_database_timezone", return_value="UTC"))
            stack.enter_context(patch.object(MODULE, "fetch_row_counts", return_value={"customers": 10}))
            stack.enter_context(
                patch.object(
                    MODULE,
                    "fetch_openmetadata_classification_catalog",
                    return_value=[
                        {
                            "name": "PII",
                            "provider": "system",
                            "description": "Personally identifiable information.",
                            "mutually_exclusive": True,
                            "allowed_on": ["table", "column"],
                            "options": [{"name": "Sensitive", "fqn": "PII.Sensitive", "description": "Sensitive personal data."}],
                        }
                    ],
                )
            )
            stack.enter_context(
                patch.object(
                    MODULE,
                    "_fetch_openmetadata_glossary_assignments",
                    return_value=(
                        {
                            "customers": {
                                "glossary_terms": [],
                                "classification_tags": [],
                                "column_glossary_terms": {"email": []},
                                "column_classification_tags": {"email": []},
                            }
                        },
                        {
                            "enabled": True,
                            "configured": True,
                            "database_service_name": "svc",
                            "schema": "public",
                            "match_strategy": "",
                            "matched_tables": 1,
                            "unmatched_tables": 0,
                            "error": "",
                        },
                    ),
                )
            )
            stack.enter_context(patch.object(MODULE, "_collect_projection_inputs"))
            stack.enter_context(patch.object(MODULE, "_projection_lookup", return_value={}))
            stack.enter_context(patch.object(MODULE, "_direct_history_projection_lookup", return_value={}))
            stack.enter_context(
                patch.object(
                    MODULE,
                    "fetch_sample_rows",
                    return_value=(["email"], [("alex@example.com",), ("sam@example.com",)]),
                )
            )
            stack.enter_context(patch.object(MODULE, "detect_partition_columns", return_value=([], "exact")))
            stack.enter_context(patch.object(MODULE, "detect_incremental_columns", return_value=[]))
            stack.enter_context(patch.object(MODULE, "fetch_column_statistics", return_value={}))
            stack.enter_context(patch.object(MODULE, "detect_join_candidates", return_value=[]))
            stack.enter_context(patch.object(MODULE, "_apply_concept_classification", return_value={"concepts": []}))
            stack.enter_context(patch.object(MODULE, "_build_description_generator", return_value=_FakeGenerator()))
            stack.enter_context(patch.object(MODULE, "fetch_openmetadata_glossary_terms", return_value=[]))
            assign_table = stack.enter_context(
                patch.object(MODULE.AzureClassificationTagAssigner, "assign_table", return_value=["PII.Sensitive"])
            )
            assign_column = stack.enter_context(
                patch.object(MODULE.AzureClassificationTagAssigner, "assign_column", return_value=["PII.Sensitive"])
            )
            result = MODULE.build_source_system_document(
                "postgresql://user:pass@localhost:5432/demo",
                schema="public",
                config={"exclude_schemas": [], "exclude_tables": [], "max_row_limit": None},
            )

        assign_table.assert_called_once()
        assign_column.assert_called_once()
        table = result["tables"][0]
        self.assertEqual(table["classification_tags"], ["PII.Sensitive"])
        self.assertEqual(table["columns"][0]["classification_tags"], ["PII.Sensitive"])

    def test_build_source_system_document_uses_azure_for_missing_descriptions(self):
        fake_engine = types.SimpleNamespace(dialect=types.SimpleNamespace(name="postgresql"))

        class _FakeGenerator:
            def __init__(self):
                self.column_sample_rows = None

            def is_available(self):
                return True

            def generate_column_description(self, **kwargs):
                self.column_sample_rows = kwargs["prompt_sample_rows"]
                return "Customer email address used for notifications."

            def generate_table_description(self, **kwargs):
                return "Customer master records including contact details."

        fake_generator = _FakeGenerator()

        with patch.object(MODULE, "get_engine", return_value=fake_engine), \
             patch.object(MODULE, "get_adapter", return_value=None), \
             patch.object(
                 MODULE,
                 "fetch_schema_metadata",
                 return_value={
                     "tables": ["customers"],
                     "columns": {
                         "customers": [
                             {"name": "email", "type": "text", "nullable": True, "is_incremental": False},
                         ]
                     },
                     "primary_keys": {"customers": []},
                     "foreign_keys": {"customers": []},
                 },
             ), \
             patch.object(MODULE, "parse_connection_info", return_value={"driver": "postgresql"}), \
             patch.object(MODULE, "fetch_database_timezone", return_value="UTC"), \
             patch.object(MODULE, "fetch_row_counts", return_value={"customers": 10}), \
             patch.object(MODULE, "_collect_projection_inputs"), \
             patch.object(MODULE, "_projection_lookup", return_value={}), \
             patch.object(MODULE, "_direct_history_projection_lookup", return_value={}), \
             patch.object(
                 MODULE,
                 "fetch_sample_rows",
                 return_value=(
                     ["email", "status"],
                     [
                         ("alex@example.com", "active"),
                         ("sam@example.com", "inactive"),
                         ("pat@example.com", "active"),
                         ("lee@example.com", "active"),
                     ],
                 ),
             ), \
             patch.object(MODULE, "detect_partition_columns", return_value=([], "exact")), \
             patch.object(MODULE, "detect_incremental_columns", return_value=[]), \
             patch.object(MODULE, "fetch_column_statistics", return_value={}), \
             patch.object(MODULE, "detect_join_candidates", return_value=[]), \
             patch.object(MODULE, "_apply_concept_classification", return_value={"concepts": []}), \
             patch.object(MODULE, "_build_description_generator", return_value=fake_generator), \
             patch.object(
                 MODULE,
                 "_load_system_description_config",
                 return_value={"system_description": "Retail CRM", "config_path": "unused.json"},
             ):
            result = MODULE.build_source_system_document(
                "postgresql://user:pass@localhost:5432/demo",
                schema="public",
                config={"exclude_schemas": [], "exclude_tables": [], "max_row_limit": None},
            )

        table = result["tables"][0]
        self.assertEqual(table["columns"][0]["column_description"], "Customer email address used for notifications.")
        self.assertEqual(table["table_description"], "Customer master records including contact details.")
        self.assertEqual(result["source_system_context"]["system_description"], "Retail CRM")
        self.assertEqual(len(fake_generator.column_sample_rows), 3)
        self.assertEqual(fake_generator.column_sample_rows[0]["email"], "alex@example.com")

    def test_build_source_system_document_falls_back_when_azure_generation_fails(self):
        fake_engine = types.SimpleNamespace(dialect=types.SimpleNamespace(name="postgresql"))

        class _FailingGenerator:
            def is_available(self):
                return True

            def generate_column_description(self, **kwargs):
                raise RuntimeError("Azure unavailable")

            def generate_table_description(self, **kwargs):
                raise RuntimeError("Azure unavailable")

        with patch.object(MODULE, "get_engine", return_value=fake_engine), \
             patch.object(MODULE, "get_adapter", return_value=None), \
             patch.object(
                 MODULE,
                 "fetch_schema_metadata",
                 return_value={
                     "tables": ["customers"],
                     "columns": {
                         "customers": [
                             {"name": "customer_id", "type": "integer", "nullable": False, "is_incremental": False},
                             {"name": "email", "type": "text", "nullable": True, "is_incremental": False},
                         ]
                     },
                     "primary_keys": {"customers": ["customer_id"]},
                     "foreign_keys": {"customers": []},
                 },
             ), \
             patch.object(MODULE, "parse_connection_info", return_value={"driver": "postgresql"}), \
             patch.object(MODULE, "fetch_database_timezone", return_value="UTC"), \
             patch.object(MODULE, "fetch_row_counts", return_value={"customers": 10}), \
             patch.object(MODULE, "_collect_projection_inputs"), \
             patch.object(MODULE, "_projection_lookup", return_value={}), \
             patch.object(MODULE, "_direct_history_projection_lookup", return_value={}), \
             patch.object(MODULE, "fetch_sample_rows", return_value=(["customer_id", "email"], [])), \
             patch.object(MODULE, "detect_partition_columns", return_value=([], "exact")), \
             patch.object(MODULE, "detect_incremental_columns", return_value=[]), \
             patch.object(MODULE, "fetch_column_statistics", return_value={}), \
             patch.object(MODULE, "detect_join_candidates", return_value=[]), \
             patch.object(MODULE, "_apply_concept_classification", return_value={"concepts": []}), \
             patch.object(MODULE, "_build_description_generator", return_value=_FailingGenerator()), \
             patch.object(
                 MODULE,
                 "_load_system_description_config",
                 return_value={"system_description": "Retail CRM", "config_path": "unused.json"},
             ):
            result = MODULE.build_source_system_document(
                "postgresql://user:pass@localhost:5432/demo",
                schema="public",
                config={"exclude_schemas": [], "exclude_tables": [], "max_row_limit": None},
            )

        table = result["tables"][0]
        self.assertEqual(table["table_description"], "Stores customer records including email.")
        self.assertEqual(table["columns"][0]["column_description"], "Primary key for the customer record.")
        self.assertEqual(table["columns"][1]["column_description"], "Contact value for the customer record.")

    def test_build_source_system_document_can_leave_missing_descriptions_blank(self):
        fake_engine = types.SimpleNamespace(dialect=types.SimpleNamespace(name="postgresql"))

        with patch.object(MODULE, "get_engine", return_value=fake_engine), \
             patch.object(MODULE, "get_adapter", return_value=None), \
             patch.object(
                 MODULE,
                 "fetch_schema_metadata",
                 return_value={
                     "tables": ["customers"],
                     "columns": {"customers": [{"name": "email", "type": "text", "nullable": True, "is_incremental": False}]},
                     "primary_keys": {"customers": []},
                     "foreign_keys": {"customers": []},
                 },
             ), \
             patch.object(MODULE, "parse_connection_info", return_value={"driver": "postgresql"}), \
             patch.object(MODULE, "fetch_database_timezone", return_value="UTC"), \
             patch.object(MODULE, "fetch_row_counts", return_value={"customers": 10}), \
             patch.object(MODULE, "_collect_projection_inputs"), \
             patch.object(MODULE, "_projection_lookup", return_value={}), \
             patch.object(MODULE, "_direct_history_projection_lookup", return_value={}), \
             patch.object(MODULE, "fetch_sample_rows", return_value=(["email"], [])), \
             patch.object(MODULE, "detect_partition_columns", return_value=([], "exact")), \
             patch.object(MODULE, "detect_incremental_columns", return_value=[]), \
             patch.object(MODULE, "fetch_column_statistics", return_value={}), \
             patch.object(MODULE, "detect_join_candidates", return_value=[]), \
             patch.object(MODULE, "_apply_concept_classification", return_value={"concepts": []}):
            result = MODULE.build_source_system_document(
                "postgresql://user:pass@localhost:5432/demo",
                schema="public",
                config={"exclude_schemas": [], "exclude_tables": [], "max_row_limit": None},
                generate_missing_descriptions=False,
            )

        table = result["tables"][0]
        self.assertEqual(table["table_description"], "")
        self.assertEqual(table["columns"][0]["column_description"], "")

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
            result = MODULE.build_source_system_document(
                "postgresql://user:pass@localhost:5432/demo",
                schema="public",
                config={"exclude_schemas": [], "exclude_tables": [], "max_row_limit": None},
            )

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
            result = MODULE.build_source_system_document(
                "postgresql://user:pass@localhost:5432/demo",
                schema="public",
                config={"exclude_schemas": [], "exclude_tables": [], "max_row_limit": None},
            )

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
