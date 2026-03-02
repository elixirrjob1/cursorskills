import importlib.util
import sys
import types
import unittest
from pathlib import Path


MODULE_PATH = Path("/home/filip/Projects/skills/.cursor/skills/source-system-analyser/scripts/source_system_analyzer.py")
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
databases = types.ModuleType("databases")
databases.get_adapter = lambda *args, **kwargs: None
databases.get_adapter_for_engine = lambda *args, **kwargs: None
sys.modules.setdefault("sqlalchemy", sqlalchemy)
sys.modules.setdefault("sqlalchemy.engine", sqlalchemy_engine)
sys.modules.setdefault("sqlalchemy.exc", sqlalchemy_exc)
sys.modules.setdefault("databases", databases)
SPEC = importlib.util.spec_from_file_location("source_system_analyzer", MODULE_PATH)
analyzer = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(analyzer)


class ClassificationUpgradeTests(unittest.TestCase):
    def test_alias_normalization_collapses_email_variants(self):
        self.assertEqual(analyzer._normalize_column_alias("email"), "email")
        self.assertEqual(analyzer._normalize_column_alias("email_address"), "email")
        self.assertEqual(analyzer._normalize_column_alias("primary_email"), "email")
        self.assertEqual(analyzer._normalize_column_alias("email_addr"), "email")

    def test_value_detectors(self):
        self.assertTrue(analyzer._is_email_value("user@example.com"))
        self.assertTrue(analyzer._is_phone_value("+1 (555) 123-4567"))
        self.assertTrue(analyzer._is_ip_value("10.0.0.1"))
        self.assertTrue(analyzer._is_timestamp_value("2026-01-01T10:11:12Z"))
        self.assertTrue(analyzer._is_date_only_value("2026-01-01"))

    def test_cross_table_consensus_promotes_email_variants(self):
        tables = [
            {
                "table": "customers",
                "schema": "public",
                "row_count": 10,
                "field_classifications": {"email": "contact"},
                "sensitive_fields": {"email": "pii_contact"},
                "columns": [
                    {
                        "name": "email",
                        "type": "varchar(255)",
                        "nullable": False,
                        "semantic_class": "contact",
                        "_sample_values": ["a@example.com", "b@example.com"],
                    }
                ],
            },
            {
                "table": "users",
                "schema": "public",
                "row_count": 10,
                "field_classifications": {},
                "sensitive_fields": {},
                "columns": [
                    {
                        "name": "email_address",
                        "type": "varchar(255)",
                        "nullable": False,
                        "semantic_class": None,
                        "_sample_values": ["c@example.com", "d@example.com"],
                    }
                ],
            },
            {
                "table": "contacts",
                "schema": "public",
                "row_count": 10,
                "field_classifications": {},
                "sensitive_fields": {},
                "columns": [
                    {
                        "name": "primary_email",
                        "type": "varchar(255)",
                        "nullable": False,
                        "semantic_class": None,
                        "_sample_values": ["e@example.com", "f@example.com"],
                    }
                ],
            },
        ]

        registry = analyzer._apply_concept_classification(tables)

        for table in tables:
            column = table["columns"][0]
            self.assertEqual(column["concept_id"], "contact.email")
            self.assertGreaterEqual(column["concept_confidence"], 0.85)
            self.assertEqual(column["concept_alias_group"], "email")
            self.assertNotIn("_sample_values", column)

        concepts = {concept["concept_id"]: concept for concept in registry["concepts"]}
        self.assertIn("contact.email", concepts)
        self.assertEqual(concepts["contact.email"]["column_count"], 3)
        self.assertEqual(concepts["contact.email"]["table_count"], 3)

    def test_generic_low_signal_field_stays_unclassified(self):
        tables = [
            {
                "table": "misc_data",
                "schema": "public",
                "row_count": 3,
                "field_classifications": {},
                "sensitive_fields": {},
                "columns": [
                    {
                        "name": "value",
                        "type": "varchar(255)",
                        "nullable": True,
                        "semantic_class": None,
                        "_sample_values": ["foo", "bar"],
                    }
                ],
            }
        ]

        analyzer._apply_concept_classification(tables)
        column = tables[0]["columns"][0]
        self.assertIsNone(column["concept_id"])
        self.assertLess(column["concept_confidence"], 0.55)

    def test_ip_substring_does_not_match_description(self):
        tables = [
            {
                "table": "products",
                "schema": "public",
                "row_count": 3,
                "field_classifications": {},
                "sensitive_fields": {},
                "join_candidates": [],
                "foreign_keys": [],
                "columns": [
                    {
                        "name": "product_description",
                        "type": "varchar(255)",
                        "nullable": True,
                        "semantic_class": None,
                        "_sample_values": ["Widget for kitchens", "Blue frame"],
                    }
                ],
            }
        ]
        analyzer._apply_concept_classification(tables)
        column = tables[0]["columns"][0]
        self.assertNotEqual(column["concept_id"], "network.ip_address")

    def test_state_does_not_become_status_but_role_can_be_type(self):
        tables = [
            {
                "table": "stores",
                "schema": "public",
                "row_count": 3,
                "field_classifications": {"state": "categorical", "role": "categorical"},
                "sensitive_fields": {},
                "join_candidates": [],
                "foreign_keys": [],
                "columns": [
                    {
                        "name": "state",
                        "type": "varchar(20)",
                        "nullable": False,
                        "semantic_class": None,
                        "_sample_values": ["CA", "NY", "TX"],
                    },
                    {
                        "name": "role",
                        "type": "varchar(20)",
                        "nullable": False,
                        "semantic_class": None,
                        "_sample_values": ["manager", "cashier", "manager"],
                    },
                ],
            }
        ]
        analyzer._apply_concept_classification(tables)
        self.assertNotEqual(tables[0]["columns"][0]["concept_id"], "entity.status")
        self.assertEqual(tables[0]["columns"][1]["concept_id"], "entity.type")

    def test_join_candidate_prefers_foreign_key_over_product_code(self):
        tables = [
            {
                "table": "inventory",
                "schema": "public",
                "row_count": 3,
                "field_classifications": {},
                "sensitive_fields": {},
                "join_candidates": [{"column": "product_id", "target_table": "products", "target_column": "product_id", "confidence": "high"}],
                "foreign_keys": [{"column": "product_id", "references": "products.product_id"}],
                "columns": [
                    {
                        "name": "product_id",
                        "type": "bigint",
                        "nullable": False,
                        "semantic_class": "product_identifier",
                        "_sample_values": [1, 2, 3],
                    }
                ],
            }
        ]
        analyzer._apply_concept_classification(tables)
        column = tables[0]["columns"][0]
        self.assertEqual(column["concept_id"], "identifier.foreign_key")

    def test_last_restocked_at_maps_to_temporal_event_time(self):
        tables = [
            {
                "table": "inventory",
                "schema": "public",
                "row_count": 3,
                "field_classifications": {"last_restocked_at": "temporal"},
                "sensitive_fields": {},
                "join_candidates": [],
                "foreign_keys": [],
                "columns": [
                    {
                        "name": "last_restocked_at",
                        "type": "datetime2",
                        "nullable": True,
                        "semantic_class": "timestamp",
                        "_sample_values": ["2026-03-01 10:00:00", "2026-03-02 12:00:00"],
                    }
                ],
            }
        ]
        analyzer._apply_concept_classification(tables)
        column = tables[0]["columns"][0]
        self.assertEqual(column["concept_id"], "temporal.event_time")


if __name__ == "__main__":
    unittest.main()
