import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


def _load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


BUILD_PATH = Path("/home/fillip/projec/cursorskills/scripts/build_description_enrichment_checklist.py")
APPLY_PATH = Path("/home/fillip/projec/cursorskills/scripts/apply_description_enrichment.py")
build_module = _load_module("build_description_enrichment_checklist", BUILD_PATH)
apply_module = _load_module("apply_description_enrichment", APPLY_PATH)


class DescriptionEnrichmentScriptsTests(unittest.TestCase):
    def test_build_checklist_collects_missing_table_and_column_descriptions(self):
        payload = {
            "tables": [
                {
                    "schema": "dbo",
                    "table": "customers",
                    "table_description": "",
                    "columns": [
                        {"name": "customer_id", "column_description": ""},
                        {"name": "email", "column_description": "Email address"},
                    ],
                }
            ]
        }

        checklist = build_module.build_checklist(payload, "schema.json")

        self.assertEqual(checklist["summary"]["missing_table_descriptions"], 1)
        self.assertEqual(checklist["summary"]["missing_column_descriptions"], 1)
        self.assertEqual(checklist["items"][0]["item_id"], "column:dbo.customers.customer_id")
        self.assertEqual(checklist["items"][0]["phase"], "column_descriptions")
        self.assertEqual(checklist["items"][1]["item_id"], "table:dbo.customers")
        self.assertEqual(checklist["items"][1]["phase"], "table_description")
        self.assertEqual(checklist["items"][1]["depends_on_item_ids"], ["column:dbo.customers.customer_id"])

    def test_apply_checklist_merges_proposed_descriptions(self):
        payload = {
            "tables": [
                {
                    "schema": "dbo",
                    "table": "customers",
                    "table_description": "",
                    "columns": [
                        {"name": "customer_id", "column_description": ""},
                        {"name": "email", "column_description": "Existing"},
                    ],
                }
            ]
        }
        checklist = {
            "items": [
                {
                    "schema": "dbo",
                    "table": "customers",
                    "column": None,
                    "field": "table_description",
                    "proposed_description": "Customer master records.",
                },
                {
                    "schema": "dbo",
                    "table": "customers",
                    "column": "customer_id",
                    "field": "column_description",
                    "proposed_description": "Primary key for the customer record.",
                },
            ]
        }

        updated = apply_module.apply_checklist(payload, checklist)

        self.assertEqual(updated["tables"][0]["table_description"], "Customer master records.")
        self.assertEqual(updated["tables"][0]["columns"][0]["column_description"], "Primary key for the customer record.")
        self.assertEqual(updated["tables"][0]["columns"][1]["column_description"], "Existing")


if __name__ == "__main__":
    unittest.main()
