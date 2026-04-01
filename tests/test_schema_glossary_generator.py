import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

from openpyxl import load_workbook


SCRIPT_PATH = Path("/home/fillip/projec/cursorskills/.cursor/skills/schema-glossary-generator/scripts/generate_glossary.py")


def _load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


glossary_module = _load_module("generate_glossary", SCRIPT_PATH)


class SchemaGlossaryGeneratorTests(unittest.TestCase):
    def setUp(self):
        self.payload = {
            "concept_registry": {
                "concepts": [
                    {
                        "concept_id": "contact.email",
                        "avg_confidence": 1.0,
                        "table_count": 2,
                        "alias_groups": ["email"],
                        "sample_columns": ["customers.email", "suppliers.email"],
                        "signals": ["name", "values"],
                    }
                ]
            },
            "tables": [
                {
                    "table": "customers",
                    "table_description": "Customer master records for the retail business.",
                    "foreign_keys": [],
                    "columns": [
                        {
                            "name": "customer_id",
                            "column_description": "Unique identifier for each customer.",
                            "concept_confidence": 0.9,
                            "semantic_class": "customer_identifier",
                            "concept_id": "identifier.foreign_key",
                            "concept_alias_group": "id",
                        },
                        {
                            "name": "email",
                            "column_description": "Stores the customer's email address.",
                            "concept_confidence": 1.0,
                            "semantic_class": "contact",
                            "concept_id": "contact.email",
                            "concept_alias_group": "email",
                        },
                        {
                            "name": "created_at",
                            "column_description": "Timestamp when the customer record was created.",
                            "concept_confidence": 1.0,
                            "semantic_class": "timestamp",
                            "concept_id": "temporal.created_at",
                            "concept_alias_group": "created_at",
                        },
                    ],
                },
                {
                    "table": "sales_orders",
                    "table_description": "Sales headers including assigned sales representative relationship.",
                    "foreign_keys": [
                        {"column": "customer_id", "references": "customers.customer_id"},
                        {"column": "store_id", "references": "stores.store_id"},
                    ],
                    "columns": [
                        {
                            "name": "status",
                            "column_description": "Indicates the current state of a sales order.",
                            "concept_confidence": 1.0,
                            "semantic_class": "category",
                            "concept_id": "entity.status",
                            "concept_alias_group": "status",
                        },
                        {
                            "name": "total_amount",
                            "column_description": "Represents the total monetary amount for a sales order.",
                            "concept_confidence": 1.0,
                            "semantic_class": "currency_amount",
                            "concept_id": "finance.currency_amount",
                            "concept_alias_group": "amount",
                        },
                    ],
                },
            ],
        }

    def test_build_glossary_merges_concepts_and_tables(self):
        glossary = glossary_module.build_glossary(self.payload, "/tmp/schema.json")
        terms = {entry["term"]: entry for entry in glossary["entries"]}

        self.assertIn("Customer", terms)
        self.assertIn("Sales Order", terms)
        self.assertIn("Email", terms)
        self.assertIn("Status", terms)
        self.assertIn("Total Amount", terms)

        email = terms["Email"]
        self.assertEqual(email["term_type"], "business_attribute")
        self.assertIn("customers.email", email["source_refs"])
        self.assertEqual(email["status"], "confirmed_from_schema")

        sales_order = terms["Sales Order"]
        self.assertEqual(sales_order["term_type"], "business_process")
        self.assertIn("sales_orders", sales_order["source_tables"])

    def test_write_glossary_excel_creates_expected_tabs(self):
        glossary = glossary_module.build_glossary(self.payload, "/tmp/schema.json")
        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "schema_glossary.xlsx"
            glossary_module.write_glossary_excel(glossary, output)
            wb = load_workbook(output, data_only=True)
            self.assertEqual(wb.sheetnames, ["Glossary", "RunMetadata"])
            glossary_rows = list(wb["Glossary"].iter_rows(values_only=True))
            self.assertEqual(glossary_rows[0][0], "term")
            self.assertGreater(len(glossary_rows), 2)

    def test_output_paths_write_beside_input(self):
        input_path = Path("/tmp/schema_azure_mssql_dbo.json")
        json_output, excel_output = glossary_module.output_paths(input_path)
        self.assertEqual(json_output.name, "schema_azure_mssql_dbo_glossary.json")
        self.assertEqual(excel_output.name, "schema_azure_mssql_dbo_glossary.xlsx")

    def test_run_writes_json_and_excel_outputs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            input_path = Path(tmpdir) / "schema_sample.json"
            input_path.write_text(json.dumps(self.payload), encoding="utf-8")
            json_output, excel_output, glossary = glossary_module.run(input_path)
            self.assertTrue(json_output.exists())
            self.assertTrue(excel_output.exists())
            written = json.loads(json_output.read_text(encoding="utf-8"))
            self.assertEqual(written["metadata"]["glossary_entry_count"], glossary["metadata"]["glossary_entry_count"])


if __name__ == "__main__":
    unittest.main()
