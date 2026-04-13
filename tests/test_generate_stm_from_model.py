import importlib.util
import sys
import unittest
from pathlib import Path
from unittest import mock


SCRIPT_PATH = Path("/home/fillip/projec/cursorskills/.cursor/skills/stm-from-data-model/scripts/generate_stm_from_model.py")


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


stm_module = _load_module("stm_from_data_model", SCRIPT_PATH)


class GenerateStmFromModelTests(unittest.TestCase):
    @mock.patch.dict(
        "os.environ",
        {
            "OPENMETADATA_BASE_URL": "http://example:8585",
            "OPENMETADATA_EMAIL": "admin@example.com",
            "OPENMETADATA_PASSWORD": "secret",
        },
        clear=False,
    )
    @mock.patch.object(stm_module, "_openmetadata_request")
    def test_fetch_openmetadata_glossary_definitions_uses_api_results(self, mock_request):
        mock_request.return_value = {
            "data": [
                {
                    "fullyQualifiedName": "RetailDomainGlossary.Supplier",
                    "name": "Supplier",
                    "description": "Supplier definition from OpenMetadata.",
                },
                {
                    "fullyQualifiedName": "RetailDomainGlossary.Product",
                    "displayName": "Product",
                    "description": "Product definition from OpenMetadata.",
                },
            ]
        }

        definitions = stm_module.fetch_openmetadata_glossary_definitions()

        self.assertEqual(
            definitions["RetailDomainGlossary.Supplier"].definition,
            "Supplier definition from OpenMetadata.",
        )
        self.assertEqual(
            definitions["RetailDomainGlossary.Product"].term_name,
            "Product",
        )
        mock_request.assert_called_once_with("GET", "glossaryTerms", params={"limit": 1000})

    def test_has_glossary_terms_detects_table_and_column_assignments(self):
        metadata = stm_module.AnalyzerMetadata(
            source_path=Path("schema.json"),
            tables={
                "suppliers": stm_module.AnalyzerTableMetadata(
                    glossary_terms=["RetailDomainGlossary.Supplier"],
                    columns={
                        "supplier_id": stm_module.AnalyzerColumnMetadata(glossary_terms=[]),
                    },
                )
            },
        )
        self.assertTrue(stm_module._has_glossary_terms(metadata))


if __name__ == "__main__":
    unittest.main()
