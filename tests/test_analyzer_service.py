import os
import types
import unittest
from unittest.mock import patch

from api import analyzer_service


class AnalyzerServiceTests(unittest.TestCase):
    def setUp(self):
        self._env_backup = dict(os.environ)
        os.environ["DATABASE_URL"] = "postgresql://user:pass@localhost:5432/testdb"
        analyzer_service._analyzer_cache.clear()
        analyzer_service._analyzer_module = None

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self._env_backup)
        analyzer_service._analyzer_cache.clear()
        analyzer_service._analyzer_module = None

    def test_get_analyzer_document_uses_cache_for_same_schema(self):
        fake_module = types.SimpleNamespace()
        fake_module.build_source_system_document = lambda database_url, schema, include_sample_data=False, dialect_override=None: {
            "metadata": {"schema_filter": schema, "database_url": database_url},
            "tables": [],
        }

        with patch("api.analyzer_service._load_analyzer_module", return_value=fake_module) as mocked_loader:
            first = analyzer_service.get_analyzer_document("public")
            second = analyzer_service.get_analyzer_document("public")

        self.assertEqual(first, second)
        self.assertEqual(first["metadata"]["schema_filter"], "public")
        mocked_loader.assert_called_once()

    def test_get_analyzer_document_uses_separate_cache_entries_per_schema(self):
        calls = []

        def build(database_url, schema, include_sample_data=False, dialect_override=None):
            calls.append(schema)
            return {"metadata": {"schema_filter": schema, "database_url": database_url}, "tables": []}

        fake_module = types.SimpleNamespace(build_source_system_document=build)

        with patch("api.analyzer_service._load_analyzer_module", return_value=fake_module):
            public_doc = analyzer_service.get_analyzer_document("public")
            app_doc = analyzer_service.get_analyzer_document("app")

        self.assertEqual(public_doc["metadata"]["schema_filter"], "public")
        self.assertEqual(app_doc["metadata"]["schema_filter"], "app")
        self.assertEqual(calls, ["public", "app"])

    def test_get_analyzer_document_raises_for_empty_schema(self):
        fake_module = types.SimpleNamespace(
            build_source_system_document=lambda database_url, schema, include_sample_data=False, dialect_override=None: {
                "error": "No tables found"
            }
        )

        with patch("api.analyzer_service._load_analyzer_module", return_value=fake_module):
            with self.assertRaises(analyzer_service.AnalyzerSchemaError) as ctx:
                analyzer_service.get_analyzer_document("public")

        self.assertEqual(str(ctx.exception), "No tables found for schema 'public'")


if __name__ == "__main__":
    unittest.main()
