import os
import asyncio
import sys
import types
import unittest
from unittest.mock import patch

from fastapi import HTTPException

sqlalchemy = types.ModuleType("sqlalchemy")
sqlalchemy.create_engine = lambda *args, **kwargs: None
sqlalchemy.MetaData = object
sqlalchemy.Table = object
sqlalchemy.inspect = lambda *args, **kwargs: None
sqlalchemy.select = lambda *args, **kwargs: None
sqlalchemy.text = lambda value: value
sqlalchemy.func = types.SimpleNamespace(count=lambda *args, **kwargs: None)
sqlalchemy_engine = types.ModuleType("sqlalchemy.engine")
sqlalchemy_engine.Engine = object
sqlalchemy_exc = types.ModuleType("sqlalchemy.exc")
sqlalchemy_exc.SAWarning = Warning
sys.modules.setdefault("sqlalchemy", sqlalchemy)
sys.modules.setdefault("sqlalchemy.engine", sqlalchemy_engine)
sys.modules.setdefault("sqlalchemy.exc", sqlalchemy_exc)

from api.auth import require_bearer_token
from api import routes


class ApiAnalyzeEndpointTests(unittest.TestCase):
    def setUp(self):
        self._env_backup = dict(os.environ)
        os.environ["API_AUTH_TOKEN"] = "secret-token"

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self._env_backup)

    def test_analyze_requires_auth(self):
        with self.assertRaises(HTTPException) as ctx:
            asyncio.run(require_bearer_token(None))

        self.assertEqual(ctx.exception.status_code, 401)
        self.assertEqual(ctx.exception.detail, "Unauthorized")

    def test_analyze_returns_full_document(self):
        payload = {
            "metadata": {"schema_filter": "public", "total_findings": 2},
            "connection": {"driver": "postgresql"},
            "source_system_context": {"contacts": []},
            "concept_registry": {"concepts": []},
            "data_quality_summary": {"critical": 0, "warning": 1, "info": 1, "by_check": {}},
            "tables": [
                {
                    "table": "customers",
                    "schema": "public",
                    "classification_summary": {"concept_counts": {}, "low_confidence_columns": []},
                    "data_quality": {"findings": []},
                    "unit_summary": {"columns_with_units": 0, "columns_without_units": 1, "mixed_unit_groups": [], "unknown_unit_columns": []},
                    "columns": [
                        {
                            "name": "email",
                            "concept_id": "contact.email",
                            "concept_confidence": 0.91,
                            "concept_evidence": [],
                            "concept_sources": [],
                            "concept_alias_group": "email",
                        }
                    ],
                }
            ],
        }

        with patch("api.routes.analyzer_service.get_analyzer_document", return_value=payload) as mocked:
            response = asyncio.run(routes.analyze_schema(None, schema="public"))

        self.assertEqual(response, payload)
        mocked.assert_called_once_with("public")

    def test_config_returns_active_connection_details(self):
        fake_url = types.SimpleNamespace(host="db.example.com", port=1433, database="warehouse")
        fake_engine = types.SimpleNamespace(dialect=types.SimpleNamespace(name="mssql"), url=fake_url)

        with patch("api.routes.db.get_engine", return_value=fake_engine), patch.dict(os.environ, {"SCHEMA": "dbo"}, clear=False):
            response = asyncio.run(routes.get_config(None))

        self.assertEqual(
            response,
            {
                "default_schema": "dbo",
                "dialect": "mssql",
                "host": "db.example.com",
                "port": "1433",
                "database": "warehouse",
            },
        )

    def test_analyze_returns_not_found_for_empty_schema(self):
        with patch(
            "api.routes.analyzer_service.get_analyzer_document",
            side_effect=routes.analyzer_service.AnalyzerSchemaError("No tables found for schema 'public'"),
        ):
            with self.assertRaises(HTTPException) as ctx:
                asyncio.run(routes.analyze_schema(None, schema="public"))

        self.assertEqual(ctx.exception.status_code, 404)
        self.assertEqual(
            ctx.exception.detail,
            {"detail": "No tables found for schema 'public'", "schema": "public"},
        )

    def test_tables_returns_not_found_for_empty_schema(self):
        with patch("api.routes.db.get_tables_metadata", return_value=[]):
            with self.assertRaises(HTTPException) as ctx:
                asyncio.run(routes.list_tables(None, schema="public"))

        self.assertEqual(ctx.exception.status_code, 404)
        self.assertEqual(
            ctx.exception.detail,
            {"detail": "No tables found for schema 'public'", "schema": "public"},
        )

    def test_get_table_returns_schema_not_found_for_empty_schema(self):
        with patch("api.routes.db.get_table_metadata", return_value=None), patch(
            "api.routes.db.get_tables_metadata", return_value=[]
        ):
            with self.assertRaises(HTTPException) as ctx:
                asyncio.run(
                    routes.get_table(
                        "customers",
                        None,
                        limit=100,
                        offset=0,
                        schema="public",
                    )
                )

        self.assertEqual(ctx.exception.status_code, 404)
        self.assertEqual(
            ctx.exception.detail,
            {"detail": "No tables found for schema 'public'", "schema": "public"},
        )

    def test_existing_routes_remain_unchanged(self):
        table_list = [{"table": "customers", "schema": "public"}]
        table_meta = {"table": "customers", "schema": "public", "columns": []}
        table_rows = [{"id": 1, "name": "Alice"}]

        with patch("api.routes.db.get_tables_metadata", return_value=table_list), patch(
            "api.routes.db.get_table_metadata", return_value=table_meta
        ), patch("api.routes.db.resolve_table_name", return_value="customers"), patch(
            "api.routes.db.get_table_data", return_value=table_rows
        ):
            tables_response = asyncio.run(routes.list_tables(None, schema="public"))
            table_response = asyncio.run(
                routes.get_table(
                    "customers",
                    None,
                    limit=100,
                    offset=0,
                    schema="public",
                )
            )
        self.assertEqual(tables_response, {"schema": "public", "tables": table_list})
        self.assertEqual(
            table_response,
            {
                "schema": "public",
                "table": "customers",
                "metadata": table_meta,
                "data": table_rows,
            },
        )


if __name__ == "__main__":
    unittest.main()
