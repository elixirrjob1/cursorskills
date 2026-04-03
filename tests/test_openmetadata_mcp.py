import importlib.util
import json
import os
import sys
import unittest
from pathlib import Path
from unittest import mock


SCRIPT_PATH = Path("/home/fillip/projec/cursorskills/tools/openmetadata_mcp/server.py")
VENDOR_PATH = Path("/home/fillip/projec/cursorskills/tools/fivetran_mcp/vendor")
if str(VENDOR_PATH) not in sys.path:
    sys.path.insert(0, str(VENDOR_PATH))


def _load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


openmetadata_module = _load_module("openmetadata_mcp_server", SCRIPT_PATH)


class OpenMetadataMcpTests(unittest.TestCase):
    def test_normalize_base_url_accepts_plain_host(self):
        self.assertEqual(
            openmetadata_module._normalize_base_url("http://52.255.209.74:8585/"),
            "http://52.255.209.74:8585/api",
        )

    def test_normalize_base_url_keeps_api_suffix(self):
        self.assertEqual(
            openmetadata_module._normalize_base_url("http://52.255.209.74:8585/api"),
            "http://52.255.209.74:8585/api",
        )

    def test_extract_token_handles_nested_payload(self):
        payload = {"data": {"jwtToken": "abc123"}}
        self.assertEqual(openmetadata_module._extract_token(payload), "abc123")

    def test_glossary_term_payload_appends_review_status(self):
        payload = openmetadata_module._glossary_term_payload(
            glossary_fqn="BusinessGlossary",
            name="Customer",
            description="Canonical customer term.",
            synonyms=["Client"],
            notes="Reviewed by governance.",
            status="approved",
        )
        self.assertEqual(payload["glossary"], "BusinessGlossary")
        self.assertEqual(payload["synonyms"], ["Client"])
        self.assertIn("Canonical customer term.", payload["description"])
        self.assertIn("Reviewed by governance.", payload["description"])
        self.assertIn("Review status: approved.", payload["description"])

    def test_service_connection_payload_sets_type_and_validates_known_service(self):
        payload = openmetadata_module._service_connection_payload(
            "Snowflake",
            {
                "account": "acct",
                "username": "user",
                "password": "pw",
                "warehouse": "wh",
            },
        )
        self.assertEqual(payload["config"]["type"], "Snowflake")
        self.assertEqual(payload["config"]["account"], "acct")

    def test_service_connection_payload_rejects_missing_required_fields(self):
        with self.assertRaises(RuntimeError) as ctx:
            openmetadata_module._service_connection_payload(
                "Postgres",
                {"username": "user", "password": "pw"},
            )
        self.assertIn("hostPort", str(ctx.exception))
        self.assertIn("database", str(ctx.exception))

    def test_ingestion_pipeline_payload_uses_database_metadata_defaults(self):
        service_ref = {
            "id": "svc-id",
            "type": "databaseService",
            "name": "retail-postgres",
        }
        payload = openmetadata_module._ingestion_pipeline_payload(
            name="retail-sync",
            service_ref=service_ref,
            source_config={},
            airflow_config={},
        )
        self.assertEqual(payload["pipelineType"], "metadata")
        self.assertEqual(payload["service"]["id"], "svc-id")
        self.assertEqual(payload["sourceConfig"]["config"]["type"], "DatabaseMetadata")
        self.assertFalse(payload["airflowConfig"]["pausePipeline"])

    def test_merge_tag_labels_is_idempotent(self):
        merged = openmetadata_module._merge_tag_labels(
            [
                {"tagFQN": "PII.Sensitive", "source": "Classification"},
                {"tagFQN": "Business.Customer", "source": "Glossary"},
            ],
            [
                {"tagFQN": "PII.Sensitive", "source": "Classification"},
                {"tagFQN": "Business.Customer", "source": "Glossary"},
                {"tagFQN": "Finance.Amount", "source": "Glossary"},
            ],
        )
        self.assertEqual(
            merged,
            [
                {"tagFQN": "PII.Sensitive", "source": "Classification"},
                {"tagFQN": "Business.Customer", "source": "Glossary"},
                {"tagFQN": "Finance.Amount", "source": "Glossary"},
            ],
        )

    @mock.patch.object(openmetadata_module, "_request")
    def test_list_tables_requests_columns_and_preserves_descriptions(self, mock_request):
        mock_request.return_value = {
            "data": [
                {
                    "name": "customers",
                    "fullyQualifiedName": "svc.db.schema.customers",
                    "description": "Customer master table.",
                    "columns": [
                        {"name": "customer_id", "description": "Primary key."},
                        {"name": "email"},
                    ],
                }
            ]
        }

        result = json.loads(openmetadata_module.list_tables("svc.db.schema", limit=25))

        self.assertEqual(result["count"], 1)
        self.assertEqual(result["items"][0]["description"], "Customer master table.")
        self.assertEqual(result["items"][0]["columns"][0]["description"], "Primary key.")
        mock_request.assert_called_once_with(
            "GET",
            "tables",
            params={
                "limit": 25,
                "databaseSchema": "svc.db.schema",
                "fields": openmetadata_module.TABLE_ENTITY_FIELDS,
            },
        )

    @mock.patch.object(openmetadata_module, "_patch_json_patch")
    @mock.patch.object(openmetadata_module, "_get_table_entity")
    def test_assign_tags_to_column_merges_without_duplicates(
        self, mock_get_table_entity, mock_patch_json_patch
    ):
        mock_get_table_entity.return_value = {
            "id": "tbl-id",
            "fullyQualifiedName": "svc.db.schema.customers",
            "columns": [
                {
                    "name": "email",
                    "tags": [
                        {"tagFQN": "PII.Sensitive", "source": "Classification"},
                    ],
                }
            ],
            "tags": [],
        }

        def _patch_side_effect(endpoint: str, patch_ops: list) -> dict:
            self.assertEqual(endpoint, "tables/tbl-id")
            self.assertEqual(patch_ops[0]["path"], "/columns/0/tags")
            return {
                "fullyQualifiedName": "svc.db.schema.customers",
                "columns": [
                    {
                        "name": "email",
                        "tags": [
                            {"tagFQN": "PII.Sensitive", "source": "Classification"},
                            {
                                "tagFQN": "Retail.Customer.Email",
                                "source": "Classification",
                                "labelType": "Manual",
                                "state": "Confirmed",
                            },
                        ],
                    }
                ],
            }

        mock_patch_json_patch.side_effect = _patch_side_effect

        result = openmetadata_module._assign_column_tags(
            table_fqn="svc.db.schema.customers",
            column_name="email",
            tag_fqns=["PII.Sensitive", "Retail.Customer.Email"],
            source="Classification",
        )

        self.assertEqual(result["column"], "email")
        self.assertEqual(
            result["tags"],
            [
                {"tagFQN": "PII.Sensitive", "source": "Classification"},
                {
                    "tagFQN": "Retail.Customer.Email",
                    "source": "Classification",
                    "labelType": "Manual",
                    "state": "Confirmed",
                },
            ],
        )

    @mock.patch.object(openmetadata_module, "_patch_json_patch")
    @mock.patch.object(openmetadata_module, "_get_table_entity")
    def test_assign_glossary_term_to_table_uses_glossary_source(
        self, mock_get_table_entity, mock_patch_json_patch
    ):
        mock_get_table_entity.return_value = {
            "id": "tbl-id",
            "fullyQualifiedName": "svc.db.schema.orders",
            "columns": [],
            "tags": [],
        }
        mock_patch_json_patch.return_value = {
            "fullyQualifiedName": "svc.db.schema.orders",
            "tags": [
                {
                    "tagFQN": "Retail.Orders.TotalAmount",
                    "source": "Glossary",
                    "labelType": "Manual",
                    "state": "Confirmed",
                }
            ],
        }

        result = json.loads(
            openmetadata_module.assign_glossary_term_to_table(
                "svc.db.schema.orders",
                "Retail.Orders.TotalAmount",
            )
        )

        self.assertEqual(result["table"], "svc.db.schema.orders")
        self.assertEqual(
            result["tags"],
            [
                {
                    "tagFQN": "Retail.Orders.TotalAmount",
                    "source": "Glossary",
                    "labelType": "Manual",
                    "state": "Confirmed",
                }
            ],
        )

    @mock.patch.dict(
        os.environ,
        {
            "OPENMETADATA_BASE_URL": "http://example:8585",
            "OPENMETADATA_EMAIL": "admin@example.com",
            "OPENMETADATA_PASSWORD": "secret",
        },
        clear=False,
    )
    @mock.patch("requests.post")
    def test_login_tries_base64_first(self, mock_post):
        response = mock.Mock()
        response.status_code = 200
        response.content = b'{"accessToken":"jwt-value"}'
        response.json.return_value = {"accessToken": "jwt-value"}
        response.raise_for_status.return_value = None
        mock_post.return_value = response
        openmetadata_module._TOKEN_CACHE["token"] = None

        token = openmetadata_module._login()

        self.assertEqual(token, "jwt-value")
        called_payload = mock_post.call_args.kwargs["json"]
        self.assertEqual(called_payload["email"], "admin@example.com")
        self.assertNotEqual(called_payload["password"], "secret")


if __name__ == "__main__":
    unittest.main()
