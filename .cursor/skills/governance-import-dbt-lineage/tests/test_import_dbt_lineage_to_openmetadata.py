import importlib.util
import pathlib
import sys
import unittest


SCRIPT_PATH = (
    pathlib.Path(__file__).resolve().parents[1]
    / "scripts"
    / "import_dbt_lineage_to_openmetadata.py"
)
SPEC = importlib.util.spec_from_file_location("lineage_importer", SCRIPT_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC is not None and SPEC.loader is not None
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class TestLineageImporter(unittest.TestCase):
    def test_is_view_model_detects_materialized_view(self) -> None:
        node = {
            "resource_type": "model",
            "name": "stg_customer",
            "config": {"materialized": "view"},
        }
        self.assertTrue(MODULE._is_view_model(node))

    def test_build_lineage_edges_flattens_views_by_default(self) -> None:
        manifest = {
            "nodes": {
                "model.test.vw_orders": {
                    "resource_type": "model",
                    "name": "vw_orders",
                    "database": "DB",
                    "schema": "STAGING",
                    "alias": "vw_orders",
                    "depends_on": {"nodes": ["source.test.orders"]},
                },
                "model.test.fact_orders": {
                    "resource_type": "model",
                    "name": "fact_orders",
                    "database": "DB",
                    "schema": "ENRICHED",
                    "alias": "fact_orders",
                    "depends_on": {"nodes": ["model.test.vw_orders"]},
                },
            },
            "sources": {
                "source.test.orders": {
                    "resource_type": "source",
                    "name": "orders",
                    "database": "DB",
                    "schema": "RAW",
                    "identifier": "orders",
                }
            },
        }
        edges = MODULE._build_lineage_edges(
            manifest,
            default_database=None,
            schema_map={},
            include_views=False,
        )
        self.assertEqual(len(edges), 1)
        _, from_ref, to_ref = edges[0]
        self.assertEqual(from_ref.schema, "RAW")
        self.assertEqual(from_ref.table, "orders")
        self.assertEqual(to_ref.schema, "ENRICHED")
        self.assertEqual(to_ref.table, "fact_orders")

    def test_build_lineage_edges_keeps_views_when_enabled(self) -> None:
        manifest = {
            "nodes": {
                "model.test.vw_orders": {
                    "resource_type": "model",
                    "name": "vw_orders",
                    "database": "DB",
                    "schema": "STAGING",
                    "alias": "vw_orders",
                    "depends_on": {"nodes": ["source.test.orders"]},
                },
                "model.test.fact_orders": {
                    "resource_type": "model",
                    "name": "fact_orders",
                    "database": "DB",
                    "schema": "ENRICHED",
                    "alias": "fact_orders",
                    "depends_on": {"nodes": ["model.test.vw_orders"]},
                },
            },
            "sources": {
                "source.test.orders": {
                    "resource_type": "source",
                    "name": "orders",
                    "database": "DB",
                    "schema": "RAW",
                    "identifier": "orders",
                }
            },
        }
        edges = MODULE._build_lineage_edges(
            manifest,
            default_database=None,
            schema_map={},
            include_views=True,
        )
        chain = {(f.table, t.table) for _, f, t in edges}
        self.assertIn(("orders", "vw_orders"), chain)
        self.assertIn(("vw_orders", "fact_orders"), chain)

    def test_build_column_lineage_details_matches_case_insensitive_names(self) -> None:
        from_entity = {
            "fullyQualifiedName": "svc.DB.RAW.orders",
            "columns": [{"name": "ORDER_ID"}, {"name": "CUSTOMER_ID"}],
        }
        to_entity = {
            "fullyQualifiedName": "svc.DB.ENRICHED.fact_orders",
            "columns": [{"name": "order_id"}, {"name": "AMOUNT"}],
        }
        details, count = MODULE._build_column_lineage_details(from_entity, to_entity)
        self.assertIsNotNone(details)
        assert details is not None
        self.assertEqual(count, 1)
        self.assertEqual(
            details["columnsLineage"][0]["fromColumns"][0],
            "svc.DB.RAW.orders.ORDER_ID",
        )
        self.assertEqual(
            details["columnsLineage"][0]["toColumn"],
            "svc.DB.ENRICHED.fact_orders.order_id",
        )


if __name__ == "__main__":
    unittest.main()
