"""Update an OpenMetadata ingestion pipeline's schema/database filter via REST.

Workaround for the `update_metadata_ingestion_pipeline` MCP tool returning
opaque 400 errors.

Usage:
    python patch_pipeline_filter.py --pipeline-id <uuid> \
        --include-schemas DBT_PROD,DBT_PROD_ENRICHED \
        --include-databases DRIP_DATA_INTELLIGENCE

Omit --include-databases to leave the database filter untouched.
"""

from __future__ import annotations

import argparse
import json
import sys

from om_client import api, login


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pipeline-id", required=True, help="Ingestion pipeline UUID.")
    ap.add_argument(
        "--include-schemas",
        required=True,
        help="Comma-separated list of schema names to include (replaces current list).",
    )
    ap.add_argument(
        "--include-databases",
        help="Comma-separated list of database names to include (replaces current list). Optional.",
    )
    args = ap.parse_args()

    _base, token = login()

    schemas = [s.strip() for s in args.include_schemas.split(",") if s.strip()]
    patch = [
        {
            "op": "replace",
            "path": "/sourceConfig/config/schemaFilterPattern/includes",
            "value": schemas,
        }
    ]
    if args.include_databases:
        databases = [s.strip() for s in args.include_databases.split(",") if s.strip()]
        patch.append(
            {
                "op": "replace",
                "path": "/sourceConfig/config/databaseFilterPattern/includes",
                "value": databases,
            }
        )

    result = api(
        "PATCH",
        f"/api/v1/services/ingestionPipelines/{args.pipeline_id}",
        token,
        body=patch,
        content_type="application/json-patch+json",
    )

    filters = result.get("sourceConfig", {}).get("config", {})
    print("updated filters:")
    print(json.dumps(filters, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
