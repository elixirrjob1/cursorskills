"""Update an OpenMetadata ingestion pipeline's schema/database filter via REST,
then re-deploy the DAG to Airflow and trigger a run.

Workaround for the `update_metadata_ingestion_pipeline` MCP tool returning
opaque 400 errors.

Usage:
    python patch_pipeline_filter.py --pipeline-id <uuid> \
        --include-schemas BRONZE_ERP__DBO,DBT_PROD_ENRICHED \
        --include-databases DRIP_DATA_INTELLIGENCE

Omit --include-databases to leave the database filter untouched.
Pass --skip-run to only patch the filter without deploying or triggering.

Why deploy before trigger:
  Airflow bakes pipeline config into the DAG file at deploy time. Patching the
  OM pipeline config does NOT automatically push the updated DAG to Airflow.
  Running `trigger` without a prior `deploy` causes Airflow to execute the stale
  DAG (old filter), silently ignoring the new schemas. Always deploy first.
"""

from __future__ import annotations

import argparse
import json
import sys

from om_client import api, login


def _post_soft(path: str, token: str) -> dict | None:
    """POST to an OM endpoint; return parsed JSON or None on any error."""
    try:
        return api("POST", path, token, body={})
    except SystemExit:
        return None


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
    ap.add_argument(
        "--skip-run",
        action="store_true",
        help="Only patch the filter; do not deploy or trigger ingestion.",
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

    if args.skip_run:
        return 0

    pid = args.pipeline_id

    # Step 1: deploy — pushes the updated DAG file to Airflow so the new filter
    # is baked in. Without this, trigger runs the stale cached DAG.
    print("deploying updated DAG to Airflow...")
    deploy_result = _post_soft(f"/api/v1/services/ingestionPipelines/deploy/{pid}", token)
    if deploy_result is None:
        print("warning: deploy returned an error; trigger may use stale config", file=sys.stderr)
    else:
        msg = deploy_result.get("reason", deploy_result)
        print(f"deploy: {msg}")

    # Step 2: trigger — fires an immediate DAG run with the freshly deployed config.
    print("triggering ingestion run...")
    trigger_result = _post_soft(f"/api/v1/services/ingestionPipelines/trigger/{pid}", token)
    if trigger_result is None:
        # Fallback: some OM versions use /run instead of /trigger
        trigger_result = _post_soft(f"/api/v1/services/ingestionPipelines/run/{pid}", token)
    if trigger_result is None:
        print("warning: could not trigger ingestion — check Airflow manually", file=sys.stderr)
    else:
        msg = trigger_result.get("reason", trigger_result)
        print(f"trigger: {msg}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
