#!/usr/bin/env python3
"""
Apply a catalog onboard plan: create OM service + pipeline, run ingestion, verify tables.

Usage:
    python .cursor/skills/catalog-sync/scripts/catalog_onboard_apply.py <plan_file>

The plan file is written by the catalog-sync onboard route (plan step).
It must live in .cursor/flat/ and follow the onboard_plan_<source>.json naming convention.

Guardrails enforced:
- Refuses if the service already exists in OM.
- Refuses if a pipeline already exists for the service.
- Refuses if password_env is missing or resolves to an empty value.
- Refuses if connection_config contains a literal non-empty "password" field.
- Records every created FQN back into the plan file's "created" section.
- Polls ingestion status up to INGESTION_TIMEOUT_SECONDS before reporting timeout.
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

# om_auth is bundled alongside this script (skill-local copy).
from om_auth import om_api_url, om_bearer_headers  # noqa: E402


def _add_shared_scripts_to_path() -> None:
    """Locate the repo-root scripts/ that holds the shared keyvault_loader.

    keyvault_loader carries the centrally-maintained ENV_VARS secret allowlist,
    so it is NOT bundled per-skill — there must be exactly one canonical copy.
    """
    for parent in _HERE.parents:
        if (parent / "scripts" / "keyvault_loader.py").is_file():
            shared = str(parent / "scripts")
            if shared not in sys.path:
                sys.path.append(shared)
            return
    print(
        "ERROR: Could not locate shared scripts/keyvault_loader.py. "
        "Run this from within the cursorskills repo so the shared secret "
        "allowlist (ENV_VARS) is available.",
        file=sys.stderr,
    )
    sys.exit(1)


_add_shared_scripts_to_path()
from keyvault_loader import load_env  # noqa: E402

try:
    from urllib import request as _urllib_request, error as _urllib_error
except ImportError:
    pass

INGESTION_TIMEOUT_SECONDS = 600  # 10 minutes
POLL_INTERVAL_SECONDS = 15


# ---------------------------------------------------------------------------
# HTTP helpers (no om_client dependency — keep this script self-contained)
# ---------------------------------------------------------------------------

def _http(
    method: str,
    path: str,
    *,
    body: object = None,
    allow_404: bool = False,
    soft_fail: bool = False,
) -> dict | None:
    url = om_api_url(path)
    headers = om_bearer_headers()
    payload = json.dumps(body).encode() if body is not None else None
    req = _urllib_request.Request(url, data=payload, headers=headers, method=method)
    try:
        with _urllib_request.urlopen(req, timeout=60) as resp:
            raw = resp.read()
            return json.loads(raw) if raw else {}
    except _urllib_error.HTTPError as exc:
        if allow_404 and exc.code == 404:
            return {}
        if soft_fail:
            return None
        detail = exc.read().decode(errors="replace")[:500]
        _die(f"OM API {method} {path} failed: {exc.code}\n{detail}")


def _get(path: str) -> dict:
    return _http("GET", path)


def _post(path: str, body: object) -> dict:
    return _http("POST", path, body=body)


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _die(msg: str) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(1)


def _load_plan(plan_path: Path) -> dict:
    if not plan_path.exists():
        _die(f"Plan file not found: {plan_path}")
    with plan_path.open(encoding="utf-8") as f:
        return json.load(f)


def _save_plan(plan_path: Path, plan: dict) -> None:
    with plan_path.open("w", encoding="utf-8") as f:
        json.dump(plan, f, indent=2)
        f.write("\n")


# ---------------------------------------------------------------------------
# Guardrail: no literal password in connection_config
# ---------------------------------------------------------------------------

def _assert_no_literal_password(connection_config: dict) -> None:
    literal_pw = connection_config.get("password", "")
    if literal_pw:
        _die(
            "connection_config contains a literal 'password' value. "
            "Move the password to .env and reference it via 'password_env' instead. "
            "The plan file must never contain secret values."
        )


# ---------------------------------------------------------------------------
# Guardrail: password_env variable must resolve
# ---------------------------------------------------------------------------

def _assert_password_env_resolves(connection_config: dict) -> None:
    env_var = connection_config.get("password_env", "").strip()
    if not env_var:
        _die(
            "connection_config is missing 'password_env'. "
            "Provide the .env variable name for the database password."
        )
    value = os.environ.get(env_var, "")
    if not value:
        _die(
            f"Environment variable '{env_var}' is not set or is empty after loading .env / Key Vault. "
            f"To fix:\n"
            f"  Option A: Add {env_var}=<password> to your .env file.\n"
            f"  Option B: Add '{env_var}' to the ENV_VARS list in scripts/keyvault_loader.py "
            f"and store the secret as '{env_var.replace('_', '-')}' in Azure Key Vault."
        )


# ---------------------------------------------------------------------------
# Guardrail: service must not already exist
# ---------------------------------------------------------------------------

def _assert_service_does_not_exist(service_name: str) -> None:
    result = _http("GET", f"v1/services/databaseServices/name/{service_name}", allow_404=True)
    if result.get("id"):
        _die(
            f"Service '{service_name}' already exists in OpenMetadata (id: {result['id']}). "
            "This route only creates new services. "
            "To update an existing service, use the standard catalog-sync configure/update workflow."
        )


# ---------------------------------------------------------------------------
# Guardrail: pipeline must not already exist for service
# ---------------------------------------------------------------------------

def _assert_pipeline_does_not_exist(service_name: str) -> None:
    result = _http("GET", f"v1/services/ingestionPipelines?service={service_name}&limit=1", allow_404=True)
    pipelines = result.get("data", [])
    if pipelines:
        _die(
            f"An ingestion pipeline already exists for service '{service_name}': "
            f"{pipelines[0].get('fullyQualifiedName', pipelines[0].get('name'))}. "
            "This route only creates new pipelines. "
            "To update or re-run an existing pipeline, use the standard catalog-sync workflow."
        )


# ---------------------------------------------------------------------------
# Create service
# ---------------------------------------------------------------------------

_AUTH_TYPE_SERVICES = {"Postgres", "Mssql", "Mysql", "Oracle"}


def _fetch_service_id(service_name: str) -> tuple[str, str]:
    """Fetch existing service; return (fqn, uuid)."""
    result = _http("GET", f"v1/services/databaseServices/name/{service_name}", allow_404=True)
    fqn = result.get("fullyQualifiedName") or result.get("name")
    uid = result.get("id")
    if not fqn or not uid:
        _die(f"Could not fetch service '{service_name}' from OM.")
    return fqn, uid


def _create_service(plan: dict) -> tuple[str, str]:
    """Create databaseService, return (fqn, uuid)."""
    conn = plan["connection_config"]
    service_type = plan["service_type"]

    # Resolve env: references for non-secret fields (e.g. hostPort stored in env)
    config: dict = {}
    for k, v in conn.items():
        if k == "password_env":
            continue
        if isinstance(v, str) and v.startswith("env:"):
            var_name = v[4:].strip()
            resolved = os.environ.get(var_name, "")
            if not resolved:
                _die(f"Environment variable '{var_name}' is not set or empty.")
            config[k] = resolved
        else:
            config[k] = v

    # If hostPort resolved to a full connection URL, parse it into components
    raw_host = config.get("hostPort", "")
    if raw_host and "://" in raw_host:
        try:
            from urllib.parse import urlparse as _urlparse
            _p = _urlparse(raw_host)
            config["hostPort"] = f"{_p.hostname}:{_p.port or 5432}"
            # Use username from URL if not explicitly overridden to something meaningful
            if _p.username and config.get("username") in (None, "", "postgres"):
                config["username"] = _p.username
            # Fill DB field from URL only when the plan already names that field.
            _db_from_url = (_p.path or "").lstrip("/")
            if _db_from_url:
                if "databaseName" in conn and not config.get("databaseName"):
                    config["databaseName"] = _db_from_url
                elif "database" in conn and not config.get("database"):
                    config["database"] = _db_from_url
        except Exception:
            _die(f"hostPort '{raw_host}' looks like a URL but could not be parsed. "
                 "Set hostPort to 'host:port' format in your plan file.")

    password = os.environ.get(conn["password_env"], "")
    if not password:
        _die(f"Environment variable '{conn['password_env']}' is not set or empty.")

    # Newer OM versions wrap password in authType for most DB connectors
    if service_type in _AUTH_TYPE_SERVICES:
        config["authType"] = {"password": password}
    else:
        config["password"] = password

    body = {
        "name": plan["service_name"],
        "serviceType": service_type,
        "connection": {
            "config": {
                "type": service_type,
                **config,
            }
        },
    }
    result = _post("v1/services/databaseServices", body)
    fqn = result.get("fullyQualifiedName") or result.get("name")
    uid = result.get("id")
    if not fqn or not uid:
        _die(f"Service creation returned no FQN/ID. Response: {json.dumps(result)[:300]}")
    return fqn, uid


# ---------------------------------------------------------------------------
# Create pipeline
# ---------------------------------------------------------------------------

def _create_pipeline(plan: dict, service_fqn: str, service_id: str) -> tuple[str, str]:
    """Create metadataIngestionPipeline, return (fqn, uuid)."""
    pipeline_name = f"{plan['service_name']}_metadata_ingestion"
    include_schemas = plan.get("include_schemas", [])

    source_config: dict = {"type": "DatabaseMetadata"}
    if include_schemas:
        source_config["schemaFilterPattern"] = {"includes": include_schemas}

    body = {
        "name": pipeline_name,
        "pipelineType": "metadata",
        "service": {"id": service_id, "type": "databaseService", "name": service_fqn},
        "sourceConfig": {"config": source_config},
        "airflowConfig": {},
    }
    result = _post("v1/services/ingestionPipelines", body)
    fqn = result.get("fullyQualifiedName") or result.get("name")
    uid = result.get("id")
    if not fqn or not uid:
        _die(f"Pipeline creation returned no FQN/ID. Response: {json.dumps(result)[:300]}")
    return fqn, uid


def _fetch_pipeline_id(pipeline_fqn: str) -> str:
    """Fetch pipeline UUID from OM by FQN."""
    encoded = _urllib_request.quote(pipeline_fqn, safe="")
    result = _http("GET", f"v1/services/ingestionPipelines/name/{encoded}", allow_404=True)
    uid = result.get("id")
    if not uid:
        _die(f"Could not fetch pipeline ID for '{pipeline_fqn}'.")
    return uid


# ---------------------------------------------------------------------------
# Run ingestion and poll
# ---------------------------------------------------------------------------

def _run_and_poll(pipeline_fqn: str, pipeline_id: str) -> None:
    # Always deploy first: pushes the current OM pipeline config (including filter) into the
    # Airflow DAG file. Without this, Airflow runs a stale cached DAG and ignores config changes.
    print("  Deploying DAG to Airflow...")
    _http("POST", f"v1/services/ingestionPipelines/deploy/{pipeline_id}", soft_fail=True)

    # Then trigger an immediate run against the freshly deployed DAG.
    triggered = False
    for candidate in ("trigger", "run"):
        result = _http("POST", f"v1/services/ingestionPipelines/{candidate}/{pipeline_id}", soft_fail=True)
        if result is not None:
            print(f"  Ingestion triggered via /{candidate}.")
            triggered = True
            break
    if not triggered:
        _die(f"Could not trigger ingestion pipeline '{pipeline_fqn}' — tried trigger/run.")
    print(f"  Polling every {POLL_INTERVAL_SECONDS}s (timeout {INGESTION_TIMEOUT_SECONDS}s)...")
    encoded_fqn = _urllib_request.quote(pipeline_fqn, safe="")

    deadline = time.time() + INGESTION_TIMEOUT_SECONDS
    while time.time() < deadline:
        time.sleep(POLL_INTERVAL_SECONDS)
        status_resp = _get(f"v1/services/ingestionPipelines/name/{encoded_fqn}?fields=pipelineStatuses")
        state = (
            status_resp.get("pipelineStatuses", {}).get("pipelineState")
            or status_resp.get("status", {}).get("pipelineState")
            or "unknown"
        )
        print(f"  Status: {state}")
        if state in ("success", "Success"):
            print("  Ingestion completed successfully.")
            return
        if state in ("failed", "Failed"):
            _die(
                f"Ingestion failed for pipeline '{pipeline_fqn}'. "
                "Check OpenMetadata UI for detailed logs."
            )

    print(
        f"\nWARNING: Ingestion did not complete within {INGESTION_TIMEOUT_SECONDS}s. "
        "It may still be running. Check status manually with get_ingestion_status in OpenMetadata, "
        "then run verification separately.",
        file=sys.stderr,
    )


# ---------------------------------------------------------------------------
# Verify tables
# ---------------------------------------------------------------------------

def _verify_tables(plan: dict, service_fqn: str) -> None:
    expected: list[str] = plan.get("expected_tables", [])
    analyzer_json = plan.get("analyzer_json", "")

    result = _get(f"v1/tables?databaseSchema={service_fqn}&limit=100&include=non-deleted")
    found_names = {t["name"] for t in result.get("data", [])}

    if expected:
        missing = [t for t in expected if t not in found_names]
        if missing:
            print(
                f"\nWARNING: {len(missing)} expected table(s) not found in OM after ingestion:",
                file=sys.stderr,
            )
            for t in missing:
                print(f"  - {t}", file=sys.stderr)
        else:
            print(f"  All {len(expected)} expected tables verified by name.")
    else:
        count = len(found_names)
        print(f"  Count-only verification: {count} table(s) imported. "
              f"(No analyzer JSON provided — named-table check not available.)")
        if count == 0:
            print("  WARNING: Zero tables found. Ingestion may have succeeded but imported nothing.",
                  file=sys.stderr)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    if len(sys.argv) != 2:
        print(f"Usage: python {Path(__file__).name} <plan_file>", file=sys.stderr)
        return 1

    plan_path = Path(sys.argv[1])

    print("Loading environment (Key Vault / .env)...")
    load_env()

    print(f"Reading plan: {plan_path}")
    plan = _load_plan(plan_path)

    service_name = plan.get("service_name", "")
    if not service_name:
        _die("Plan file is missing 'service_name'.")

    conn = plan.get("connection_config", {})

    # --- Guardrails ---
    print("Running pre-apply guardrails...")
    _assert_no_literal_password(conn)
    _assert_password_env_resolves(conn)

    # --- Create or resume service ---
    existing_service_fqn = plan["created"].get("service_fqn")
    if existing_service_fqn:
        print(f"Resuming partial apply — service '{existing_service_fqn}' already created, fetching UUID...")
        service_fqn, service_id = _fetch_service_id(existing_service_fqn)
        print(f"  Fetched: {service_fqn} (id: {service_id})")
    else:
        _assert_service_does_not_exist(service_name)
        _assert_pipeline_does_not_exist(service_name)
        print("  All guardrails passed.")
        print(f"Creating databaseService '{service_name}'...")
        service_fqn, service_id = _create_service(plan)
        print(f"  Created: {service_fqn} (id: {service_id})")
        plan["created"]["service_fqn"] = service_fqn
        _save_plan(plan_path, plan)

    # --- Create pipeline ---
    pipeline_name = f"{service_name}_metadata_ingestion"
    existing_pipeline_fqn = plan["created"].get("pipeline_fqn")
    if existing_pipeline_fqn:
        print(f"Resuming partial apply — pipeline '{existing_pipeline_fqn}' already created, fetching UUID...")
        pipeline_fqn = existing_pipeline_fqn
        pipeline_id = _fetch_pipeline_id(pipeline_fqn)
        print(f"  Fetched pipeline ID: {pipeline_id}")
    else:
        _assert_pipeline_does_not_exist(service_name)
        print(f"Creating ingestionPipeline '{pipeline_name}'...")
        pipeline_fqn, pipeline_id = _create_pipeline(plan, service_fqn, service_id)
        print(f"  Created: {pipeline_fqn} (id: {pipeline_id})")
        plan["created"]["pipeline_fqn"] = pipeline_fqn
        _save_plan(plan_path, plan)

    # --- Run ingestion ---
    print("Running ingestion...")
    _run_and_poll(pipeline_fqn, pipeline_id)

    # --- Verify tables ---
    print("Verifying imported tables...")
    _verify_tables(plan, service_fqn)

    print(
        f"\n'{service_name}' is live in OpenMetadata.\n"
        "To assign tags or glossary terms, use catalog-glossary-tagger or stm-to-catalog-enricher.\n"
        "Do NOT use this route for tagging or for re-running ingestion on existing services."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
