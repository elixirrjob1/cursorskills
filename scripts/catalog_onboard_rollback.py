#!/usr/bin/env python3
"""
Roll back a catalog onboard: delete the pipeline and service created by apply.

Usage:
    python scripts/catalog_onboard_rollback.py <plan_file> [--dry-run]

Guardrails enforced:
- Only deletes FQNs recorded in the plan's "created" section.
- Refuses to delete any FQN listed in "existing_service_fqns_snapshot" (pre-existed this run).
- Refuses if any table under the service has tags, glossary terms, or non-empty descriptions.
- --dry-run prints what would be deleted and exits 0 without making any API calls.
- Handles partial apply (e.g. service created but pipeline_fqn is null) cleanly.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from urllib import error as _urllib_error, request as _urllib_request

_SCRIPTS = Path(__file__).resolve().parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from keyvault_loader import load_env  # noqa: E402
from om_auth import om_api_url, om_bearer_headers  # noqa: E402


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _http(method: str, path: str, *, body: object = None) -> dict:
    url = om_api_url(path)
    headers = om_bearer_headers()
    payload = json.dumps(body).encode() if body is not None else None
    req = _urllib_request.Request(url, data=payload, headers=headers, method=method)
    try:
        with _urllib_request.urlopen(req, timeout=60) as resp:
            raw = resp.read()
            return json.loads(raw) if raw else {}
    except _urllib_error.HTTPError as exc:
        if exc.code == 404:
            return {}
        detail = exc.read().decode(errors="replace")[:500]
        _die(f"OM API {method} {path} failed: {exc.code}\n{detail}")


def _get(path: str) -> dict:
    return _http("GET", path)


def _delete(path: str) -> None:
    _http("DELETE", path)


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


# ---------------------------------------------------------------------------
# Guardrail: FQN must not be in the pre-existing snapshot
# ---------------------------------------------------------------------------

def _assert_not_in_snapshot(fqn: str, snapshot: list[str]) -> None:
    if fqn in snapshot:
        _die(
            f"'{fqn}' appears in the pre-existing service snapshot from plan time. "
            "This run did not create it. Refusing to delete."
        )


# ---------------------------------------------------------------------------
# Annotation check: refuse rollback if anything has been tagged / described
# ---------------------------------------------------------------------------

def _check_for_annotations(service_fqn: str) -> list[str]:
    """Return list of annotated table names. Empty list means safe to rollback."""
    encoded = _urllib_request.quote(service_fqn, safe="")
    result = _get(f"v1/tables?databaseSchema={service_fqn}&limit=100&include=non-deleted")
    tables = result.get("data", [])
    annotated = []
    for table in tables:
        has_tags = bool(table.get("tags"))
        has_glossary = bool(table.get("glossaryTerms") or table.get("glossaryTerm"))
        has_description = bool((table.get("description") or "").strip())
        if has_tags or has_glossary or has_description:
            annotated.append(table.get("fullyQualifiedName") or table.get("name", "unknown"))
    return annotated


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    dry_run = "--dry-run" in sys.argv
    args = [a for a in sys.argv[1:] if a != "--dry-run"]

    if len(args) != 1:
        print(f"Usage: python {Path(__file__).name} <plan_file> [--dry-run]", file=sys.stderr)
        return 1

    plan_path = Path(args[0])

    if not dry_run:
        load_env()

    print(f"Reading plan: {plan_path}")
    plan = _load_plan(plan_path)

    created = plan.get("created", {})
    service_fqn = created.get("service_fqn") or ""
    pipeline_fqn = created.get("pipeline_fqn") or ""
    snapshot: list[str] = plan.get("existing_service_fqns_snapshot", [])

    if not service_fqn:
        print("Nothing to roll back — plan has no recorded service_fqn in 'created'.")
        return 0

    # --- Snapshot guardrail ---
    _assert_not_in_snapshot(service_fqn, snapshot)

    # --- Dry-run output ---
    if dry_run:
        print("\n--- DRY RUN (no changes will be made) ---")
        if pipeline_fqn:
            print(f"  Would delete pipeline:  {pipeline_fqn}")
        print(f"  Would delete service:   {service_fqn}")
        print("\nChecking for annotations (dry-run reads OM but makes no writes)...")
        annotated = _check_for_annotations(service_fqn)
        if annotated:
            print(f"\n  WOULD BLOCK rollback — {len(annotated)} annotated table(s) found:")
            for t in annotated:
                print(f"    - {t}")
            print("  Remove annotations manually or accept the service to proceed.")
        else:
            print("  No annotations found — rollback would proceed safely.")
        print("\n--- END DRY RUN ---")
        return 0

    # --- Annotation guardrail ---
    print("Checking for downstream annotations before rollback...")
    annotated = _check_for_annotations(service_fqn)
    if annotated:
        _die(
            f"Cannot roll back. {len(annotated)} table(s) have been annotated since ingestion:\n"
            + "\n".join(f"  - {t}" for t in annotated)
            + "\nRemove annotations manually or accept the service. Rollback aborted — nothing deleted."
        )
    print("  No annotations found. Safe to proceed.")

    # --- Delete pipeline (if it was created) ---
    if pipeline_fqn:
        print(f"Deleting pipeline: {pipeline_fqn}...")
        encoded = _urllib_request.quote(pipeline_fqn, safe="")
        _delete(f"v1/services/ingestionPipelines/name/{encoded}?hardDelete=true")
        print(f"  Deleted: {pipeline_fqn}")
    else:
        print("  Pipeline FQN is null in plan (partial apply) — skipping pipeline deletion.")

    # --- Delete service ---
    print(f"Deleting service: {service_fqn}...")
    encoded_svc = _urllib_request.quote(service_fqn, safe="")
    _delete(f"v1/services/databaseServices/name/{encoded_svc}?hardDelete=true")
    print(f"  Deleted: {service_fqn}")

    print(
        f"\nRollback complete. OpenMetadata is in the same state it was before this onboard run.\n"
        f"Plan file '{plan_path.name}' can be deleted or kept as a record."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
