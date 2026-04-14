#!/usr/bin/env python3
"""Push classification tags from analyzer JSON into OpenMetadata.

Three phases:
1. Ensure Classification entities exist  (PUT /classifications)
2. Ensure Tag entities exist under each  (PUT /tags)
3. Assign tags to tables and columns     (PATCH /tables/{id})

Idempotent — safe to re-run.
"""

from __future__ import annotations

import base64
import json
import os
import sys
import time
from collections import defaultdict
from typing import Any

import requests
from dotenv import load_dotenv

load_dotenv()

API_VERSION_PREFIX = "v1"
_TOKEN_CACHE: dict[str, Any] = {"token": None}


# ---------------------------------------------------------------------------
# OpenMetadata API helpers (same pattern as push_descriptions_to_openmetadata)
# ---------------------------------------------------------------------------

def _api_root() -> str:
    base = os.getenv("OPENMETADATA_BASE_URL", "").strip().rstrip("/")
    if not base:
        raise RuntimeError("Missing OPENMETADATA_BASE_URL")
    return f"{base}/api" if not base.endswith("/api") else base


def _api_url(path: str) -> str:
    cleaned = path.lstrip("/")
    if not cleaned.startswith(f"{API_VERSION_PREFIX}/"):
        cleaned = f"{API_VERSION_PREFIX}/{cleaned}"
    return f"{_api_root()}/{cleaned}"


def _login() -> str:
    jwt = os.getenv("OPENMETADATA_JWT_TOKEN", "").strip()
    if jwt:
        return jwt
    cached = _TOKEN_CACHE.get("token")
    if isinstance(cached, str) and cached.strip():
        return cached.strip()
    email = os.getenv("OPENMETADATA_EMAIL", "").strip()
    password = os.getenv("OPENMETADATA_PASSWORD", "")
    if not email or not password:
        raise RuntimeError("Missing OPENMETADATA_EMAIL / OPENMETADATA_PASSWORD")
    encoded = base64.b64encode(password.encode()).decode("ascii")
    for payload in [{"email": email, "password": encoded}, {"email": email, "password": password}]:
        try:
            r = requests.post(
                _api_url("users/login"),
                headers={"Accept": "application/json", "Content-Type": "application/json"},
                json=payload, timeout=(10, 30),
            )
            r.raise_for_status()
            data = r.json() if r.content else {}
            for key in ("accessToken", "jwtToken", "token", "id_token"):
                tok = data.get(key)
                if isinstance(tok, str) and tok.strip():
                    _TOKEN_CACHE["token"] = tok.strip()
                    return tok.strip()
        except Exception:
            continue
    raise RuntimeError("OpenMetadata login failed")


def _headers(content_type: str = "application/json") -> dict[str, str]:
    return {
        "Accept": "application/json",
        "Content-Type": content_type,
        "Authorization": f"Bearer {_login()}",
    }


def _request(method: str, endpoint: str, payload: Any = None, params: dict | None = None) -> Any:
    for attempt in range(2):
        try:
            r = requests.request(
                method, _api_url(endpoint),
                headers=_headers(), json=payload, params=params, timeout=(10, 30),
            )
            if r.status_code == 401 and attempt < 1:
                _TOKEN_CACHE["token"] = None
                continue
            r.raise_for_status()
            if r.status_code == 204 or not r.content:
                return {"success": True}
            return r.json()
        except Exception as e:
            if attempt == 1:
                raise RuntimeError(f"{method} {endpoint} failed: {e}") from e
            time.sleep(1)


def _patch_json(endpoint: str, ops: list[dict[str, Any]]) -> Any:
    hdrs = _headers("application/json-patch+json")
    for attempt in range(2):
        try:
            r = requests.patch(_api_url(endpoint), headers=hdrs, json=ops, timeout=(10, 30))
            if r.status_code == 401 and attempt < 1:
                _TOKEN_CACHE["token"] = None
                continue
            r.raise_for_status()
            return r.json() if r.content else {"success": True}
        except Exception as e:
            if attempt == 1:
                raise RuntimeError(f"PATCH {endpoint} failed: {e}") from e
            time.sleep(1)


# ---------------------------------------------------------------------------
# Phase 1 & 2: ensure Classification + Tag definitions exist
# ---------------------------------------------------------------------------

def _ensure_classification(name: str) -> dict[str, Any]:
    """PUT a Classification (idempotent create-or-update)."""
    return _request("PUT", "classifications", payload={
        "name": name,
        "displayName": name,
        "description": f"Auto-created from analyzer JSON — {name}",
        "mutuallyExclusive": True,
    })


def _ensure_tag(classification_name: str, tag_name: str) -> dict[str, Any]:
    """PUT a Tag under a Classification (idempotent create-or-update)."""
    return _request("PUT", "tags", payload={
        "name": tag_name,
        "displayName": tag_name,
        "description": f"{classification_name}.{tag_name}",
        "classification": classification_name,
    })


# ---------------------------------------------------------------------------
# Phase 3: assign tags to tables and columns
# ---------------------------------------------------------------------------

def _tag_label(tag_fqn: str) -> dict[str, Any]:
    return {"tagFQN": tag_fqn, "source": "Classification", "labelType": "Manual", "state": "Confirmed"}


def _merge_labels(existing: list[dict] | None, new_fqns: list[str]) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = list(existing or [])
    seen = {(t.get("tagFQN", ""), t.get("source", "")) for t in merged}
    for fqn in new_fqns:
        key = (fqn, "Classification")
        if key not in seen:
            merged.append(_tag_label(fqn))
            seen.add(key)
    return merged


def _assign_table_tags(table_id: str, existing_tags: list[dict] | None, tag_fqns: list[str]) -> dict:
    merged = _merge_labels(existing_tags, tag_fqns)
    return _patch_json(f"tables/{table_id}", [{"op": "replace", "path": "/tags", "value": merged}])


def _assign_column_tags(
    table_id: str, col_index: int, existing_tags: list[dict] | None, tag_fqns: list[str],
) -> dict:
    merged = _merge_labels(existing_tags, tag_fqns)
    return _patch_json(
        f"tables/{table_id}",
        [{"op": "replace", "path": f"/columns/{col_index}/tags", "value": merged}],
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    analyzer_path = sys.argv[1] if len(sys.argv) > 1 else "LATEST_SCHEMA/schema_azure_mssql_dbo.json"
    fqn_prefix = sys.argv[2] if len(sys.argv) > 2 else "snowflake_fivetran.DRIP_DATA_INTELLIGENCE.BRONZE_ERP__DBO"

    with open(analyzer_path) as f:
        data = json.load(f)

    tables = data.get("tables", [])
    print(f"Loaded {len(tables)} tables from {analyzer_path}")
    print(f"Target FQN prefix: {fqn_prefix}\n")

    # ── Collect all unique Classification.Tag pairs ──
    all_tags: set[str] = set()
    for t in tables:
        all_tags.update(t.get("classification_tags", []))
        for c in t.get("columns", []):
            all_tags.update(c.get("classification_tags", []))

    by_class: dict[str, set[str]] = defaultdict(set)
    for tag in all_tags:
        parts = tag.split(".", 1)
        if len(parts) == 2:
            by_class[parts[0]].add(parts[1])

    print(f"Found {len(by_class)} classifications, {len(all_tags)} unique tags\n")

    # ── Phase 1: ensure Classifications ──
    print("Phase 1: Creating classification definitions …")
    for cls_name in sorted(by_class):
        try:
            _ensure_classification(cls_name)
            print(f"  ✓ {cls_name}")
        except Exception as e:
            print(f"  ✗ {cls_name}: {e}")

    # ── Phase 2: ensure Tags ──
    print("\nPhase 2: Creating tag definitions …")
    for cls_name in sorted(by_class):
        for tag_name in sorted(by_class[cls_name]):
            try:
                _ensure_tag(cls_name, tag_name)
                print(f"  ✓ {cls_name}.{tag_name}")
            except Exception as e:
                print(f"  ✗ {cls_name}.{tag_name}: {e}")

    # ── Fetch OpenMetadata tables for assignment ──
    print("\nFetching OpenMetadata tables …")
    om_resp = _request("GET", "tables", params={
        "databaseSchema": fqn_prefix, "fields": "columns,tags", "limit": 500,
    })
    om_tables = om_resp.get("data", [])
    om_lookup: dict[str, dict] = {t["name"].upper(): t for t in om_tables}
    print(f"  {len(om_lookup)} tables in schema\n")

    # ── Phase 3: assign tags ──
    print("Phase 3: Assigning tags to tables and columns …")
    stats = {"tables": 0, "columns": 0, "skipped": 0, "errors": []}

    for analyzer_table in tables:
        table_name = analyzer_table.get("table", "")
        table_tags = analyzer_table.get("classification_tags", [])

        if table_name.upper() not in om_lookup:
            print(f"  SKIP {table_name} — not in OpenMetadata")
            stats["skipped"] += 1
            continue

        om_table = om_lookup[table_name.upper()]
        table_id = om_table["id"]

        if table_tags:
            try:
                om_table = _assign_table_tags(table_id, om_table.get("tags"), table_tags)
                print(f"  ✓ table {table_name} ({len(table_tags)} tags)")
                stats["tables"] += 1
            except Exception as e:
                msg = f"FAIL table {table_name}: {e}"
                print(f"  ✗ {msg}")
                stats["errors"].append(msg)

        om_columns = om_table.get("columns", [])
        om_col_idx: dict[str, int] = {c["name"].upper(): i for i, c in enumerate(om_columns)}

        for analyzer_col in analyzer_table.get("columns", []):
            col_name = analyzer_col.get("name", "")
            col_tags = analyzer_col.get("classification_tags", [])
            if not col_tags:
                continue
            if col_name.upper() not in om_col_idx:
                continue

            idx = om_col_idx[col_name.upper()]
            try:
                _assign_column_tags(table_id, idx, om_columns[idx].get("tags"), col_tags)
                print(f"    ✓ {table_name}.{col_name} ({len(col_tags)} tags)")
                stats["columns"] += 1
            except Exception as e:
                msg = f"FAIL {table_name}.{col_name}: {e}"
                print(f"    ✗ {msg}")
                stats["errors"].append(msg)

    print(f"\n{'=' * 60}")
    print(f"Tables tagged:   {stats['tables']}")
    print(f"Columns tagged:  {stats['columns']}")
    print(f"Tables skipped:  {stats['skipped']}")
    print(f"Errors:          {len(stats['errors'])}")
    if stats["errors"]:
        for err in stats["errors"]:
            print(f"  - {err}")


if __name__ == "__main__":
    main()
