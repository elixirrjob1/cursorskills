#!/usr/bin/env python3
"""Import dbt lineage edges into OpenMetadata from a dbt manifest.

This script reads dbt model dependencies from manifest.json and writes table
lineage edges to OpenMetadata using PUT /api/v1/lineage.

Credentials and endpoint (never hardcoded):

  OPENMETADATA_BASE_URL   Required. Prefer https://...
  OPENMETADATA_EMAIL      Required unless you adopt JWT separately later
  OPENMETADATA_PASSWORD   Required unless you adopt JWT separately later

Optional operational env (defaults are secure for shared/CI runs):

  OPENMETADATA_ENV_FILE               Path to dotenv file (when not using --env-file)
  OPENMETADATA_DOTENV_WALK_PARENTS   If \"true\"/\"1\"/\"yes\"/\"on\", walk cwd parents for .env
  OPENMETADATA_ALLOW_INSECURE_HTTP   If \"true\"/\"1\"/\"yes\"/\"on\", allow http:// URLs
  OPENMETADATA_DEBUG_ERRORS          If \"true\"/\"1\"/\"yes\"/\"on\", include API error bodies in messages

CLI overrides nothing hardcoded:

  --env-file PATH   Dotenv loaded before resolving OPENMETADATA_* (peeked early from argv)
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib import error, parse, request


def _truthy_env(name: str) -> bool:
    v = os.getenv(name, "").strip().lower()
    return v in {"1", "true", "yes", "on"}


def _apply_env_lines(text: str) -> None:
    for line in text.splitlines():
        raw = line.strip()
        if not raw or raw.startswith("#") or "=" not in raw:
            continue
        key, _, val = raw.partition("=")
        os.environ.setdefault(key.strip(), val.strip().strip('"').strip("'"))


def _load_one_env_file(path: Path) -> None:
    if path.is_file():
        _apply_env_lines(path.read_text(encoding="utf-8"))


def _load_dotenv_sources(*, cwd: Path, explicit_env_file: str | None) -> None:
    """Populate os.environ from dotenv without hardcoding repo paths."""
    if explicit_env_file:
        _load_one_env_file(Path(explicit_env_file).expanduser().resolve())
        return
    env_ref = os.getenv("OPENMETADATA_ENV_FILE", "").strip()
    if env_ref:
        _load_one_env_file(Path(env_ref).expanduser().resolve())
        return
    if _truthy_env("OPENMETADATA_DOTENV_WALK_PARENTS"):
        for candidate in [cwd.resolve(), *cwd.resolve().parents]:
            env_file = candidate / ".env"
            if env_file.is_file():
                _apply_env_lines(env_file.read_text(encoding="utf-8"))
                return
        return
    dot = cwd.resolve() / ".env"
    if dot.is_file():
        _apply_env_lines(dot.read_text(encoding="utf-8"))


def _peek_env_file_arg(argv: list[str]) -> str | None:
    for i, a in enumerate(argv):
        if a.startswith("--env-file=") and len(a) > len("--env-file="):
            return a.split("=", 1)[1]
        if a == "--env-file" and i + 1 < len(argv):
            return argv[i + 1]
    return None


def _require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise SystemExit(f"error: required env var {name} is not set")
    return value


def _normalize_om_base_url(value: str) -> str:
    base = value.strip().rstrip("/")
    if not base:
        raise SystemExit("error: OPENMETADATA_BASE_URL is empty")
    parsed = parse.urlsplit(base)
    scheme = (parsed.scheme or "").lower()
    if not scheme:
        raise SystemExit(
            "error: OPENMETADATA_BASE_URL must include a URI scheme "
            "(e.g. https://host:8585)."
        )
    if scheme == "http" and not _truthy_env("OPENMETADATA_ALLOW_INSECURE_HTTP"):
        raise SystemExit(
            "error: OPENMETADATA_BASE_URL uses HTTP. Use HTTPS or set "
            "OPENMETADATA_ALLOW_INSECURE_HTTP=true when you explicitly accept cleartext."
        )
    if base.endswith("/api"):
        return base
    return f"{base}/api"


def _http_error_suffix(exc: error.HTTPError) -> str:
    if _truthy_env("OPENMETADATA_DEBUG_ERRORS"):
        detail = exc.read().decode(errors="replace")
        return f": {detail[:500]}"
    return ""


def _login() -> tuple[str, str]:
    base_url = _normalize_om_base_url(_require_env("OPENMETADATA_BASE_URL"))
    email = _require_env("OPENMETADATA_EMAIL")
    password = _require_env("OPENMETADATA_PASSWORD")
    encoded_password = base64.b64encode(password.encode("utf-8")).decode("ascii")
    body = {"email": email, "password": encoded_password}
    payload = json.dumps(body).encode("utf-8")
    req = request.Request(
        f"{base_url}/v1/users/login",
        data=payload,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read() or b"{}")
    except error.HTTPError as exc:
        suf = _http_error_suffix(exc)
        raise SystemExit(f"error: OpenMetadata login failed ({exc.code}){suf}") from exc
    except error.URLError as exc:
        raise SystemExit("error: OpenMetadata login failed (network)") from exc
    token = str(data.get("accessToken") or data.get("jwtToken") or "").strip()
    if not token:
        raise SystemExit("error: login response missing token")
    return base_url, token


def _api(
    method: str,
    api_base: str,
    token: str,
    path: str,
    *,
    body: dict[str, Any] | list[dict[str, Any]] | None = None,
) -> Any:
    url = f"{api_base}{path if path.startswith('/') else '/' + path}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }
    payload = None
    if body is not None:
        payload = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = request.Request(url, data=payload, headers=headers, method=method)
    try:
        with request.urlopen(req, timeout=45) as resp:
            raw = resp.read()
            return json.loads(raw) if raw else {}
    except error.HTTPError as exc:
        suf = _http_error_suffix(exc)
        raise RuntimeError(f"{method} {path} failed ({exc.code}){suf}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"{method} {path} failed (network)") from exc


@dataclass(frozen=True)
class TableRef:
    database: str
    schema: str
    table: str


@dataclass
class Counts:
    total_edges_from_manifest: int = 0
    attempted_edges: int = 0
    created_or_updated_edges: int = 0
    skipped_missing_entities: int = 0
    skipped_unsupported_dependencies: int = 0
    errors: int = 0


def _norm(value: str) -> str:
    return value.strip()


def _upper(value: str) -> str:
    return _norm(value).upper()


def _table_fqn(service: str, ref: TableRef) -> str:
    return f"{service}.{ref.database}.{ref.schema}.{ref.table}"


def _parse_schema_map(raw: str | None) -> dict[str, str]:
    if not raw:
        return {}
    out: dict[str, str] = {}
    for pair in raw.split(","):
        part = pair.strip()
        if not part:
            continue
        if ":" not in part:
            raise SystemExit(
                "error: invalid --schema-map format; use FROM:TO,FROM2:TO2"
            )
        src, dst = part.split(":", 1)
        src_n = _upper(src)
        dst_n = _upper(dst)
        if not src_n or not dst_n:
            raise SystemExit(
                "error: invalid --schema-map entry; empty source/target schema"
            )
        out[src_n] = dst_n
    return out


def _table_from_dbt_node(
    node: dict[str, Any],
    *,
    default_database: str | None = None,
    schema_map: dict[str, str] | None = None,
) -> TableRef | None:
    database = _norm(default_database or str(node.get("database") or ""))
    schema = _norm(str(node.get("schema") or ""))
    table = _norm(str(node.get("alias") or node.get("identifier") or node.get("name") or ""))
    if not (database and schema and table):
        return None
    schema_upper = _upper(schema)
    mapped_schema = (schema_map or {}).get(schema_upper, schema)
    return TableRef(database=_upper(database), schema=mapped_schema, table=table)


def _collect_nodes(manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    nodes = dict(manifest.get("nodes") or {})
    sources = dict(manifest.get("sources") or {})
    snapshots = dict(manifest.get("snapshots") or {})
    seeds = dict(manifest.get("seeds") or {})
    out: dict[str, dict[str, Any]] = {}
    out.update(nodes)
    out.update(sources)
    out.update(snapshots)
    out.update(seeds)
    return out


def _resolve_table_entity(
    api_base: str,
    token: str,
    service: str,
    ref: TableRef,
    cache: dict[str, dict[str, Any] | None],
) -> dict[str, Any] | None:
    table_variants = [ref.table, ref.table.lower(), ref.table.upper()]
    schema_variants = [ref.schema, ref.schema.upper(), ref.schema.lower()]
    db_variants = [ref.database, ref.database.upper(), ref.database.lower()]
    candidates = [
        TableRef(db, schema, table)
        for db in db_variants
        for schema in schema_variants
        for table in table_variants
    ]
    for cand in candidates:
        fqn = _table_fqn(service, cand)
        if fqn in cache:
            entity = cache[fqn]
            if entity is not None:
                return entity
            continue
        encoded = parse.quote(fqn, safe="")
        try:
            payload = _api("GET", api_base, token, f"/v1/tables/name/{encoded}")
            cache[fqn] = payload
            return payload
        except RuntimeError:
            cache[fqn] = None
            continue
    return None


def _build_lineage_edges(
    manifest: dict[str, Any],
    *,
    default_database: str | None,
    schema_map: dict[str, str],
) -> list[tuple[str, TableRef, TableRef]]:
    collected = _collect_nodes(manifest)
    edges: list[tuple[str, TableRef, TableRef]] = []

    def _is_view_model(node: dict[str, Any]) -> bool:
        if str(node.get("resource_type") or "") != "model":
            return False
        name = str(node.get("name") or "")
        alias = str(node.get("alias") or "")
        return name.lower().startswith("vw_") or alias.lower().startswith("vw_")

    def _upstream_refs(node_id: str, seen: set[str]) -> list[TableRef]:
        node = collected.get(node_id) or {}
        refs: list[TableRef] = []
        for dep_id in ((node.get("depends_on") or {}).get("nodes") or []):
            if dep_id in seen:
                continue
            dep = collected.get(dep_id)
            if not dep:
                continue
            dep_type = str(dep.get("resource_type") or "")
            if dep_type not in {"model", "source", "seed", "snapshot"}:
                continue
            if dep_type == "model" and _is_view_model(dep):
                refs.extend(_upstream_refs(dep_id, seen | {dep_id}))
                continue
            dep_ref = _table_from_dbt_node(
                dep, default_database=default_database, schema_map=schema_map
            )
            if dep_ref:
                refs.append(dep_ref)
        deduped: list[TableRef] = []
        seen_keys: set[tuple[str, str, str]] = set()
        for ref in refs:
            key = (ref.database, ref.schema, ref.table)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            deduped.append(ref)
        return deduped

    for node_id, node in (manifest.get("nodes") or {}).items():
        if str(node.get("resource_type")) != "model":
            continue
        if _is_view_model(node):
            # OM currently catalogs enriched physical tables; skip intermediary vw_* targets.
            continue
        to_ref = _table_from_dbt_node(
            node, default_database=default_database, schema_map=schema_map
        )
        if to_ref is None:
            continue
        for from_ref in _upstream_refs(node_id, {node_id}):
            edges.append((node_id, from_ref, to_ref))
    return edges


def main() -> None:
    _load_dotenv_sources(
        cwd=Path.cwd(),
        explicit_env_file=_peek_env_file_arg(sys.argv[1:]),
    )

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manifest",
        required=True,
        help="Path to dbt manifest.json",
    )
    parser.add_argument(
        "--service",
        required=True,
        help="OpenMetadata database service name used in table FQNs",
    )
    parser.add_argument(
        "--default-database",
        help="Override dbt node database when manifest omits/normalizes it",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Resolve entities and print planned edges without writing lineage",
    )
    parser.add_argument(
        "--schema-map",
        help="Schema remap as FROM:TO pairs (comma-separated), e.g. DBT_DEV:DBT_PROD,DBT_DEV_ENRICHED:DBT_PROD_ENRICHED",
    )
    parser.add_argument(
        "--env-file",
        help="Load this dotenv file before reading OPENMETADATA_* (overrides OPENMETADATA_ENV_FILE for this run)",
    )
    args = parser.parse_args()

    manifest_path = Path(args.manifest).resolve()
    if not manifest_path.is_file():
        raise SystemExit(f"error: manifest file not found: {manifest_path}")

    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SystemExit(f"error: manifest is not valid JSON: {exc}") from exc

    api_base, token = _login()
    service = _norm(args.service)
    if not service:
        raise SystemExit("error: --service cannot be empty")

    schema_map = _parse_schema_map(args.schema_map)

    cache: dict[str, dict[str, Any] | None] = {}
    counts = Counts()

    planned = _build_lineage_edges(
        manifest,
        default_database=args.default_database,
        schema_map=schema_map,
    )
    counts.total_edges_from_manifest = len(planned)

    if not planned:
        print("No model dependency edges found in manifest.")
        return

    for node_id, from_ref, to_ref in planned:
        if not (from_ref.database and from_ref.schema and from_ref.table):
            counts.skipped_unsupported_dependencies += 1
            continue

        from_entity = _resolve_table_entity(api_base, token, service, from_ref, cache)
        to_entity = _resolve_table_entity(api_base, token, service, to_ref, cache)
        if from_entity is None or to_entity is None:
            counts.skipped_missing_entities += 1
            missing = []
            if from_entity is None:
                missing.append(_table_fqn(service, from_ref))
            if to_entity is None:
                missing.append(_table_fqn(service, to_ref))
            print(f"[SKIP] unresolved entity for {node_id}: {', '.join(missing)}")
            continue

        edge = {
            "edge": {
                "fromEntity": {"id": from_entity["id"], "type": "table"},
                "toEntity": {"id": to_entity["id"], "type": "table"},
                "description": f"Imported from dbt manifest dependency ({node_id})",
            }
        }

        counts.attempted_edges += 1
        if args.dry_run:
            print(
                "[DRY] "
                f"{from_entity.get('fullyQualifiedName')} -> {to_entity.get('fullyQualifiedName')}"
            )
            continue

        try:
            _api("PUT", api_base, token, "/v1/lineage", body=edge)
            counts.created_or_updated_edges += 1
            print(
                "[OK] "
                f"{from_entity.get('fullyQualifiedName')} -> {to_entity.get('fullyQualifiedName')}"
            )
        except RuntimeError as exc:
            counts.errors += 1
            print(
                "[ERR] "
                f"{from_entity.get('fullyQualifiedName')} -> {to_entity.get('fullyQualifiedName')}: {exc}"
            )

    print("\nSummary")
    print(f"- manifest edges: {counts.total_edges_from_manifest}")
    print(f"- attempted lineage writes: {counts.attempted_edges}")
    if args.dry_run:
        print("- writes executed: 0 (dry-run)")
    else:
        print(f"- successful writes: {counts.created_or_updated_edges}")
        print(f"- write errors: {counts.errors}")
    print(f"- skipped (missing OM entities): {counts.skipped_missing_entities}")
    print(f"- skipped (unsupported deps): {counts.skipped_unsupported_dependencies}")

    if counts.errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

