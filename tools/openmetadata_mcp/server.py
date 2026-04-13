#!/usr/bin/env python3
"""Local MCP server for OpenMetadata glossary, sync, and tagging operations."""

from __future__ import annotations

import base64
import json
import os
import time
from datetime import datetime
from typing import Any

import requests

from mcp.server.fastmcp import FastMCP


mcp = FastMCP("openmetadata-mcp")

USER_AGENT = "openmetadata-cursor-mcp"
LOGIN_ENDPOINT = "users/login"
API_VERSION_PREFIX = "v1"

DATABASE_SERVICE_ENDPOINT = "services/databaseServices"
INGESTION_PIPELINE_ENDPOINT = "services/ingestionPipelines"

ENTITY_ENDPOINTS = {
    "database_service": DATABASE_SERVICE_ENDPOINT,
    "ingestion_pipeline": INGESTION_PIPELINE_ENDPOINT,
    "database": "databases",
    "schema": "databaseSchemas",
    "table": "tables",
    "classification": "classifications",
    "tag": "tags",
    "glossary": "glossaries",
    "glossary_term": "glossaryTerms",
}

PIPELINE_RUN_ENDPOINT_CANDIDATES = (
    "deploy",
    "trigger",
    "run",
)
TABLE_ENTITY_FIELDS = "columns,tags"

_TOKEN_CACHE: dict[str, Any] = {"token": None}


def _normalize_base_url(value: str) -> str:
    base = (value or "").strip().rstrip("/")
    if not base:
        raise RuntimeError("Missing OpenMetadata base URL. Set OPENMETADATA_BASE_URL.")
    if base.endswith("/api"):
        return base
    return f"{base}/api"


def _api_root() -> str:
    return _normalize_base_url(os.getenv("OPENMETADATA_BASE_URL", ""))


def _api_url(path: str) -> str:
    cleaned = path.lstrip("/")
    if not cleaned.startswith(f"{API_VERSION_PREFIX}/"):
        cleaned = f"{API_VERSION_PREFIX}/{cleaned}"
    return f"{_api_root()}/{cleaned}"


def _login_payloads() -> list[dict[str, str]]:
    email = os.getenv("OPENMETADATA_EMAIL", "").strip()
    password = os.getenv("OPENMETADATA_PASSWORD", "")
    if not email or not password:
        raise RuntimeError(
            "Missing OpenMetadata credentials. Set OPENMETADATA_EMAIL and OPENMETADATA_PASSWORD."
        )

    encoded_password = base64.b64encode(password.encode("utf-8")).decode("ascii")
    return [
        {"email": email, "password": encoded_password},
        {"email": email, "password": password},
    ]


def _extract_token(payload: Any) -> str | None:
    if isinstance(payload, str) and payload.strip():
        return payload.strip()
    if isinstance(payload, dict):
        for key in ("accessToken", "jwtToken", "token", "id_token"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        for nested_key in ("data", "response"):
            nested = payload.get(nested_key)
            token = _extract_token(nested)
            if token:
                return token
    return None


def _login() -> str:
    jwt_token = os.getenv("OPENMETADATA_JWT_TOKEN", "").strip()
    if jwt_token:
        return jwt_token

    cached = _TOKEN_CACHE.get("token")
    if isinstance(cached, str) and cached.strip():
        return cached.strip()

    timeout = (10, 30)
    last_error: Exception | None = None
    for payload in _login_payloads():
        try:
            response = requests.post(
                _api_url(LOGIN_ENDPOINT),
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                    "User-Agent": USER_AGENT,
                },
                json=payload,
                timeout=timeout,
            )
            response.raise_for_status()
            data = response.json() if response.content else {}
            token = _extract_token(data)
            if token:
                _TOKEN_CACHE["token"] = token
                return token
            last_error = RuntimeError("OpenMetadata login succeeded but no JWT token was returned.")
        except requests.exceptions.RequestException as exc:
            last_error = exc

    raise RuntimeError(f"OpenMetadata login failed: {last_error}") from last_error


def _headers() -> dict[str, str]:
    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {_login()}",
        "User-Agent": USER_AGENT,
    }


def _clean_dict(payload: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in payload.items() if value is not None}


def _request(
    method: str,
    endpoint: str,
    payload: dict[str, Any] | None = None,
    params: dict[str, Any] | None = None,
    max_retries: int = 2,
) -> Any:
    timeout = (10, 30)
    last_error: Exception | None = None

    for attempt in range(max_retries):
        try:
            response = requests.request(
                method=method,
                url=_api_url(endpoint),
                headers=_headers(),
                json=payload,
                params=params,
                timeout=timeout,
            )
            if response.status_code == 401 and not os.getenv("OPENMETADATA_JWT_TOKEN"):
                _TOKEN_CACHE["token"] = None
                if attempt < max_retries - 1:
                    continue
            response.raise_for_status()
            if response.status_code == 204 or not response.content:
                return {"message": "Request completed", "success": True}
            return response.json()
        except requests.exceptions.RequestException as exc:
            last_error = exc
            if attempt == max_retries - 1:
                break
            time.sleep(2**attempt)

    raise RuntimeError(f"OpenMetadata API request failed: {last_error}") from last_error


def _patch_json_patch(endpoint: str, patch_ops: list[dict[str, Any]]) -> Any:
    """PATCH with JSON Patch (RFC 6902). OpenMetadata expects Content-Type application/json-patch+json."""
    timeout = (10, 30)
    last_error: Exception | None = None
    headers = dict(_headers())
    headers["Content-Type"] = "application/json-patch+json"
    for attempt in range(2):
        try:
            response = requests.request(
                method="PATCH",
                url=_api_url(endpoint),
                headers=headers,
                json=patch_ops,
                timeout=timeout,
            )
            if response.status_code == 401 and not os.getenv("OPENMETADATA_JWT_TOKEN"):
                _TOKEN_CACHE["token"] = None
                if attempt < 1:
                    continue
            response.raise_for_status()
            if response.status_code == 204 or not response.content:
                return {"message": "Request completed", "success": True}
            return response.json()
        except requests.exceptions.RequestException as exc:
            last_error = exc
            if attempt == 1:
                break
            time.sleep(2**attempt)

    raise RuntimeError(f"OpenMetadata API request failed: {last_error}") from last_error


def _request_with_fallbacks(method: str, endpoints: list[str]) -> Any:
    errors: list[str] = []
    for endpoint in endpoints:
        try:
            return _request(method, endpoint)
        except RuntimeError as exc:
            errors.append(f"{endpoint}: {exc}")
    raise RuntimeError("All OpenMetadata API fallback requests failed: " + " | ".join(errors))


def _render(payload: dict[str, Any]) -> str:
    return json.dumps(payload, indent=2, ensure_ascii=True)


def _normalize_service_type(service_type: str) -> str:
    normalized = (service_type or "").strip()
    if not normalized:
        raise RuntimeError("service_type is required.")
    return normalized[0].upper() + normalized[1:]


def _required_connection_fields(service_type: str) -> set[str]:
    normalized = _normalize_service_type(service_type)
    mapping = {
        "Snowflake": {"username", "password", "account", "warehouse"},
        "Postgres": {"username", "password", "hostPort", "database"},
        "Mysql": {"username", "password", "hostPort", "database"},
        "Mssql": {"username", "password", "hostPort", "database"},
        "Oracle": {"username", "password", "hostPort", "serviceName"},
    }
    return mapping.get(normalized, set())


def _validate_connection_config(service_type: str, connection_config: dict[str, Any]) -> None:
    required_fields = _required_connection_fields(service_type)
    if not required_fields:
        return

    missing = sorted(
        field
        for field in required_fields
        if connection_config.get(field) in (None, "", [], {})
    )
    if missing:
        raise RuntimeError(
            f"Missing required connection fields for {_normalize_service_type(service_type)}: "
            + ", ".join(missing)
        )


def _service_connection_payload(
    service_type: str,
    connection_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    config = dict(connection_config or {})
    normalized_type = _normalize_service_type(service_type)
    if "type" not in config:
        config["type"] = normalized_type
    _validate_connection_config(normalized_type, config)
    return {"config": config}


def _database_service_payload(
    name: str,
    service_type: str,
    connection_config: dict[str, Any],
    description: str | None = None,
) -> dict[str, Any]:
    normalized_type = _normalize_service_type(service_type)
    return _clean_dict(
        {
            "name": name,
            "serviceType": normalized_type,
            "connection": _service_connection_payload(normalized_type, connection_config),
            "description": description,
        }
    )


def _entity_ref(entity: dict[str, Any], default_type: str) -> dict[str, Any]:
    entity_id = entity.get("id")
    entity_name = entity.get("name") or entity.get("fullyQualifiedName")
    if not entity_id or not entity_name:
        raise RuntimeError(f"Cannot build entity reference from payload: {entity}")
    return {
        "id": entity_id,
        "type": entity.get("type") or default_type,
        "name": entity_name,
        "fullyQualifiedName": entity.get("fullyQualifiedName"),
    }


def _ingestion_pipeline_payload(
    name: str,
    service_ref: dict[str, Any],
    source_config: dict[str, Any] | None = None,
    airflow_config: dict[str, Any] | None = None,
    description: str | None = None,
    enabled: bool = True,
) -> dict[str, Any]:
    source_cfg = dict(source_config or {})
    if "config" not in source_cfg:
        source_cfg = {"config": source_cfg}
    source_cfg["config"].setdefault("type", "DatabaseMetadata")

    airflow_cfg = dict(airflow_config or {})
    airflow_cfg.setdefault("pausePipeline", not enabled)

    return _clean_dict(
        {
            "name": name,
            "pipelineType": "metadata",
            "service": service_ref,
            "sourceConfig": source_cfg,
            "airflowConfig": airflow_cfg,
            "description": description,
            "enabled": enabled,
        }
    )


def _resolve_endpoint(entity_type: str) -> str:
    endpoint = ENTITY_ENDPOINTS.get(entity_type)
    if not endpoint:
        raise RuntimeError(f"Unsupported entity type: {entity_type}")
    return endpoint


def _list_entities(entity_type: str, limit: int = 100, **params: Any) -> dict[str, Any]:
    response = _request(
        "GET",
        _resolve_endpoint(entity_type),
        params=_clean_dict({"limit": limit, **params}),
    )
    items = response.get("data", []) if isinstance(response, dict) else []
    return {
        "success": True,
        "count": len(items),
        "items": items,
    }


def _get_by_name(entity_type: str, fully_qualified_name: str, fields: str | None = None) -> dict[str, Any]:
    params = _clean_dict({"fields": fields})
    response = _request(
        "GET",
        f"{_resolve_endpoint(entity_type)}/name/{fully_qualified_name}",
        params=params or None,
    )
    return {"success": True, "entity": response}


def _get_database_service_by_name(service_name: str) -> dict[str, Any]:
    return _get_by_name("database_service", service_name).get("entity", {})


def _find_pipeline_by_name(name: str) -> dict[str, Any]:
    return _get_by_name("ingestion_pipeline", name).get("entity", {})


def _glossary_payload(
    name: str,
    display_name: str | None = None,
    description: str | None = None,
    mutually_exclusive: bool | None = None,
) -> dict[str, Any]:
    return _clean_dict(
        {
            "name": name,
            "displayName": display_name or name,
            "description": description,
            "mutuallyExclusive": mutually_exclusive,
        }
    )


def _glossary_term_payload(
    glossary_fqn: str,
    name: str,
    display_name: str | None = None,
    description: str | None = None,
    synonyms: list[str] | None = None,
    notes: str | None = None,
    status: str | None = None,
) -> dict[str, Any]:
    description_parts = [part.strip() for part in [description, notes] if part and part.strip()]
    if status and status.strip():
        description_parts.append(f"Review status: {status.strip()}.")

    return _clean_dict(
        {
            "name": name,
            "displayName": display_name or name,
            "description": "\n\n".join(description_parts) if description_parts else None,
            "glossary": glossary_fqn,
            "synonyms": synonyms or None,
        }
    )


def _tag_label(tag_fqn: str, source: str) -> dict[str, Any]:
    return {
        "tagFQN": tag_fqn,
        "source": source,
        "labelType": "Manual",
        "state": "Confirmed",
    }


def _merge_tag_labels(existing: list[dict[str, Any]] | None, additions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for label in (existing or []) + additions:
        tag_fqn = str(label.get("tagFQN") or "").strip()
        source = str(label.get("source") or "").strip()
        key = (tag_fqn, source)
        if not tag_fqn or key in seen:
            continue
        seen.add(key)
        merged.append(label)
    return merged


def _get_table_entity(table_fqn: str, fields: str = "columns,tags") -> dict[str, Any]:
    return _get_by_name("table", table_fqn, fields=fields).get("entity", {})


def _find_column_index(table_entity: dict[str, Any], column_name: str) -> int:
    columns = table_entity.get("columns") or []
    for index, column in enumerate(columns):
        if str(column.get("name", "")).lower() == column_name.lower():
            return index
    raise RuntimeError(
        f"Column '{column_name}' was not found in table '{table_entity.get('fullyQualifiedName')}'."
    )


def _assign_table_tags(table_fqn: str, tag_fqns: list[str], source: str) -> dict[str, Any]:
    table_entity = _get_table_entity(table_fqn, fields="columns,tags")
    table_id = table_entity.get("id")
    if not table_id:
        raise RuntimeError(f"Table entity missing id for '{table_fqn}'.")
    additions = [_tag_label(tag_fqn, source) for tag_fqn in tag_fqns]
    merged_tags = _merge_tag_labels(table_entity.get("tags"), additions)
    updated = _patch_json_patch(
        f"tables/{table_id}",
        [{"op": "replace", "path": "/tags", "value": merged_tags}],
    )
    return {
        "success": True,
        "table": updated.get("fullyQualifiedName") or table_fqn,
        "tags": updated.get("tags", []),
    }


def _assign_column_tags(
    table_fqn: str,
    column_name: str,
    tag_fqns: list[str],
    source: str,
) -> dict[str, Any]:
    table_entity = _get_table_entity(table_fqn, fields="columns,tags")
    table_id = table_entity.get("id")
    if not table_id:
        raise RuntimeError(f"Table entity missing id for '{table_fqn}'.")
    column_index = _find_column_index(table_entity, column_name)
    column = dict(table_entity["columns"][column_index])
    additions = [_tag_label(tag_fqn, source) for tag_fqn in tag_fqns]
    merged = _merge_tag_labels(column.get("tags"), additions)
    updated = _patch_json_patch(
        f"tables/{table_id}",
        [{"op": "replace", "path": f"/columns/{column_index}/tags", "value": merged}],
    )
    updated_column = updated.get("columns", [])[column_index]
    return {
        "success": True,
        "table": updated.get("fullyQualifiedName") or table_fqn,
        "column": updated_column.get("name") or column_name,
        "tags": updated_column.get("tags", []),
    }


@mcp.tool()
def test_connection() -> str:
    """Validate OpenMetadata auth and connectivity."""
    response = _request("GET", "users", params={"limit": 1})
    users = response.get("data", []) if isinstance(response, dict) else []
    return _render(
        {
            "success": True,
            "users_returned": len(users) if isinstance(users, list) else 0,
            "base_url": os.getenv("OPENMETADATA_BASE_URL", ""),
            "timestamp": datetime.now().isoformat(),
        }
    )


@mcp.tool()
def list_database_services(limit: int = 100) -> str:
    """List database services configured in OpenMetadata."""
    return _render(_list_entities("database_service", limit=limit))


@mcp.tool()
def get_database_service(service_name: str) -> str:
    """Fetch one database service by fully qualified name."""
    return _render({"success": True, "database_service": _get_database_service_by_name(service_name)})


@mcp.tool()
def create_database_service(
    name: str,
    service_type: str,
    connection_config: dict[str, Any],
    description: str | None = None,
) -> str:
    """Create a database service for any supported OpenMetadata database type."""
    response = _request(
        "POST",
        DATABASE_SERVICE_ENDPOINT,
        payload=_database_service_payload(
            name=name,
            service_type=service_type,
            connection_config=connection_config,
            description=description,
        ),
    )
    return _render({"success": True, "database_service": response})


@mcp.tool()
def update_database_service(
    name: str,
    service_type: str,
    connection_config: dict[str, Any],
    description: str | None = None,
) -> str:
    """Create or update a database service by name."""
    response = _request(
        "PUT",
        DATABASE_SERVICE_ENDPOINT,
        payload=_database_service_payload(
            name=name,
            service_type=service_type,
            connection_config=connection_config,
            description=description,
        ),
    )
    return _render({"success": True, "database_service": response})


@mcp.tool()
def list_ingestion_pipelines(limit: int = 100, service_name: str | None = None) -> str:
    """List ingestion pipelines, optionally filtered by service name."""
    return _render(
        _list_entities(
            "ingestion_pipeline",
            limit=limit,
            service=service_name,
        )
    )


@mcp.tool()
def get_ingestion_pipeline(pipeline_name: str) -> str:
    """Fetch one ingestion pipeline by fully qualified name."""
    return _render({"success": True, "ingestion_pipeline": _find_pipeline_by_name(pipeline_name)})


@mcp.tool()
def create_metadata_ingestion_pipeline(
    name: str,
    database_service_name: str,
    source_config: dict[str, Any] | None = None,
    airflow_config: dict[str, Any] | None = None,
    description: str | None = None,
    enabled: bool = True,
) -> str:
    """Create a metadata ingestion pipeline for a database service."""
    service = _get_database_service_by_name(database_service_name)
    response = _request(
        "POST",
        INGESTION_PIPELINE_ENDPOINT,
        payload=_ingestion_pipeline_payload(
            name=name,
            service_ref=_entity_ref(service, "databaseService"),
            source_config=source_config,
            airflow_config=airflow_config,
            description=description,
            enabled=enabled,
        ),
    )
    return _render({"success": True, "ingestion_pipeline": response})


@mcp.tool()
def update_metadata_ingestion_pipeline(
    name: str,
    database_service_name: str,
    source_config: dict[str, Any] | None = None,
    airflow_config: dict[str, Any] | None = None,
    description: str | None = None,
    enabled: bool = True,
) -> str:
    """Create or update a metadata ingestion pipeline for a database service."""
    service = _get_database_service_by_name(database_service_name)
    response = _request(
        "PUT",
        INGESTION_PIPELINE_ENDPOINT,
        payload=_ingestion_pipeline_payload(
            name=name,
            service_ref=_entity_ref(service, "databaseService"),
            source_config=source_config,
            airflow_config=airflow_config,
            description=description,
            enabled=enabled,
        ),
    )
    return _render({"success": True, "ingestion_pipeline": response})


@mcp.tool()
def run_ingestion_pipeline(pipeline_name: str) -> str:
    """Trigger an ingestion pipeline run using the first supported OpenMetadata endpoint."""
    pipeline = _find_pipeline_by_name(pipeline_name)
    pipeline_id = pipeline.get("id")
    if not pipeline_id:
        raise RuntimeError(f"Ingestion pipeline '{pipeline_name}' does not have an id.")

    response = _request_with_fallbacks(
        "POST",
        [
            f"{INGESTION_PIPELINE_ENDPOINT}/{candidate}/{pipeline_id}"
            for candidate in PIPELINE_RUN_ENDPOINT_CANDIDATES
        ],
    )
    return _render({"success": True, "ingestion_pipeline": pipeline, "run_response": response})


@mcp.tool()
def get_ingestion_status(pipeline_name: str) -> str:
    """Fetch the latest ingestion pipeline entity for status inspection."""
    pipeline = _find_pipeline_by_name(pipeline_name)
    return _render(
        {
            "success": True,
            "pipeline_name": pipeline_name,
            "ingestion_pipeline": pipeline,
            "pipeline_state": pipeline.get("pipelineStatus")
            or pipeline.get("airflowConfig")
            or pipeline.get("deployed"),
        }
    )


@mcp.tool()
def list_databases(service_name: str | None = None, limit: int = 100) -> str:
    """List databases discovered in OpenMetadata."""
    return _render(_list_entities("database", limit=limit, service=service_name))


@mcp.tool()
def list_schemas(database_name: str | None = None, limit: int = 100) -> str:
    """List database schemas discovered in OpenMetadata."""
    return _render(_list_entities("schema", limit=limit, database=database_name))


@mcp.tool()
def list_tables(database_schema: str | None = None, limit: int = 100) -> str:
    """List tables with their columns and any existing descriptions returned by OpenMetadata."""
    return _render(
        _list_entities(
            "table",
            limit=limit,
            databaseSchema=database_schema,
            fields=TABLE_ENTITY_FIELDS,
        )
    )


@mcp.tool()
def get_table(table_fqn: str) -> str:
    """Fetch one table by fully qualified name, including columns and tags."""
    return _render({"success": True, "table": _get_table_entity(table_fqn, fields=TABLE_ENTITY_FIELDS)})


@mcp.tool()
def get_column(table_fqn: str, column_name: str) -> str:
    """Fetch one column from an OpenMetadata table entity."""
    table = _get_table_entity(table_fqn, fields="columns,tags")
    column_index = _find_column_index(table, column_name)
    return _render(
        {
            "success": True,
            "table": table.get("fullyQualifiedName") or table_fqn,
            "column": table.get("columns", [])[column_index],
        }
    )


@mcp.tool()
def list_glossaries(limit: int = 100) -> str:
    """List glossaries in OpenMetadata."""
    response = _request("GET", "glossaries", params={"limit": limit})
    items = response.get("data", []) if isinstance(response, dict) else []
    return _render({"success": True, "count": len(items), "glossaries": items})


@mcp.tool()
def get_glossary(glossary_fqn: str) -> str:
    """Fetch one glossary by fully qualified name."""
    response = _request("GET", f"glossaries/name/{glossary_fqn}")
    return _render({"success": True, "glossary": response})


@mcp.tool()
def create_glossary(
    name: str,
    display_name: str | None = None,
    description: str | None = None,
    mutually_exclusive: bool = False,
) -> str:
    """Create a glossary."""
    response = _request(
        "POST",
        "glossaries",
        payload=_glossary_payload(
            name=name,
            display_name=display_name,
            description=description,
            mutually_exclusive=mutually_exclusive,
        ),
    )
    return _render({"success": True, "glossary": response})


@mcp.tool()
def update_glossary(
    name: str,
    display_name: str | None = None,
    description: str | None = None,
    mutually_exclusive: bool = False,
) -> str:
    """Create or update a glossary by name."""
    response = _request(
        "PUT",
        "glossaries",
        payload=_glossary_payload(
            name=name,
            display_name=display_name,
            description=description,
            mutually_exclusive=mutually_exclusive,
        ),
    )
    return _render({"success": True, "glossary": response})


@mcp.tool()
def list_glossary_terms(glossary_fqn: str | None = None, limit: int = 100) -> str:
    """List glossary terms, optionally filtered by glossary FQN."""
    params = _clean_dict({"glossary": glossary_fqn, "limit": limit})
    response = _request("GET", "glossaryTerms", params=params)
    items = response.get("data", []) if isinstance(response, dict) else []
    return _render({"success": True, "count": len(items), "terms": items})


@mcp.tool()
def get_glossary_term(term_fqn: str) -> str:
    """Fetch one glossary term by fully qualified name."""
    response = _request("GET", f"glossaryTerms/name/{term_fqn}")
    return _render({"success": True, "glossary_term": response})


@mcp.tool()
def create_glossary_term(
    glossary_fqn: str,
    name: str,
    display_name: str | None = None,
    description: str | None = None,
    synonyms: list[str] | None = None,
    notes: str | None = None,
    status: str = "approved",
) -> str:
    """Create a glossary term under an existing glossary."""
    if status == "rejected":
        raise RuntimeError("Rejected glossary terms are not published to OpenMetadata.")

    response = _request(
        "POST",
        "glossaryTerms",
        payload=_glossary_term_payload(
            glossary_fqn=glossary_fqn,
            name=name,
            display_name=display_name,
            description=description,
            synonyms=synonyms,
            notes=notes,
            status=status,
        ),
    )
    return _render({"success": True, "glossary_term": response})


@mcp.tool()
def update_glossary_term(
    glossary_fqn: str,
    name: str,
    display_name: str | None = None,
    description: str | None = None,
    synonyms: list[str] | None = None,
    notes: str | None = None,
    status: str = "approved",
) -> str:
    """Create or update a glossary term under an existing glossary."""
    if status == "rejected":
        raise RuntimeError("Rejected glossary terms are not published to OpenMetadata.")

    response = _request(
        "PUT",
        "glossaryTerms",
        payload=_glossary_term_payload(
            glossary_fqn=glossary_fqn,
            name=name,
            display_name=display_name,
            description=description,
            synonyms=synonyms,
            notes=notes,
            status=status,
        ),
    )
    return _render({"success": True, "glossary_term": response})


@mcp.tool()
def list_classifications(limit: int = 100) -> str:
    """List classification definitions in OpenMetadata."""
    return _render(_list_entities("classification", limit=limit))


@mcp.tool()
def list_tags(classification_name: str | None = None, limit: int = 100) -> str:
    """List tags in OpenMetadata, optionally filtered by classification name."""
    return _render(_list_entities("tag", limit=limit, classification=classification_name))


@mcp.tool()
def assign_glossary_term_to_table(table_fqn: str, glossary_term_fqn: str) -> str:
    """Assign a glossary term label to a table entity."""
    return _render(_assign_table_tags(table_fqn, [glossary_term_fqn], source="Glossary"))


@mcp.tool()
def assign_glossary_term_to_column(table_fqn: str, column_name: str, glossary_term_fqn: str) -> str:
    """Assign a glossary term label to a table column."""
    return _render(
        _assign_column_tags(
            table_fqn=table_fqn,
            column_name=column_name,
            tag_fqns=[glossary_term_fqn],
            source="Glossary",
        )
    )


@mcp.tool()
def assign_tags_to_table(table_fqn: str, tag_fqns: list[str]) -> str:
    """Assign one or more classification tags to a table entity."""
    return _render(_assign_table_tags(table_fqn, tag_fqns, source="Classification"))


@mcp.tool()
def assign_tags_to_column(table_fqn: str, column_name: str, tag_fqns: list[str]) -> str:
    """Assign one or more classification tags to a table column."""
    return _render(
        _assign_column_tags(
            table_fqn=table_fqn,
            column_name=column_name,
            tag_fqns=tag_fqns,
            source="Classification",
        )
    )


@mcp.tool()
def update_table_description(table_fqn: str, description: str) -> str:
    """Update the description of a table entity in OpenMetadata."""
    table_entity = _get_table_entity(table_fqn, fields="columns,tags")
    table_id = table_entity.get("id")
    if not table_id:
        raise RuntimeError(f"Table entity missing id for '{table_fqn}'.")
    op = "replace" if table_entity.get("description") else "add"
    updated = _patch_json_patch(
        f"tables/{table_id}",
        [{"op": op, "path": "/description", "value": description}],
    )
    return _render(
        {
            "success": True,
            "table": updated.get("fullyQualifiedName") or table_fqn,
            "description": updated.get("description", ""),
        }
    )


@mcp.tool()
def update_column_description(table_fqn: str, column_name: str, description: str) -> str:
    """Update the description of a column within a table entity in OpenMetadata."""
    table_entity = _get_table_entity(table_fqn, fields="columns,tags")
    table_id = table_entity.get("id")
    if not table_id:
        raise RuntimeError(f"Table entity missing id for '{table_fqn}'.")
    column_index = _find_column_index(table_entity, column_name)
    column = table_entity["columns"][column_index]
    op = "replace" if column.get("description") else "add"
    updated = _patch_json_patch(
        f"tables/{table_id}",
        [{"op": op, "path": f"/columns/{column_index}/description", "value": description}],
    )
    updated_column = updated.get("columns", [])[column_index]
    return _render(
        {
            "success": True,
            "table": updated.get("fullyQualifiedName") or table_fqn,
            "column": updated_column.get("name") or column_name,
            "description": updated_column.get("description", ""),
        }
    )


if __name__ == "__main__":
    mcp.run()
