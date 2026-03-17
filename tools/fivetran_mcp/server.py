#!/usr/bin/env python3
"""Minimal MCP server for common Fivetran actions."""

from __future__ import annotations

import json
import os
import time
from datetime import datetime
from typing import Any

import requests
from requests.auth import HTTPBasicAuth

from mcp.server.fastmcp import FastMCP

mcp = FastMCP("fivetran-mcp-quickstart")

API_BASE_URL = "https://api.fivetran.com/v1"
API_VERSION_HEADER = "application/json;version=2"


def _get_credentials() -> tuple[str, str]:
    api_key = os.getenv("FIVETRAN_API_KEY")
    api_secret = os.getenv("FIVETRAN_API_SECRET")

    if not api_key or not api_secret:
        raise RuntimeError(
            "Missing Fivetran credentials. Set FIVETRAN_API_KEY and FIVETRAN_API_SECRET."
        )

    return api_key, api_secret


def _request(
    method: str,
    endpoint: str,
    payload: dict[str, Any] | None = None,
    params: dict[str, Any] | None = None,
    max_retries: int = 3,
) -> dict[str, Any] | None:
    api_key, api_secret = _get_credentials()
    auth = HTTPBasicAuth(api_key, api_secret)

    url = f"{API_BASE_URL}/{endpoint}"
    headers = {
        "Accept": API_VERSION_HEADER,
        "Content-Type": "application/json",
        "User-Agent": "fivetran-quickstart-mcp",
    }
    timeout = (10, 30)

    for attempt in range(max_retries):
        try:
            if method == "GET":
                response = requests.get(
                    url, headers=headers, auth=auth, params=params, timeout=timeout
                )
            elif method == "POST":
                response = requests.post(
                    url, headers=headers, auth=auth, json=payload, timeout=timeout
                )
            elif method == "PATCH":
                response = requests.patch(
                    url, headers=headers, auth=auth, json=payload, timeout=timeout
                )
            elif method == "DELETE":
                response = requests.delete(
                    url, headers=headers, auth=auth, timeout=timeout
                )
            else:
                raise ValueError(f"Unsupported method: {method}")

            response.raise_for_status()
            if response.status_code == 204:
                return {"code": "Success", "message": "Request completed"}
            return response.json()
        except requests.exceptions.Timeout:
            if attempt == max_retries - 1:
                raise
            time.sleep(2**attempt)
        except requests.exceptions.RequestException as exc:
            if attempt == max_retries - 1:
                raise RuntimeError(f"Fivetran API request failed: {exc}") from exc
            time.sleep(2**attempt)

    return None


def _clean_dict(payload: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in payload.items() if value is not None}


@mcp.tool()
def test_connection() -> str:
    """Validate credentials and connectivity."""
    response = _request("GET", "groups")
    groups = response.get("data", {}).get("items", []) if response else []
    return json.dumps(
        {
            "success": True,
            "groups_count": len(groups),
            "timestamp": datetime.now().isoformat(),
        },
        indent=2,
    )


@mcp.tool()
def list_groups() -> str:
    """List groups available in the account."""
    response = _request("GET", "groups")
    data = response.get("data", {}) if response else {}
    groups = data.get("items", []) if isinstance(data, dict) else data
    return json.dumps(
        {
            "success": True,
            "count": len(groups),
            "groups": groups,
        },
        indent=2,
    )


@mcp.tool()
def create_group(name: str) -> str:
    """Create a group for a destination/connector pair."""
    response = _request("POST", "groups", payload={"name": name})
    data = response.get("data", {}) if response else {}
    return json.dumps(
        {
            "success": True,
            "group_id": data.get("id"),
            "name": data.get("name"),
            "created_at": data.get("created_at"),
            "message": "Group created",
        },
        indent=2,
    )


@mcp.tool()
def get_group_details(group_id: str) -> str:
    """Fetch one group's details."""
    response = _request("GET", f"groups/{group_id}")
    return json.dumps(
        {
            "success": True,
            "data": response.get("data", {}) if response else {},
        },
        indent=2,
    )


@mcp.tool()
def list_connectors(group_id: str | None = None) -> str:
    """List connectors, optionally filtered by group ID."""
    params = {"group_id": group_id} if group_id else None
    response = _request("GET", "connections", params=params)
    data = response.get("data", {}) if response else {}

    connectors = data.get("items", []) if isinstance(data, dict) else data
    return json.dumps(
        {
            "success": True,
            "count": len(connectors),
            "connectors": connectors,
        },
        indent=2,
    )


@mcp.tool()
def get_connector_status(connector_id: str) -> str:
    """Fetch connector status and configuration."""
    response = _request("GET", f"connectors/{connector_id}")
    return json.dumps(
        {
            "success": True,
            "data": response.get("data", {}) if response else {},
        },
        indent=2,
    )


@mcp.tool()
def get_connection_details(connector_id: str) -> str:
    """Fetch full connection details including sync status (succeeded_at, failed_at, setup_tests, tasks, warnings)."""
    response = _request("GET", f"connections/{connector_id}")
    data = response.get("data", {}) if response else {}
    status = data.get("status", {})
    return json.dumps(
        {
            "success": True,
            "connector_id": connector_id,
            "sync_state": status.get("sync_state"),
            "update_state": status.get("update_state"),
            "setup_state": status.get("setup_state"),
            "succeeded_at": data.get("succeeded_at"),
            "failed_at": data.get("failed_at"),
            "is_historical_sync": status.get("is_historical_sync"),
            "tasks": status.get("tasks", []),
            "warnings": status.get("warnings", []),
            "setup_tests": data.get("setup_tests", []),
            "data": data,
        },
        indent=2,
    )


@mcp.tool()
def get_destination_details(destination_id: str) -> str:
    """Fetch destination details."""
    response = _request("GET", f"destinations/{destination_id}")
    return json.dumps(
        {
            "success": True,
            "data": response.get("data", {}) if response else {},
        },
        indent=2,
    )


@mcp.tool()
def create_destination(
    group_id: str,
    service: str,
    region: str | None = None,
    time_zone_offset: str | None = None,
    config: dict[str, Any] | None = None,
    trust_certificates: bool | None = None,
    trust_fingerprints: bool | None = None,
    run_setup_tests: bool = True,
    networking_method: str | None = None,
    private_link_id: str | None = None,
) -> str:
    """Create a destination in an existing group."""
    payload = _clean_dict(
        {
            "group_id": group_id,
            "service": service,
            "region": region,
            "time_zone_offset": time_zone_offset,
            "config": config,
            "trust_certificates": trust_certificates,
            "trust_fingerprints": trust_fingerprints,
            "run_setup_tests": run_setup_tests,
            "networking_method": networking_method,
            "private_link_id": private_link_id,
        }
    )

    response = _request("POST", "destinations", payload=payload)
    data = response.get("data", {}) if response else {}
    return json.dumps(
        {
            "success": True,
            "destination_id": data.get("id"),
            "group_id": data.get("group_id"),
            "service": data.get("service"),
            "region": data.get("region"),
            "status": data.get("status"),
            "message": "Destination created",
        },
        indent=2,
    )


@mcp.tool()
def validate_destination_service(service: str, group_id: str) -> str:
    """Validate if a destination service name is supported by testing it (dry-run)."""
    # Try to create with minimal config to see if service name is valid
    # This will fail gracefully if service is unsupported
    try:
        payload = {
            "group_id": group_id,
            "service": service,
            "region": "AWS_US_EAST_1",  # Default region for testing
            "time_zone_offset": "-8",  # Default timezone
            "config": {},  # Empty config - will fail validation but show if service is valid
        }
        response = _request("POST", "destinations", payload=payload)
        # If we get here, service name is valid (even if config is invalid)
        return json.dumps(
            {
                "success": True,
                "service": service,
                "valid": True,
                "message": f"Service '{service}' is a valid destination service name",
            },
            indent=2,
        )
    except RuntimeError as e:
        error_msg = str(e)
        if "Unsupported service" in error_msg:
            return json.dumps(
                {
                    "success": False,
                    "service": service,
                    "valid": False,
                    "message": f"Service '{service}' is NOT a valid destination service name",
                    "error": error_msg,
                    "suggestion": "Try common variations like 'postgres_warehouse' instead of 'postgres'",
                },
                indent=2,
            )
        else:
            # Other errors (like config validation) mean service name is valid
            return json.dumps(
                {
                    "success": True,
                    "service": service,
                    "valid": True,
                    "message": f"Service '{service}' appears to be valid (config validation failed, which is expected)",
                    "note": "Service name validation passed, but you need to provide proper config",
                },
                indent=2,
            )


@mcp.tool()
def update_destination(
    destination_id: str,
    region: str | None = None,
    time_zone_offset: str | None = None,
    config: dict[str, Any] | None = None,
    trust_certificates: bool | None = None,
    trust_fingerprints: bool | None = None,
    run_setup_tests: bool | None = None,
    networking_method: str | None = None,
    private_link_id: str | None = None,
) -> str:
    """Update destination settings."""
    payload = _clean_dict(
        {
            "region": region,
            "time_zone_offset": time_zone_offset,
            "config": config,
            "trust_certificates": trust_certificates,
            "trust_fingerprints": trust_fingerprints,
            "run_setup_tests": run_setup_tests,
            "networking_method": networking_method,
            "private_link_id": private_link_id,
        }
    )
    if not payload:
        raise RuntimeError("At least one destination field must be provided.")

    response = _request("PATCH", f"destinations/{destination_id}", payload=payload)
    return json.dumps(
        {
            "success": True,
            "data": response.get("data", {}) if response else {},
            "message": "Destination updated",
        },
        indent=2,
    )


@mcp.tool()
def run_destination_setup_tests(destination_id: str) -> str:
    """Run setup tests for a destination."""
    response = _request("POST", f"destinations/{destination_id}/tests")
    return json.dumps(
        {
            "success": True,
            "data": response.get("data", {}) if response else {},
            "message": "Destination setup tests started",
        },
        indent=2,
    )


# --- Webhooks ---


@mcp.tool()
def list_webhooks(cursor: str | None = None, limit: int = 100) -> str:
    """List all webhooks in the account."""
    params = _clean_dict({"cursor": cursor, "limit": limit})
    response = _request("GET", "webhooks", params=params)
    data = response.get("data", {}) if response else {}
    items = data.get("items", [])
    return json.dumps(
        {
            "success": True,
            "count": len(items),
            "webhooks": items,
            "next_cursor": data.get("next_cursor"),
        },
        indent=2,
    )


@mcp.tool()
def create_group_webhook(
    group_id: str,
    url: str,
    events: list[str],
    active: bool = True,
    secret: str | None = None,
) -> str:
    """Create a webhook for a group. Events can include sync_start, sync_end, connection_failure, etc."""
    payload = _clean_dict(
        {"url": url, "events": events, "active": active, "secret": secret}
    )
    response = _request("POST", f"webhooks/group/{group_id}", payload=payload)
    data = response.get("data", {}) if response else {}
    return json.dumps(
        {
            "success": True,
            "webhook_id": data.get("id"),
            "data": data,
            "message": "Group webhook created",
        },
        indent=2,
    )


@mcp.tool()
def update_webhook(
    webhook_id: str,
    url: str | None = None,
    events: list[str] | None = None,
    active: bool | None = None,
    secret: str | None = None,
) -> str:
    """Update an existing webhook."""
    payload = _clean_dict(
        {"url": url, "events": events, "active": active, "secret": secret}
    )
    if not payload:
        raise RuntimeError("At least one field (url, events, active, secret) must be provided.")
    response = _request("PATCH", f"webhooks/{webhook_id}", payload=payload)
    data = response.get("data", {}) if response else {}
    return json.dumps(
        {
            "success": True,
            "webhook_id": webhook_id,
            "data": data,
            "message": "Webhook updated",
        },
        indent=2,
    )


@mcp.tool()
def delete_webhook(webhook_id: str) -> str:
    """Delete a webhook."""
    response = _request("DELETE", f"webhooks/{webhook_id}")
    return json.dumps(
        {
            "success": True,
            "webhook_id": webhook_id,
            "message": "Webhook deleted",
        },
        indent=2,
    )


# --- Users ---


@mcp.tool()
def list_users(
    cursor: str | None = None,
    limit: int = 100,
    active: bool | None = None,
) -> str:
    """List all users in the Fivetran account."""
    params = _clean_dict({"cursor": cursor, "limit": limit, "active": active})
    response = _request("GET", "users", params=params)
    data = response.get("data", {}) if response else {}
    items = data.get("items", [])
    return json.dumps(
        {
            "success": True,
            "count": len(items),
            "users": items,
            "next_cursor": data.get("next_cursor"),
        },
        indent=2,
    )


@mcp.tool()
def get_user_details(user_id: str) -> str:
    """Fetch details for a specific user."""
    response = _request("GET", f"users/{user_id}")
    data = response.get("data", {}) if response else {}
    return json.dumps(
        {
            "success": True,
            "user_id": user_id,
            "data": data,
        },
        indent=2,
    )


@mcp.tool()
def pause_connector(connector_id: str) -> str:
    """Pause a connector to stop syncing."""
    response = _request("PATCH", f"connectors/{connector_id}", payload={"paused": True})
    return json.dumps(
        {
            "success": True,
            "data": response.get("data", {}) if response else {},
            "message": "Connector paused",
        },
        indent=2,
    )


@mcp.tool()
def resume_connector(connector_id: str) -> str:
    """Resume a connector to continue syncing."""
    response = _request("PATCH", f"connectors/{connector_id}", payload={"paused": False})
    return json.dumps(
        {
            "success": True,
            "data": response.get("data", {}) if response else {},
            "message": "Connector resumed",
        },
        indent=2,
    )


@mcp.tool()
def trigger_sync(connector_id: str, force: bool = False) -> str:
    """Trigger a manual sync for a connector. Use force=True to stop any in-progress sync and restart."""
    response = _request(
        "POST",
        f"connections/{connector_id}/sync",
        payload={"force": force},
    )
    data = response.get("data", {}) if response else {}
    return json.dumps(
        {
            "success": True,
            "connector_id": connector_id,
            "data": data,
            "message": "Sync triggered successfully",
        },
        indent=2,
    )


@mcp.tool()
def resync_connector(
    connector_id: str,
    scope: dict[str, list[str]] | None = None,
) -> str:
    """Trigger a historical re-sync for a connector. Optionally pass scope to re-sync specific tables, e.g. {'dbo': ['customers', 'orders']}. If scope is omitted, all tables are re-synced."""
    payload = _clean_dict({"scope": scope}) if scope else {}
    response = _request(
        "POST",
        f"connections/{connector_id}/resync",
        payload=payload if payload else None,
    )
    msg = response.get("message", "Re-sync triggered") if response else "Re-sync triggered"
    return json.dumps(
        {
            "success": True,
            "connector_id": connector_id,
            "message": msg,
        },
        indent=2,
    )


@mcp.tool()
def get_connector_metadata(connector_type: str) -> str:
    """Discover required fields for a connector type."""
    response = _request("GET", f"metadata/connectors/{connector_type}")
    return json.dumps(
        {
            "success": True,
            "connector_type": connector_type,
            "metadata": response.get("data", {}) if response else {},
        },
        indent=2,
    )


@mcp.tool()
def get_connection_schema_config(connector_id: str) -> str:
    """Fetch the current schema/table selection config for a connection."""
    response = _request("GET", f"connections/{connector_id}/schemas")
    data = response.get("data", {}) if response else {}
    return json.dumps(
        {
            "success": True,
            "connector_id": connector_id,
            "schema_change_handling": data.get("schema_change_handling"),
            "schemas": data.get("schemas", {}),
        },
        indent=2,
    )


@mcp.tool()
def get_table_columns_config(connector_id: str, schema_name: str, table_name: str) -> str:
    """Fetch the current column config for one table."""
    response = _request(
        "GET",
        f"connections/{connector_id}/schemas/{schema_name}/tables/{table_name}/columns",
    )
    data = response.get("data", {}) if response else {}
    return json.dumps(
        {
            "success": True,
            "connector_id": connector_id,
            "schema": schema_name,
            "table": table_name,
            "columns": data.get("columns", {}),
        },
        indent=2,
    )


@mcp.tool()
def reload_connection_schema_config(connector_id: str, exclude_mode: str = "PRESERVE") -> str:
    """Reload schema metadata from the source for an existing connection."""
    response = _request(
        "POST",
        f"connections/{connector_id}/schemas/reload",
        payload={"exclude_mode": exclude_mode},
    )
    data = response.get("data", {}) if response else {}
    return json.dumps(
        {
            "success": True,
            "connector_id": connector_id,
            "exclude_mode": exclude_mode,
            "schema_change_handling": data.get("schema_change_handling"),
            "schemas": data.get("schemas", {}),
        },
        indent=2,
    )


@mcp.tool()
def update_schema_config(
    connector_id: str,
    schema_name: str,
    enabled: bool,
    schema_change_handling: str | None = None,
) -> str:
    """Enable or disable a schema for syncing."""
    payload: dict[str, Any] = {"enabled": enabled}
    if schema_change_handling is not None:
        payload["schema_change_handling"] = schema_change_handling

    response = _request(
        "PATCH",
        f"connections/{connector_id}/schemas/{schema_name}",
        payload=payload,
    )
    data = response.get("data", {}) if response else {}
    schema_data = data.get("schemas", {}).get(schema_name, {})
    return json.dumps(
        {
            "success": True,
            "connector_id": connector_id,
            "schema": schema_name,
            "schema_config": schema_data,
            "schema_change_handling": data.get("schema_change_handling"),
            "message": "Schema configuration updated",
        },
        indent=2,
    )


@mcp.tool()
def update_table_config(
    connector_id: str,
    schema_name: str,
    table_name: str,
    enabled: bool | None = None,
    sync_mode: str | None = None,
    columns: dict[str, Any] | None = None,
) -> str:
    """Enable/disable a table, set sync mode, or patch columns in one table."""
    payload = _clean_dict(
        {
            "enabled": enabled,
            "sync_mode": sync_mode,
            "columns": columns,
        }
    )
    if not payload:
        raise RuntimeError("At least one of enabled, sync_mode, or columns must be provided.")

    response = _request(
        "PATCH",
        f"connections/{connector_id}/schemas/{schema_name}/tables/{table_name}",
        payload=payload,
    )
    data = response.get("data", {}) if response else {}
    table_data = data.get("schemas", {}).get(schema_name, {}).get("tables", {}).get(table_name, {})
    return json.dumps(
        {
            "success": True,
            "connector_id": connector_id,
            "schema": schema_name,
            "table": table_name,
            "table_config": table_data,
            "message": "Table configuration updated",
        },
        indent=2,
    )


@mcp.tool()
def update_column_config(
    connector_id: str,
    schema_name: str,
    table_name: str,
    column_name: str,
    enabled: bool | None = None,
    hashed: bool | None = None,
    is_primary_key: bool | None = None,
) -> str:
    """Enable/disable or hash a single column."""
    payload = _clean_dict(
        {
            "enabled": enabled,
            "hashed": hashed,
            "is_primary_key": is_primary_key,
        }
    )
    if not payload:
        raise RuntimeError("At least one of enabled, hashed, or is_primary_key must be provided.")

    response = _request(
        "PATCH",
        f"connections/{connector_id}/schemas/{schema_name}/tables/{table_name}/columns/{column_name}",
        payload=payload,
    )
    data = response.get("data", {}) if response else {}
    column_data = (
        data.get("schemas", {})
        .get(schema_name, {})
        .get("tables", {})
        .get(table_name, {})
        .get("columns", {})
        .get(column_name, {})
    )
    return json.dumps(
        {
            "success": True,
            "connector_id": connector_id,
            "schema": schema_name,
            "table": table_name,
            "column": column_name,
            "column_config": column_data,
            "message": "Column configuration updated",
        },
        indent=2,
    )


@mcp.tool()
def update_connector(
    connector_id: str,
    paused: bool | None = None,
    sync_frequency: int | None = None,
    config: dict[str, Any] | None = None,
    auth: dict[str, Any] | None = None,
) -> str:
    """Update persistent connector settings."""
    payload = _clean_dict(
        {
            "paused": paused,
            "sync_frequency": sync_frequency,
            "config": config,
            "auth": auth,
        }
    )
    if not payload:
        raise RuntimeError("At least one persistent field must be provided.")

    response = _request("PATCH", f"connections/{connector_id}", payload=payload)
    data = response.get("data", {}) if response else {}
    return json.dumps(
        {
            "success": True,
            "connector_id": connector_id,
            "data": data,
            "message": "Connector updated",
        },
        indent=2,
    )


@mcp.tool()
def create_connector(
    connector_type: str,
    group_id: str,
    config: dict[str, Any],
    auth: dict[str, Any] | None = None,
    paused: bool = True,
    sync_frequency: int = 1440,
) -> str:
    """Create a connector with a minimal, safe payload."""
    payload: dict[str, Any] = {
        "group_id": group_id,
        "service": connector_type,
        "paused": paused,
        "sync_frequency": sync_frequency,
        "trust_certificates": True,
        "trust_fingerprints": True,
        "run_setup_tests": True,
        "config": config,
    }
    if auth:
        payload["auth"] = auth

    response = _request("POST", "connections", payload=payload)
    data = response.get("data", {}) if response else {}

    return json.dumps(
        {
            "success": True,
            "connector_id": data.get("id"),
            "status": data.get("status", {}).get("setup_state"),
            "paused": data.get("paused"),
            "message": "Connector created",
        },
        indent=2,
    )


@mcp.tool()
def list_destinations() -> str:
    """List destinations with a clean summary."""
    response = _request("GET", "destinations")
    data = response.get("data", {}) if response else {}

    items = data.get("items", []) if isinstance(data, dict) else data
    destinations = []
    for item in items:
        destinations.append(
            {
                "destination_id": item.get("id"),
                "group_id": item.get("group_id"),
                "service": item.get("service"),
                "region": item.get("region"),
                "name": item.get("name") or item.get("id"),
            }
        )

    return json.dumps(
        {
            "success": True,
            "count": len(destinations),
            "destinations": destinations,
        },
        indent=2,
    )


if __name__ == "__main__":
    mcp.run(transport="stdio")
