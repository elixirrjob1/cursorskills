from __future__ import annotations

import json
from pathlib import Path
from typing import Any


CONFIG_FILENAME = "db-analysis-config.json"
DEFAULT_CONFIG = {
    "exclude_schemas": [],
    "exclude_tables": [],
    "max_row_limit": None,
}


def config_path() -> Path:
    return Path.cwd() / CONFIG_FILENAME


def action_required_payload(tool_name: str) -> dict[str, Any]:
    return {
        "error": "db_analysis_config_required",
        "detail": f"{CONFIG_FILENAME} was not found.",
        "tool": tool_name,
        "required_config_file": CONFIG_FILENAME,
        "instruction": (
            "You must ask the user whether to exclude any schemas, exclude any tables, "
            "and set max_row_limit. Do not assume there are no exclusions. "
            "Save the answers to db-analysis-config.json before rerunning."
        ),
        "config_template": dict(DEFAULT_CONFIG),
    }


def _normalize_string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError("Expected a list of strings.")
    normalized: list[str] = []
    for item in value:
        text = str(item or "").strip()
        if text:
            normalized.append(text)
    return normalized


def normalize_config(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("Configuration must be a JSON object.")

    max_row_limit = payload.get("max_row_limit")
    if max_row_limit in ("", None):
        normalized_limit = None
    else:
        normalized_limit = int(max_row_limit)
        if normalized_limit <= 0:
            raise ValueError("max_row_limit must be a positive integer or null.")

    return {
        "exclude_schemas": _normalize_string_list(payload.get("exclude_schemas")),
        "exclude_tables": _normalize_string_list(payload.get("exclude_tables")),
        "max_row_limit": normalized_limit,
    }


def load_config(*, tool_name: str, required: bool = True) -> dict[str, Any]:
    path = config_path()
    if not path.exists():
        return action_required_payload(tool_name) if required else dict(DEFAULT_CONFIG)

    with open(path, encoding="utf-8") as f:
        raw = json.load(f)
    return normalize_config(raw)


def should_exclude_schema(schema_name: str | None, config: dict[str, Any]) -> bool:
    schema = str(schema_name or "").strip().lower()
    excluded = {str(item).strip().lower() for item in config.get("exclude_schemas", []) if str(item).strip()}
    return bool(schema and schema in excluded)


def should_exclude_table(schema_name: str | None, table_name: str | None, config: dict[str, Any]) -> bool:
    table = str(table_name or "").strip().lower()
    schema = str(schema_name or "").strip().lower()
    if not table:
        return False

    excluded = {str(item).strip().lower() for item in config.get("exclude_tables", []) if str(item).strip()}
    if table in excluded:
        return True
    if schema and f"{schema}.{table}" in excluded:
        return True
    return False


def apply_sample_row_limit(limit: int, config: dict[str, Any]) -> int:
    configured = config.get("max_row_limit")
    base_limit = max(1, int(limit))
    if configured is None:
        return base_limit
    return max(1, min(base_limit, int(configured)))
