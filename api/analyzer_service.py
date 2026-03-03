"""Shared analyzer-compatible document builder for API surfaces."""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path
from threading import Lock
from time import monotonic
from types import ModuleType
from typing import Any


_ANALYZER_CACHE_TTL_SECONDS = 300
_analyzer_cache_lock = Lock()
_analyzer_cache: dict[str, dict[str, Any]] = {}
_analyzer_module: ModuleType | None = None


class AnalyzerSchemaError(ValueError):
    """Raised when the requested schema cannot produce an analyzer document."""


def _load_analyzer_module() -> ModuleType:
    global _analyzer_module
    if _analyzer_module is not None:
        return _analyzer_module

    script_path = (
        Path(__file__).resolve().parent.parent
        / ".cursor"
        / "skills"
        / "source-system-analyser"
        / "scripts"
        / "source_system_analyzer.py"
    )
    spec = importlib.util.spec_from_file_location("source_system_analyzer", script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load analyzer module from {script_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    _analyzer_module = module
    return module


def _cache_key(schema: str, include_sample_data: bool, dialect_override: str | None) -> str:
    dialect = dialect_override or ""
    return f"{schema}|samples={int(include_sample_data)}|dialect={dialect}"


def get_analyzer_document(
    schema: str,
    *,
    include_sample_data: bool = False,
    dialect_override: str | None = None,
) -> dict[str, Any]:
    """Return an analyzer-compatible schema document for the configured database."""
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set")

    cache_key = _cache_key(schema, include_sample_data, dialect_override)
    now = monotonic()
    with _analyzer_cache_lock:
        existing = _analyzer_cache.get(cache_key)
        if existing and now < float(existing["expires_at"]):
            return existing["document"]

    module = _load_analyzer_module()
    document = module.build_source_system_document(
        database_url,
        schema=schema,
        include_sample_data=include_sample_data,
        dialect_override=dialect_override,
    )
    if document.get("error") == "No tables found":
        raise AnalyzerSchemaError(f"No tables found for schema '{schema}'")

    with _analyzer_cache_lock:
        _analyzer_cache[cache_key] = {
            "document": document,
            "expires_at": now + _ANALYZER_CACHE_TTL_SECONDS,
        }
    return document
