"""API routes: list tables and get table data with structural metadata."""

import os
from fastapi import APIRouter, Depends, HTTPException, Query

from api.auth import require_bearer_token
from api import db

router = APIRouter(prefix="/api", tags=["data"])


def _default_schema() -> str:
    return os.environ.get("SCHEMA", "public").strip() or "public"


def _resolve_schema(schema: str | None) -> str:
    if schema and schema.strip():
        return schema.strip()
    return _default_schema()


@router.get("/tables")
async def list_tables(
    _: None = Depends(require_bearer_token),
    schema: str | None = Query(None, description="Database schema to query; defaults to SCHEMA env or 'public'."),
):
    """List tables with schema.json-compatible structural metadata."""
    schema_name = _resolve_schema(schema)
    try:
        tables = db.get_tables_metadata(schema_name)
        return {"schema": schema_name, "tables": tables}
    except Exception as e:
        raise HTTPException(status_code=500, detail={"detail": "Database error", "schema": schema_name}) from e


@router.get("/{table}")
async def get_table(
    table: str,
    _: None = Depends(require_bearer_token),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    schema: str | None = Query(None, description="Database schema to query; defaults to SCHEMA env or 'public'."),
):
    """Return rows and metadata from the given table. Supports limit and offset."""
    schema_name = _resolve_schema(schema)
    try:
        metadata = db.get_table_metadata(schema_name, table)
    except Exception as e:
        raise HTTPException(status_code=500, detail={"detail": "Database error", "schema": schema_name}) from e
    if metadata is None:
        raise HTTPException(status_code=404, detail={"detail": "Table not found", "schema": schema_name})

    resolved_table = db.resolve_table_name(schema_name, table)
    if not resolved_table:
        raise HTTPException(status_code=404, detail={"detail": "Table not found", "schema": schema_name})
    try:
        rows = db.get_table_data(resolved_table, schema_name, limit=limit, offset=offset)
        return {
            "schema": schema_name,
            "table": resolved_table,
            "metadata": metadata,
            "data": rows,
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail={"detail": str(e), "schema": schema_name}) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail={"detail": "Database error", "schema": schema_name}) from e
