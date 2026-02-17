"""API routes: list tables and get table data as JSON."""

import os
from fastapi import APIRouter, Depends, HTTPException, Query

from api.auth import require_bearer_token
from api import db

router = APIRouter(prefix="/api", tags=["data"])


def _schema() -> str:
    return os.environ.get("SCHEMA", "public").strip() or "public"


@router.get("/tables")
async def list_tables(_: None = Depends(require_bearer_token)):
    """List table names in the configured schema."""
    try:
        schema = _schema()
        tables = db.get_tables(schema)
        return {"schema": schema, "tables": tables}
    except Exception as e:
        raise HTTPException(status_code=500, detail="Database error") from e


@router.get("/{table}")
async def get_table(
    table: str,
    _: None = Depends(require_bearer_token),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """Return rows from the given table as JSON. Supports limit and offset."""
    schema = _schema()
    try:
        tables = db.get_tables(schema)
    except Exception as e:
        raise HTTPException(status_code=500, detail="Database error") from e
    if table not in tables:
        raise HTTPException(status_code=404, detail="Table not found")
    try:
        rows = db.get_table_data(table, schema, limit=limit, offset=offset)
        return {"schema": schema, "table": table, "data": rows}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail="Database error") from e
