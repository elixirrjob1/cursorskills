"""Azure Functions entrypoint for the simulated API."""

import json
import os
import sys
from pathlib import Path

import azure.functions as func

_root = Path(__file__).resolve().parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from scripts.keyvault_loader import load_env

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)
_initialized = False
_db = None


def _init() -> None:
    global _initialized, _db
    if _initialized:
        return
    from sqlalchemy import create_engine
    from api import db as db_module

    load_env()
    if not os.environ.get("DATABASE_URL"):
        raise RuntimeError("DATABASE_URL is not set (Key Vault or local settings)")
    if not os.environ.get("API_AUTH_TOKEN"):
        raise RuntimeError("API_AUTH_TOKEN is not set (Key Vault or local settings)")
    database_url = os.environ["DATABASE_URL"]
    connect_args = {"timeout": 10} if database_url.startswith("mssql+") else {"connect_timeout": 10}
    engine = create_engine(
        database_url,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
        connect_args=connect_args,
    )
    db_module.set_engine(engine)
    _db = db_module
    _initialized = True


def _json_response(payload: dict, status: int = 200) -> func.HttpResponse:
    return func.HttpResponse(
        body=json.dumps(payload, default=str),
        status_code=status,
        mimetype="application/json",
    )


def _unauthorized() -> func.HttpResponse:
    return _json_response({"detail": "Unauthorized"}, status=401)


def _validate_bearer(req: func.HttpRequest) -> bool:
    token = os.environ.get("API_AUTH_TOKEN")
    if not token:
        return False
    auth = req.headers.get("authorization", "")
    if not auth.lower().startswith("bearer "):
        return False
    return auth[7:].strip() == token


def _schema() -> str:
    return os.environ.get("SCHEMA", "public").strip() or "public"


@app.route(route="api/tables", methods=["GET"])
def list_tables(req: func.HttpRequest) -> func.HttpResponse:
    try:
        _init()
    except Exception as exc:
        return _json_response({"detail": str(exc)}, status=503)
    if not _validate_bearer(req):
        return _unauthorized()
    schema = _schema()
    try:
        tables = _db.get_tables_metadata(schema)
        return _json_response({"schema": schema, "tables": tables})
    except Exception:
        return _json_response({"detail": "Database error"}, status=500)


@app.route(route="api/{table}", methods=["GET"])
def get_table(req: func.HttpRequest) -> func.HttpResponse:
    try:
        _init()
    except Exception as exc:
        return _json_response({"detail": str(exc)}, status=503)
    if not _validate_bearer(req):
        return _unauthorized()

    table = (req.route_params.get("table") or "").strip()
    if table == "tables":
        return list_tables(req)
    if not table:
        return _json_response({"detail": "Table not found"}, status=404)

    try:
        limit = int(req.params.get("limit", "100"))
        offset = int(req.params.get("offset", "0"))
    except ValueError:
        return _json_response({"detail": "limit and offset must be integers"}, status=400)
    if limit < 1 or limit > 1000:
        return _json_response({"detail": "limit must be between 1 and 1000"}, status=400)
    if offset < 0:
        return _json_response({"detail": "offset must be >= 0"}, status=400)

    schema = _schema()
    try:
        metadata = _db.get_table_metadata(schema, table)
    except Exception:
        return _json_response({"detail": "Database error"}, status=500)
    if metadata is None:
        return _json_response({"detail": "Table not found"}, status=404)

    resolved_table = _db.resolve_table_name(schema, table)
    if not resolved_table:
        return _json_response({"detail": "Table not found"}, status=404)

    try:
        rows = _db.get_table_data(resolved_table, schema, limit=limit, offset=offset)
        return _json_response(
            {
                "schema": schema,
                "table": resolved_table,
                "metadata": metadata,
                "data": rows,
            }
        )
    except ValueError as exc:
        return _json_response({"detail": str(exc)}, status=400)
    except Exception:
        return _json_response({"detail": "Database error"}, status=500)
