"""
Fivetran Connector SDK — dbt Cloud bronze_dbt_logs
Tables:
  stg_dbt_runs             — one row per dbt Cloud run
  stg_dbt_run_steps        — one row per step within a run (includes raw LOGS text)
  stg_dbt_run_artifacts    — artifact paths per run
  stg_dbt_run_model_results — one row per model execution, parsed from step LOGS

Auth:   Bearer token via env var DBT_SERVICE_TOKEN
Config: base_url, account_id (resolved from env vars by default)
"""

import json
import os
import re
from datetime import datetime, timezone
from typing import Generator

import requests
from fivetran_connector_sdk import Connector, Logging as log, Operations as op


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_BASE_URL = "https://kd329.us1.dbt.com"
PAGE_SIZE = 100
REQUEST_TIMEOUT = 30


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _headers(config: dict) -> dict:
    token = config.get("dbt_service_token") or os.environ.get("DBT_SERVICE_TOKEN", "")
    if not token:
        raise ValueError("DBT_SERVICE_TOKEN not found in config or environment")
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def _account_id(config: dict) -> str:
    aid = config.get("account_id") or os.environ.get("DBT_ACCOUNT_ID", "")
    if not aid:
        raise ValueError("account_id not found in config or environment")
    return str(aid)


def _base_url(config: dict) -> str:
    raw = (config.get("base_url") or os.environ.get("DBT_BASE_URL") or DEFAULT_BASE_URL).rstrip("/")
    # Strip a trailing /api segment so callers can pass either form
    if raw.endswith("/api"):
        raw = raw[:-4]
    return raw


def _get(url: str, headers: dict, params: dict | None = None) -> dict:
    resp = requests.get(url, headers=headers, params=params, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def _paginate(base_url: str, path: str, headers: dict, extra_params: dict | None = None) -> Generator[dict, None, None]:
    """Yield individual data items from a paginated dbt Cloud list endpoint."""
    offset = 0
    while True:
        params = {"limit": PAGE_SIZE, "offset": offset, **(extra_params or {})}
        body = _get(f"{base_url}{path}", headers, params)
        data = body.get("data") or []
        if not data:
            break
        for item in data:
            yield item
        # dbt Cloud returns extra.pagination.total_count when present
        total = (body.get("extra") or {}).get("pagination", {}).get("total_count")
        offset += len(data)
        if total is not None and offset >= total:
            break
        if len(data) < PAGE_SIZE:
            break


# Column names whose string values should be cast to timezone-aware datetime
_TS_COL = re.compile(r'(_at|_date|_time)$', re.IGNORECASE)


def _parse_ts(value: str) -> datetime | str:
    """Parse an ISO-8601 string to a timezone-aware datetime; return original string on failure."""
    if not isinstance(value, str) or not value:
        return value
    try:
        dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return value


def _flatten(record: dict) -> dict:
    """Flatten nested keys to JSON strings; cast timestamp columns to datetime objects."""
    out = {}
    for k, v in record.items():
        if isinstance(v, (dict, list)):
            out[k] = json.dumps(v)
        elif _TS_COL.search(k) and isinstance(v, str):
            out[k] = _parse_ts(v)
        else:
            out[k] = v
    return out


def _cursor_key(table: str) -> str:
    return f"{table}_cursor"


# ---------------------------------------------------------------------------
# Log parser — extracts per-model results from a dbt step LOGS string
# ---------------------------------------------------------------------------

_RE_ANSI = re.compile(r'\x1b\[[0-9;]*m')


def _strip_ansi(text: str) -> str:
    return _RE_ANSI.sub('', text)


# After stripping ANSI codes the result lines look like:
#   "12:41:00      INFO    Succeeded [   1.3s] model     DBT_PROD.vw_FactSales (view)"
#   "12:41:00      INFO      Errored [   1.78s] model     DBT_PROD.vw_DimDate (view)"
#   "12:41:00      INFO      Skipped [    0.0s] model     DBT_PROD.vw_DimDate (view)"
_RE_RESULT = re.compile(
    r'(Succeeded|Errored|Skipped)\s+\[\s*([\d.]+)s\]\s+model\s+'
    r'([\w]+)\.([\w]+)\s+\((\w+)\)',
    re.IGNORECASE,
)

# Error blocks: "ERROR dbt1302: Database Error in model vw_DimDate (...)"
_RE_ERROR_HEADER = re.compile(
    r'ERROR\s+(dbt\d+):\s+(.+?)\s+in model\s+(\w+)',
    re.IGNORECASE,
)
_RE_SNOWFLAKE_ERR = re.compile(
    r'\[Snowflake\]\s+(\d+)\s+\((\w+)\):\s+(.+)',
    re.IGNORECASE,
)


def _parse_model_results(step_id: int, run_id: int, account_id: int, logs: str) -> list[dict]:
    """Parse a dbt step LOGS string into one dict per model result."""
    if not logs:
        return []

    logs = _strip_ansi(logs)

    # Build a lookup of model_name → (error_code, error_message) from ERROR blocks
    error_lookup: dict[str, tuple[str, str]] = {}
    for m in _RE_ERROR_HEADER.finditer(logs):
        err_code, err_type, model_name = m.group(1), m.group(2), m.group(3)
        # Try to find the Snowflake error detail that follows this block
        snippet = logs[m.end():m.end() + 400]
        sf = _RE_SNOWFLAKE_ERR.search(snippet)
        if sf:
            detail = f"[{sf.group(1)}] ({sf.group(2)}): {sf.group(3).strip()}"
        else:
            detail = err_type.strip()
        error_lookup[model_name] = (err_code, detail)

    results = []
    for m in _RE_RESULT.finditer(logs):
        status_raw, duration_s, schema, model_name, mat_type = (
            m.group(1), m.group(2), m.group(3), m.group(4), m.group(5)
        )
        status = status_raw.lower()
        err_code, err_msg = error_lookup.get(model_name, (None, None))

        results.append({
            "step_id":       step_id,
            "run_id":        run_id,
            "account_id":    account_id,
            "model_schema":  schema,
            "model_name":    model_name,
            "model_type":    mat_type.lower(),
            "status":        status,
            "duration_s":    float(duration_s),
            "error_code":    err_code,
            "error_message": err_msg,
        })
    return results


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

def schema(configuration: dict):
    # columns dict format: {"col_name": "TYPE_STRING"}
    # Valid type strings: BOOLEAN, SHORT, INT, LONG, DECIMAL, FLOAT, DOUBLE,
    #                     NAIVE_DATE, NAIVE_DATETIME, UTC_DATETIME, BINARY, XML, STRING, JSON
    _TS   = "UTC_DATETIME"  # → TIMESTAMP_TZ(9) in Snowflake
    _INT  = "LONG"          # → NUMBER(38,0)
    _BOOL = "BOOLEAN"

    return [
        {
            "table": "stg_dbt_runs",
            "primary_key": ["id"],
            "columns": {
                "id":                    _INT,
                "account_id":            _INT,
                "project_id":            _INT,
                "environment_id":        _INT,
                "job_definition_id":     _INT,
                "job_id":                _INT,
                "trigger_id":            _INT,
                "status":                _INT,
                "created_at":            _TS,
                "updated_at":            _TS,
                "dequeued_at":           _TS,
                "started_at":            _TS,
                "finished_at":           _TS,
                "last_heartbeat_at":     _TS,
                "should_start_at":       _TS,
                "in_progress":           _BOOL,
                "is_complete":           _BOOL,
                "is_success":            _BOOL,
                "is_error":              _BOOL,
                "is_cancelled":          _BOOL,
                "artifacts_saved":       _BOOL,
                "has_docs_generated":    _BOOL,
                "has_sources_generated": _BOOL,
                "notifications_sent":    _BOOL,
                "can_retry":             _BOOL,
            },
        },
        {
            "table": "stg_dbt_run_steps",
            "primary_key": ["id"],
            "columns": {
                "id":          _INT,
                "run_id":      _INT,
                "account_id":  _INT,
                "index":       _INT,
                "status":      _INT,
                "created_at":  _TS,
                "updated_at":  _TS,
                "started_at":  _TS,
                "finished_at": _TS,
            },
        },
        {
            "table": "stg_dbt_run_artifacts",
            "primary_key": ["run_id", "artifact_path"],
            "columns": {
                "run_id":     _INT,
                "account_id": _INT,
                "_synced_at": _TS,
            },
        },
        {
            "table": "stg_dbt_run_model_results",
            "primary_key": ["step_id", "model_schema", "model_name"],
            "columns": {
                "step_id":       _INT,
                "run_id":        _INT,
                "account_id":    _INT,
                "model_schema":  "STRING",
                "model_name":    "STRING",
                "model_type":    "STRING",
                "status":        "STRING",
                "duration_s":    "DOUBLE",
                "error_code":    "STRING",
                "error_message": "STRING",
            },
        },
    ]


# ---------------------------------------------------------------------------
# Update
# ---------------------------------------------------------------------------

def update(configuration: dict, state: dict):
    headers = _headers(configuration)
    account = _account_id(configuration)
    base = _base_url(configuration)

    # -----------------------------------------------------------------------
    # stg_dbt_runs — incremental via updated_at cursor
    # -----------------------------------------------------------------------
    run_ids_to_fetch_steps = []

    # dbt Cloud runs API does not support order_by; use id as cursor (runs are immutable
    # once complete — new runs always have higher ids)
    runs_cursor_id = int(state.get(_cursor_key("stg_dbt_runs"), 0))
    max_run_id = runs_cursor_id
    log.info(f"Syncing stg_dbt_runs (cursor_id={runs_cursor_id})")
    path = f"/api/v2/accounts/{account}/runs/"

    for run in _paginate(base, path, headers):
        run_id_val = run.get("id") or 0

        # Skip runs we've already processed
        if run_id_val <= runs_cursor_id:
            continue

        flat = _flatten(run)
        flat.setdefault("account_id", int(account))
        yield op.upsert("stg_dbt_runs", flat)

        if run_id_val > max_run_id:
            max_run_id = run_id_val
            new_runs_cursor = str(max_run_id)

        run_ids_to_fetch_steps.append(run_id_val)

    if max_run_id > runs_cursor_id:
        new_runs_cursor = str(max_run_id)
        yield op.checkpoint({**state, _cursor_key("stg_dbt_runs"): new_runs_cursor})
        state = {**state, _cursor_key("stg_dbt_runs"): new_runs_cursor}

    # -----------------------------------------------------------------------
    # stg_dbt_run_steps — fetched for each new/updated run
    # -----------------------------------------------------------------------
    log.info(f"Syncing stg_dbt_run_steps for {len(run_ids_to_fetch_steps)} runs")
    for run_id in run_ids_to_fetch_steps:
        run_detail_path = f"/api/v2/accounts/{account}/runs/{run_id}/"
        try:
            body = _get(f"{base}{run_detail_path}", headers,
                        {"include_related": '["run_steps"]'})
            steps = (body.get("data") or {}).get("run_steps") or []
            for step in steps:
                if isinstance(step, dict):
                    flat = _flatten(step)
                    flat.setdefault("run_id", run_id)
                    flat.setdefault("account_id", int(account))
                    yield op.upsert("stg_dbt_run_steps", flat)

                    # Parse model-level results from log text
                    raw_logs = step.get("logs") or ""
                    for model_row in _parse_model_results(
                        step_id=step.get("id"),
                        run_id=run_id,
                        account_id=int(account),
                        logs=raw_logs,
                    ):
                        yield op.upsert("stg_dbt_run_model_results", model_row)
        except Exception as e:
            log.warning(f"Could not fetch steps for run {run_id}: {e}")

    # -----------------------------------------------------------------------
    # stg_dbt_run_artifacts — full reload (no incremental column)
    # -----------------------------------------------------------------------
    artifacts_cursor = state.get(_cursor_key("stg_dbt_run_artifacts"))

    # Only re-fetch artifacts for runs that are complete (is_complete=True) and
    # were updated since last cursor, using run_ids already collected above.
    log.info("Syncing stg_dbt_run_artifacts")
    for run_id in run_ids_to_fetch_steps:
        artifact_path = f"/api/v2/accounts/{account}/runs/{run_id}/artifacts/"
        try:
            body = _get(f"{base}{artifact_path}", headers)
            paths = body.get("data") or []
            for artifact in paths:
                if isinstance(artifact, str):
                    yield op.upsert("stg_dbt_run_artifacts", {
                        "run_id": run_id,
                        "artifact_path": artifact,
                        "account_id": int(account),
                        "_synced_at": _now_utc(),
                    })
                elif isinstance(artifact, dict):
                    flat = _flatten(artifact)
                    flat.setdefault("run_id", run_id)
                    flat.setdefault("account_id", int(account))
                    yield op.upsert("stg_dbt_run_artifacts", flat)
        except Exception as e:
            log.warning(f"Could not fetch artifacts for run {run_id}: {e}")

    yield op.checkpoint({**state, _cursor_key("stg_dbt_run_artifacts"): _now_utc().isoformat()})


# ---------------------------------------------------------------------------
# Connector entry point
# ---------------------------------------------------------------------------

connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    import os as _os
    connector.debug(configuration={
        "base_url":          _os.environ.get("DBT_BASE_URL", DEFAULT_BASE_URL),   # no trailing /api
        "account_id":        _os.environ.get("DBT_ACCOUNT_ID", ""),
        "dbt_service_token": _os.environ.get("DBT_SERVICE_TOKEN", ""),
    })
