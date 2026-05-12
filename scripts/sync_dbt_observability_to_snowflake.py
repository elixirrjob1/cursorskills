#!/usr/bin/env python3
"""
Sync dbt Cloud run metadata, per-step console/debug logs, and run_results.json
nodes into Snowflake tables (see scripts/dbt_observability/ddl.sql).

Prerequisites:
  - dbt: DBT_ACCOUNT_ID, DBT_HOST, and OAuth (mcp.yml) or DBT_PAT (see dbt_cloud_client).
  - Snowflake: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USERNAME, SNOWFLAKE_PASSWORD,
    SNOWFLAKE_WAREHOUSE, SNOWFLAKE_ROLE, SNOWFLAKE_DATABASE
  - Optional: SNOWFLAKE_DBT_OBSERVABILITY_SCHEMA (default DBT_OBSERVABILITY)

Usage:
  python3 scripts/sync_dbt_observability_to_snowflake.py --runs 15
  python3 scripts/sync_dbt_observability_to_snowflake.py --runs 5 --job 12345 --dry-run
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path

import requests

_REPO = Path(__file__).resolve().parent.parent
_SCRIPTS = Path(__file__).resolve().parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from dbt_cloud_client import (  # noqa: E402
    api_base,
    api_get,
    api_get_run_with_steps,
    api_get_step_with_logs,
    load_env_from_repo_root,
)

try:
    import snowflake.connector
except ImportError:
    print("Install: pip install snowflake-connector-python", file=sys.stderr)
    sys.exit(1)

MAX_LOG_CHARS = 15_000_000
MAX_COMPILED = 1_000_000
MAX_MESSAGE = 1_000_000


def _dbt_api_user_message(exc: requests.HTTPError) -> str | None:
    resp = exc.response
    if resp is None:
        return None
    try:
        body = resp.json()
        st = body.get("status") or {}
        return st.get("user_message") or st.get("developer_message")
    except Exception:
        return None


def _load_env_full() -> None:
    """Populate os.environ from repo .env (overwrites empty only via setdefault in dbt client)."""
    load_env_from_repo_root()
    env_path = _REPO / ".env"
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                k, v = k.strip(), v.strip()
                if k:
                    os.environ.setdefault(k, v)


def _validate_ident(name: str, label: str) -> str:
    n = name.strip()
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", n):
        raise SystemExit(f"Invalid {label} identifier: {name!r} (use letters, numbers, underscore)")
    return n.upper()


def _trunc(s: str | None, n: int) -> str | None:
    if s is None:
        return None
    if len(s) <= n:
        return s
    return s[: max(0, n - 40)] + "\n\n...[truncated]...\n"


def _parse_ntz(value: str | None) -> datetime | None:
    if not value:
        return None
    dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return dt.replace(tzinfo=None)


def _snowflake_connect():
    _load_env_full()

    def _first(*keys: str) -> str:
        for k in keys:
            v = os.environ.get(k, "").strip()
            if v:
                return v
        return ""

    account = _first("SNOWFLAKE_ACCOUNT")
    user = _first("SNOWFLAKE_USERNAME", "SNOWFLAKE_USER", "SNOWFLAKE_FIVETRAN_USER")
    password = _first("SNOWFLAKE_PASSWORD", "SNOWFLAKE_FIVETRAN_PASSWORD")
    warehouse = _first("SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_FIVETRAN_WAREHOUSE", "SNOWFLAKE_SQL_API_EXECUTION_WAREHOUSE")
    database = _first(
        "SNOWFLAKE_DATABASE",
        "SNOWFLAKE_FIVETRAN_DATABASE",
        "SNOWFLAKE_DRIP_DATABASE",
    )
    role = _first("SNOWFLAKE_ROLE", "SNOWFLAKE_FIVETRAN_ROLE")
    if not all([account, user, password, warehouse, database]):
        print(
            "ERROR: Snowflake connection env missing. Set account, user, password, warehouse, database — e.g.\n"
            "  SNOWFLAKE_ACCOUNT, SNOWFLAKE_USERNAME (or SNOWFLAKE_USER / SNOWFLAKE_FIVETRAN_USER),\n"
            "  SNOWFLAKE_PASSWORD (or SNOWFLAKE_FIVETRAN_PASSWORD),\n"
            "  SNOWFLAKE_WAREHOUSE (or SNOWFLAKE_FIVETRAN_WAREHOUSE),\n"
            "  SNOWFLAKE_DATABASE (or SNOWFLAKE_DRIP_DATABASE / SNOWFLAKE_FIVETRAN_DATABASE).",
            file=sys.stderr,
        )
        sys.exit(1)
    kwargs: dict = {
        "account": account,
        "user": user,
        "password": password,
        "warehouse": warehouse,
        "database": database,
    }
    if role:
        kwargs["role"] = role
    return snowflake.connector.connect(**kwargs)


def _apply_ddl(conn, database: str, obs_schema: str) -> None:
    ddl_path = _REPO / "scripts/dbt_observability/ddl.sql"
    raw = ddl_path.read_text(encoding="utf-8")
    substituted = raw.replace("{{OBS_SCHEMA}}", f"{database}.{obs_schema}")
    statements = [s.strip() for s in substituted.split(";") if s.strip()]
    with conn.cursor() as cur:
        for stmt in statements:
            cur.execute(stmt)


def _fetch_job_map() -> dict[int, str]:
    """Best-effort job id → name. PATs are often not allowed to call /jobs/; then we return {}."""
    try:
        jobs_resp = api_get("/jobs/", {"limit": 100})
        return {int(j["id"]): j.get("name") or f"job-{j['id']}" for j in jobs_resp.get("data", [])}
    except requests.HTTPError as e:
        code = e.response.status_code if e.response is not None else None
        if code in (400, 403, 404):
            print(
                f"warn: GET /jobs/ failed ({code}); job names will come from run payloads where available",
                file=sys.stderr,
            )
            return {}
        raise


def _job_name_from_run_payload(run: dict) -> str | None:
    """Resolve human-readable job name from a run dict (include_related job/trigger)."""
    if not run:
        return None
    job = run.get("job")
    if isinstance(job, dict):
        n = job.get("name")
        if n:
            return str(n)[:512]
    jd = run.get("job_definition")
    if isinstance(jd, dict):
        n = jd.get("name")
        if n:
            return str(n)[:512]
    trig = run.get("trigger") or {}
    if isinstance(trig, dict):
        tj = trig.get("job")
        if isinstance(tj, dict) and tj.get("name"):
            return str(tj["name"])[:512]
    return None


def _enrich_job_map_from_runs(job_map: dict[int, str], runs: list[dict]) -> dict[int, str]:
    out = dict(job_map)
    for run in runs:
        raw = run.get("job_definition_id")
        if raw is None:
            continue
        jid = int(raw)
        if out.get(jid):
            continue
        name = _job_name_from_run_payload(run)
        if name:
            out[jid] = name
    return out


def _list_runs(limit: int, job_id: int | None) -> list[dict]:
    base: dict = {"limit": limit, "order_by": "-id"}
    if job_id is not None:
        base["job_definition_id"] = job_id
    # Prefer richer payloads; narrow if PAT/API rejects include_related.
    related_sets: list[list[str]] = [["trigger", "job"], ["trigger"], []]
    for related in related_sets:
        params = dict(base)
        if related:
            params["include_related"] = json.dumps(related)
        try:
            runs_resp = api_get("/runs/", params)
            return list(runs_resp.get("data") or [])
        except requests.HTTPError as e:
            code = e.response.status_code if e.response is not None else None
            if related and code in (400, 403) and related != related_sets[-1]:
                print(
                    f"warn: include_related {related} rejected ({code}); retrying with a narrower request",
                    file=sys.stderr,
                )
                continue
            um = _dbt_api_user_message(e)
            if um:
                print(f"dbt API: {um}", file=sys.stderr)
            if code == 403:
                print(
                    "hint: Use a dbt Cloud service token scoped to this account, or refresh OAuth: "
                    "cd dbt_project/drip_transformations && uvx dbt-mcp auth",
                    file=sys.stderr,
                )
            raise
    return []


def _safe_int(v):
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        try:
            return int(float(v))
        except (TypeError, ValueError):
            return None


def _fetch_run_results(run_id: int) -> list[dict]:
    try:
        data = api_get(f"/runs/{run_id}/artifacts/run_results.json", timeout=180)
        return list(data.get("results") or [])
    except Exception as e:
        print(f"  warn: run_results for {run_id}: {e}", file=sys.stderr)
        return []


def _result_key(run_id: int, unique_id: str) -> str:
    h = hashlib.sha256(f"{run_id}:{unique_id}".encode("utf-8")).hexdigest()
    return h[:64]


def _resource_parts(unique_id: str) -> tuple[str, str]:
    """Return (resource_type, short_name)."""
    parts = unique_id.split(".")
    if len(parts) >= 2:
        return parts[0], parts[-1]
    return "unknown", unique_id


def sync_run(conn, fq: str, account_id: str, job_map: dict[int, str], run_summary: dict, dry_run: bool) -> None:
    run_id = int(run_summary["id"])
    detail = api_get_run_with_steps(run_id)
    run = detail.get("data") or run_summary
    raw_jid = run.get("job_definition_id") or run_summary.get("job_definition_id")
    jid = int(raw_jid) if raw_jid is not None else None
    job_name = None
    if jid is not None:
        job_name = job_map.get(jid) or _job_name_from_run_payload(run_summary) or _job_name_from_run_payload(run)
        if not job_name:
            job_name = f"job-{jid}"

    run_duration_ms = run.get("duration") or run.get("duration_ms")
    queued_ms = run.get("queued_duration") or run.get("queued_duration_ms")

    def _to_int_ms(v):
        if v is None:
            return None
        try:
            return int(float(v))
        except (TypeError, ValueError):
            return None

    run_row = (
        run_id,
        str(account_id),
        jid,
        (job_name or "")[:512] if job_name else None,
        run.get("environment_id"),
        run.get("project_id"),
        run.get("status"),
        (run.get("status_humanized") or "")[:128],
        bool(run.get("is_error")),
        bool(run.get("is_cancelled")),
        bool(run.get("should_full_refresh")),
        (run.get("git_branch") or run.get("branch") or "")[:512],
        (run.get("git_sha") or run.get("sha") or "")[:128],
        _parse_ntz(run.get("created_at")),
        _parse_ntz(run.get("updated_at")),
        _parse_ntz(run.get("started_at")),
        _parse_ntz(run.get("finished_at")),
        _to_int_ms(run_duration_ms),
        _to_int_ms(queued_ms),
    )

    merge_run = f"""
MERGE INTO {fq}.DBT_RUNS t
USING (
  SELECT
    %s AS run_id, %s AS account_id, %s AS job_definition_id, %s AS job_name,
    %s AS environment_id, %s AS project_id, %s AS status, %s AS status_humanized,
    %s AS is_error, %s AS is_cancelled, %s AS should_full_refresh,
    %s AS git_branch, %s AS git_sha,
    %s AS created_at, %s AS updated_at, %s AS started_at, %s AS finished_at,
    %s AS run_duration_ms, %s AS queued_duration_ms
) s
ON t.run_id = s.run_id
WHEN MATCHED THEN UPDATE SET
  account_id = s.account_id, job_definition_id = s.job_definition_id, job_name = s.job_name,
  environment_id = s.environment_id, project_id = s.project_id, status = s.status,
  status_humanized = s.status_humanized, is_error = s.is_error, is_cancelled = s.is_cancelled,
  should_full_refresh = s.should_full_refresh, git_branch = s.git_branch, git_sha = s.git_sha,
  created_at = s.created_at, updated_at = s.updated_at, started_at = s.started_at,
  finished_at = s.finished_at, run_duration_ms = s.run_duration_ms, queued_duration_ms = s.queued_duration_ms,
  loaded_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
  run_id, account_id, job_definition_id, job_name, environment_id, project_id, status, status_humanized,
  is_error, is_cancelled, should_full_refresh, git_branch, git_sha, created_at, updated_at, started_at,
  finished_at, run_duration_ms, queued_duration_ms
) VALUES (
  s.run_id, s.account_id, s.job_definition_id, s.job_name, s.environment_id, s.project_id, s.status, s.status_humanized,
  s.is_error, s.is_cancelled, s.should_full_refresh, s.git_branch, s.git_sha, s.created_at, s.updated_at, s.started_at,
  s.finished_at, s.run_duration_ms, s.queued_duration_ms
)
"""

    steps_payload: list[tuple] = []
    for step in sorted(run.get("run_steps") or [], key=lambda x: int(x.get("index") or 0)):
        sid = int(step["id"])
        try:
            step_detail = api_get_step_with_logs(sid).get("data") or step
        except Exception as e:
            print(f"  warn: step {sid} logs: {e}", file=sys.stderr)
            step_detail = step
        logs = _trunc(step_detail.get("logs"), MAX_LOG_CHARS)
        dbg = _trunc(step_detail.get("debug_logs"), MAX_LOG_CHARS)
        steps_payload.append(
            (
                sid,
                run_id,
                int(step_detail.get("index") or step.get("index") or 0),
                (step_detail.get("name") or step.get("name") or "")[:512],
                str(step_detail.get("status") or "")[:64],
                (step_detail.get("status_humanized") or "")[:128],
                _parse_ntz(step_detail.get("started_at")),
                _parse_ntz(step_detail.get("finished_at")),
                logs,
                dbg,
            )
        )

    results = _fetch_run_results(run_id)
    node_rows: list[tuple] = []
    for r in results:
        uid = str(r.get("unique_id") or "")
        if not uid:
            continue
        rtype, shortn = _resource_parts(uid)
        ar = r.get("adapter_response") or {}
        compiled = _trunc(r.get("compiled_code") or r.get("compiled_sql"), MAX_COMPILED)
        node_rows.append(
            (
                _result_key(run_id, uid),
                run_id,
                uid[:512],
                rtype[:64],
                shortn[:512],
                str(r.get("status") or "")[:64],
                float(r.get("execution_time") or 0) if r.get("execution_time") is not None else None,
                _trunc(str(r.get("message") or ""), MAX_MESSAGE),
                _safe_int(ar.get("rows_affected") or ar.get("num_rows_affected")),
                _safe_int(ar.get("num_rows_inserted")),
                _safe_int(ar.get("num_rows_updated")),
                _safe_int(ar.get("num_rows_deleted")),
                str(r.get("thread_id") or "")[:64],
                compiled,
            )
        )

    if dry_run:
        print(f"[dry-run] run {run_id}: {len(steps_payload)} steps, {len(node_rows)} node results", file=sys.stderr)
        return

    with conn.cursor() as cur:
        cur.execute(merge_run, run_row)
        cur.execute(f"DELETE FROM {fq}.DBT_RUN_STEPS WHERE run_id = %s", (run_id,))
        if steps_payload:
            cur.executemany(
                f"""
INSERT INTO {fq}.DBT_RUN_STEPS (
  step_id, run_id, step_index, step_name, status, status_humanized,
  started_at, finished_at, logs_text, debug_logs_text
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
""",
                steps_payload,
            )
        cur.execute(f"DELETE FROM {fq}.DBT_NODE_RESULTS WHERE run_id = %s", (run_id,))
        if node_rows:
            cur.executemany(
                f"""
INSERT INTO {fq}.DBT_NODE_RESULTS (
  result_key, run_id, unique_id, resource_type, node_name, status,
  execution_time_seconds, message, rows_affected, rows_inserted, rows_updated, rows_deleted,
  thread_id, compiled_sql
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
""",
                node_rows,
            )


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync dbt Cloud observability into Snowflake")
    parser.add_argument("--runs", type=int, default=10, help="Number of recent runs to sync")
    parser.add_argument("--job", type=int, default=None, help="Filter to a single job_definition_id")
    parser.add_argument("--dry-run", action="store_true", help="Fetch from dbt API only; do not write Snowflake")
    args = parser.parse_args()

    _load_env_full()
    _, _, account_id = api_base()
    database = _validate_ident(
        os.environ.get("SNOWFLAKE_DATABASE", "").strip()
        or os.environ.get("SNOWFLAKE_FIVETRAN_DATABASE", "").strip()
        or os.environ.get("SNOWFLAKE_DRIP_DATABASE", "").strip(),
        "SNOWFLAKE_DATABASE",
    )
    obs_schema = _validate_ident(
        os.environ.get("SNOWFLAKE_DBT_OBSERVABILITY_SCHEMA", "DBT_OBSERVABILITY").strip(),
        "SNOWFLAKE_DBT_OBSERVABILITY_SCHEMA",
    )
    fq = f"{database}.{obs_schema}"

    job_map = _fetch_job_map()
    runs = _list_runs(args.runs, args.job)
    job_map = _enrich_job_map_from_runs(job_map, runs)
    print(f"Syncing {len(runs)} run(s) into {fq}.* (account {account_id})...", file=sys.stderr)

    if args.dry_run:
        for run in runs:
            sync_run(None, fq, account_id, job_map, run, dry_run=True)
        return

    conn = _snowflake_connect()
    try:
        _apply_ddl(conn, database, obs_schema)
        for run in runs:
            rid = run.get("id")
            print(f"  run {rid} ({run.get('status_humanized', '?')})", file=sys.stderr)
            sync_run(conn, fq, account_id, job_map, run, dry_run=False)
        conn.commit()
        print("Done.", file=sys.stderr)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
