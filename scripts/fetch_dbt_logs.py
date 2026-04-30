#!/usr/bin/env python3
"""
Fetch full dbt Cloud run logs in one shot via the REST API.

Outputs a JSON object to stdout with two arrays:
  model_runs: [{executed_at, job, model, status, exec_time, rows_affected,
                rows_inserted, rows_updated, rows_deleted}]
  test_runs:  [{executed_at, job, model, test, status, exec_time}]

Usage:
  python3 scripts/fetch_dbt_logs.py              # last 10 runs across all jobs
  python3 scripts/fetch_dbt_logs.py --runs 20    # last 20 runs
  python3 scripts/fetch_dbt_logs.py --job 123    # specific job id only
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

try:
    import requests
except ImportError:
    print("requests not installed — run: pip install requests", file=sys.stderr)
    sys.exit(1)

# ── Load .env ────────────────────────────────────────────────────────────────
def _load_env():
    env_path = Path(__file__).parent.parent / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip())

_load_env()

ACCOUNT_ID = os.environ.get("DBT_ACCOUNT_ID", "70471823552613")
DBT_HOST   = os.environ.get("DBT_HOST", "rm291.us1.dbt.com")
BASE = f"https://{DBT_HOST}/api/v2/accounts/{ACCOUNT_ID}"

def _get_access_token() -> str:
    """
    Get a valid access token. Priority:
    1. Refresh via mcp.yml OAuth tokens (auto-refreshes if not expired)
    2. Fall back to DBT_PAT env var

    If both fail with 401, run: uvx dbt-mcp auth
    (re-authenticates and writes fresh tokens to mcp.yml)
    """
    import re
    mcp_yml = Path(__file__).parent.parent / "dbt_project/drip_transformations/mcp.yml"
    if mcp_yml.exists():
        try:
            raw = mcp_yml.read_text()
            refresh   = re.search(r'refresh_token:\s*(\S+)', raw)
            client_id = re.search(r'client_id:\s*(\S+)', raw)
            host_pfx  = re.search(r'host_prefix:\s*(\S+)', raw)
            if refresh and client_id and host_pfx:
                r = requests.post(
                    f"https://{host_pfx.group(1)}.us1.dbt.com/oauth/token",
                    data={"grant_type": "refresh_token",
                          "refresh_token": refresh.group(1),
                          "client_id": client_id.group(1)},
                    timeout=15,
                )
                if r.status_code == 200:
                    tok = r.json().get("access_token", "")
                    if tok:
                        print("Auth: refreshed OAuth token from mcp.yml", file=sys.stderr)
                        return tok
                elif r.status_code == 400:
                    print("Auth: mcp.yml refresh token expired — run: uvx dbt-mcp auth", file=sys.stderr)
        except Exception as e:
            print(f"Auth: mcp.yml read failed ({e})", file=sys.stderr)

    pat = os.environ.get("DBT_PAT", "")
    if not pat:
        print("ERROR: No valid auth found.", file=sys.stderr)
        print("  Fix: run  uvx dbt-mcp auth  in the dbt_project directory", file=sys.stderr)
        print("  Then re-run this script.", file=sys.stderr)
        sys.exit(1)
    print("Auth: using DBT_PAT from .env (may be stale)", file=sys.stderr)
    return pat

_token  = _get_access_token()
HEADERS = {"Authorization": f"Bearer {_token}", "Content-Type": "application/json"}

# ── Helpers ──────────────────────────────────────────────────────────────────
def get(path, params=None):
    r = requests.get(f"{BASE}{path}", headers=HEADERS, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def fmt_ts(ts: str) -> str:
    """ISO timestamp → 'YYYY-MM-DD HH:MM:SS UTC'"""
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        dt = dt.astimezone(timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return ts or ""

def short_name(unique_id: str) -> str:
    """model.drip_transformations.FactSales → FactSales"""
    return unique_id.split(".")[-1] if "." in unique_id else unique_id

def fmt_time(t) -> str:
    try:
        return f"{float(t):.2f}s"
    except Exception:
        return "—"

def row_val(v) -> str:
    if v is None:
        return "—"
    try:
        return str(int(v))
    except Exception:
        return "—"

# ── Main logic ───────────────────────────────────────────────────────────────
def fetch_runs(num_runs: int, job_id: int | None):
    """Return list of recent run dicts with job_name attached."""
    # Fetch job name map
    jobs_resp = get("/jobs/", {"limit": 100})
    job_map = {j["id"]: j["name"] for j in jobs_resp.get("data", [])}

    params = {"limit": num_runs, "order_by": "-id", "include_related": ["trigger"]}
    if job_id:
        params["job_definition_id"] = job_id
    runs_resp = get("/runs/", params)
    runs = runs_resp.get("data", [])

    for run in runs:
        jid = run.get("job_definition_id") or run.get("trigger", {}).get("job_definition_id")
        run["_job_name"] = job_map.get(jid, f"job-{jid}")
    return runs

def fetch_run_results(run_id: int):
    """Fetch run_results.json artifact for a run. Returns list of result dicts."""
    try:
        data = get(f"/runs/{run_id}/artifacts/run_results.json")
        return data.get("results", [])
    except Exception as e:
        print(f"  warn: run {run_id} artifacts unavailable ({e})", file=sys.stderr)
        return []

def parse_results(run, results):
    """Split results into model_runs and test_runs."""
    model_runs, test_runs = [], []
    executed_at = fmt_ts(run.get("finished_at") or run.get("created_at") or "")
    job_name = run["_job_name"]

    for r in results:
        uid = r.get("unique_id", "")
        status = r.get("status", "")
        exec_time = fmt_time(r.get("execution_time"))
        ar = r.get("adapter_response") or {}

        if uid.startswith("model.") or uid.startswith("snapshot."):
            rows_affected = row_val(ar.get("rows_affected") or ar.get("num_rows_affected"))
            rows_inserted = row_val(ar.get("num_rows_inserted"))
            rows_updated  = row_val(ar.get("num_rows_updated"))
            rows_deleted  = row_val(ar.get("num_rows_deleted"))
            model_runs.append({
                "executed_at":   executed_at,
                "job":           job_name,
                "model":         short_name(uid),
                "status":        status,
                "exec_time":     exec_time,
                "rows_affected": rows_affected,
                "rows_inserted": rows_inserted,
                "rows_updated":  rows_updated,
                "rows_deleted":  rows_deleted,
            })

        elif uid.startswith("test."):
            # derive parent model from test unique_id or node info
            node = r.get("node") or {}
            attached = node.get("attached_node", uid)
            model_runs_append = short_name(attached)
            test_name = short_name(uid)
            test_runs.append({
                "executed_at": executed_at,
                "job":         job_name,
                "model":       model_runs_append,
                "test":        test_name,
                "status":      status,
                "exec_time":   exec_time,
            })

    return model_runs, test_runs

def main():
    parser = argparse.ArgumentParser(description="Fetch dbt Cloud run logs")
    parser.add_argument("--runs", type=int, default=10, help="Number of recent runs to fetch (default 10)")
    parser.add_argument("--job",  type=int, default=None, help="Limit to a specific job ID")
    args = parser.parse_args()

    print(f"Fetching last {args.runs} runs from dbt Cloud account {ACCOUNT_ID}...", file=sys.stderr)
    runs = fetch_runs(args.runs, args.job)
    print(f"Found {len(runs)} runs. Fetching artifacts...", file=sys.stderr)

    all_model_runs, all_test_runs = [], []
    for run in runs:
        rid = run["id"]
        print(f"  run {rid} ({run['_job_name']}) status={run.get('status_humanized','?')}", file=sys.stderr)
        results = fetch_run_results(rid)
        mr, tr = parse_results(run, results)
        all_model_runs.extend(mr)
        all_test_runs.extend(tr)

    output = {
        "model_runs": all_model_runs,
        "test_runs":  all_test_runs,
        "meta": {
            "runs_fetched": len(runs),
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            "account_id":   ACCOUNT_ID,
        }
    }
    print(json.dumps(output, indent=2))

if __name__ == "__main__":
    main()
