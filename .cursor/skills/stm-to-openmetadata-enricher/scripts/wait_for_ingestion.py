"""Poll an OpenMetadata ingestion pipeline until its latest run completes.

Usage:
    python wait_for_ingestion.py --pipeline-id <uuid> [--timeout 300]

Exits 0 on pipelineState=success, 1 on failure/timeout. Will trigger one
retry of the DAG if no new run appears within ~30 seconds of start.
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timezone

from om_client import api, login


def _fetch_status(token: str, pipeline_id: str) -> dict | None:
    resp = api(
        "GET",
        f"/api/v1/services/ingestionPipelines/{pipeline_id}?fields=pipelineStatuses",
        token,
    )
    return resp.get("pipelineStatuses") or None


def _trigger(token: str, pipeline_id: str) -> str:
    resp = api(
        "POST",
        f"/api/v1/services/ingestionPipelines/trigger/{pipeline_id}",
        token,
    )
    return resp.get("reason", "(no reason)")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pipeline-id", required=True)
    ap.add_argument("--timeout", type=int, default=300, help="Seconds to wait for completion.")
    ap.add_argument("--poll", type=int, default=10, help="Poll interval in seconds.")
    args = ap.parse_args()

    _base, token = login()

    baseline = _fetch_status(token, args.pipeline_id)
    baseline_start = baseline.get("startDate") if baseline else 0
    print(f"baseline lastRun startDate: {baseline_start}")

    print("triggering pipeline...")
    print(_trigger(token, args.pipeline_id))

    deadline = time.time() + args.timeout
    retriggered = False

    while time.time() < deadline:
        time.sleep(args.poll)
        status = _fetch_status(token, args.pipeline_id)
        if not status:
            print("  (no status yet)")
            continue
        start = status.get("startDate", 0)
        state = status.get("pipelineState")
        iso = datetime.fromtimestamp(start / 1000, tz=timezone.utc).isoformat() if start else "?"
        print(f"  state={state} startDate={iso}")

        if start <= baseline_start:
            # Still pointing at the pre-trigger run. If ~30 s passed with no new run, retrigger once.
            if not retriggered and time.time() > (deadline - args.timeout + 30):
                print("no new run queued yet; retriggering once...")
                print(_trigger(token, args.pipeline_id))
                retriggered = True
            continue

        if state in ("success", "partialSuccess"):
            summary = status.get("status", [])
            print(f"pipeline succeeded: {summary}")
            return 0
        if state in ("failed",):
            print(f"pipeline failed: {status}")
            return 1
        # running / queued / etc → keep polling

    print(f"timed out after {args.timeout}s waiting for ingestion to complete.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
