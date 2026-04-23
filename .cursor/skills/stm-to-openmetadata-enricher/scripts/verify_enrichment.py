"""Spot-check that a set of OpenMetadata tables have been enriched.

For every table, assert:
  1. table.description is a non-empty string
  2. len(table.tags) >= 1
  3. At least one column has a non-empty description

Usage:
    python verify_enrichment.py \
        --schema-fqn snowflake_fivetran.DRIP_DATA_INTELLIGENCE.DBT_PROD_ENRICHED \
        --tables DIMCUSTOMER,DIMDATE,FACTSALES

Exits 0 if every table passes, else 1.
"""

from __future__ import annotations

import argparse
import sys

from om_client import api, login


def _get_table(token: str, fqn: str) -> dict:
    return api("GET", f"/api/v1/tables/name/{fqn}?fields=tags,columns", token)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--schema-fqn", required=True, help="e.g. <service>.<db>.<schema>")
    ap.add_argument("--tables", required=True, help="Comma-separated table names (UPPERCASE).")
    args = ap.parse_args()

    _base, token = login()
    names = [t.strip() for t in args.tables.split(",") if t.strip()]

    failures: list[str] = []
    rows: list[tuple[str, bool, int, int, int]] = []

    for name in names:
        fqn = f"{args.schema_fqn}.{name}"
        try:
            t = _get_table(token, fqn)
        except RuntimeError as exc:
            print(f"ERROR loading {fqn}: {exc}")
            failures.append(name)
            rows.append((name, False, 0, 0, 0))
            continue

        desc = (t.get("description") or "").strip()
        tags = t.get("tags") or []
        cols = t.get("columns") or []
        cols_with_desc = sum(1 for c in cols if (c.get("description") or "").strip())

        ok = bool(desc) and len(tags) >= 1 and cols_with_desc >= 1
        rows.append((name, ok, len(tags), cols_with_desc, len(cols)))
        if not ok:
            failures.append(name)

    print(f"{'Table':<30} {'OK':<4} {'tags':<6} {'cols w/ desc':<14}")
    print("-" * 60)
    for name, ok, ntags, ncdesc, ncols in rows:
        mark = "yes" if ok else "NO"
        print(f"{name:<30} {mark:<4} {ntags:<6} {ncdesc}/{ncols}")

    if failures:
        print()
        print(f"FAILED: {', '.join(failures)}")
        return 1
    print()
    print(f"all {len(names)} tables passed verification.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
