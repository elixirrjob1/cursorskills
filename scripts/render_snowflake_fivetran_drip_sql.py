#!/usr/bin/env python3
"""Render scripts/snowflake_fivetran_drip_bronze_erp.sql using variables from repo .env."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--template",
        default=str(REPO_ROOT / "scripts/snowflake_fivetran_drip_bronze_erp.sql"),
        help="Path to SQL template with {{VAR}} placeholders",
    )
    p.add_argument(
        "-o",
        "--output",
        help="Write rendered SQL to this file (default: stdout)",
    )
    p.add_argument(
        "--allow-empty-password",
        action="store_true",
        help="Allow missing SNOWFLAKE_FIVETRAN_PASSWORD (for debugging template only)",
    )
    args = p.parse_args()

    sys.path.insert(0, str(REPO_ROOT / "scripts"))
    from snowflake_fivetran_template import render_template

    try:
        out = render_template(
            args.template, require_password=not args.allow_empty_password
        )
    except SystemExit as e:
        print(str(e), file=sys.stderr)
        return 1

    if args.output:
        Path(args.output).write_text(out, encoding="utf-8")
    else:
        sys.stdout.write(out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
