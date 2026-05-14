#!/usr/bin/env python3
"""Wrapper for the currently tested API provider."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

DEFAULT_BASE_URL = "https://skillssimapifilip20260218.azurewebsites.net"


def main() -> int:
    parser = argparse.ArgumentParser(description="Call the configured test API via api_reader")
    parser.add_argument("--base-url", default=os.environ.get("API_BASE_URL", DEFAULT_BASE_URL))
    parser.add_argument("--path", action="append", default=[])
    parser.add_argument("--discover", action="store_true")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    api_reader = Path(__file__).resolve().parents[1] / "api_reader.py"
    cmd = [sys.executable, str(api_reader), args.base_url, "--output", args.output]

    if args.discover:
        cmd.append("--discover")

    paths = list(args.path)
    if not args.discover and not paths:
        paths = ["/api/tables", "/api/customers"]

    for p in paths:
        cmd.extend(["--path", p])

    return subprocess.run(cmd).returncode


if __name__ == "__main__":
    raise SystemExit(main())
