#!/usr/bin/env python3
"""
Read a REST API: fetch paths or discover endpoints.
Usage:
  api_reader.py <base_url> [--path PATH ...] [--discover] [--bearer TOKEN] [--api-key KEY] [--output FILE]
"""

import argparse
import json
import sys
from urllib.parse import urljoin

import requests

DISCOVERY_PATHS = [
    "/api/tables",
    "/api",
    "/openapi.json",
    "/swagger.json",
    "/docs",
    "/",
]


def main() -> int:
    p = argparse.ArgumentParser(description="Read REST API: fetch paths or discover endpoints.")
    p.add_argument("base_url", help="Base URL of the API (e.g. http://localhost:8000)")
    p.add_argument("--path", action="append", dest="paths", metavar="PATH", help="Path to fetch (e.g. /api/tables); can repeat")
    p.add_argument("--discover", action="store_true", help="Try common discovery paths")
    p.add_argument("--bearer", metavar="TOKEN", help="Bearer token for Authorization header")
    p.add_argument("--api-key", metavar="KEY", help="API key value")
    p.add_argument("--api-key-header", default="X-API-Key", help="Header name for API key (default: X-API-Key)")
    p.add_argument("--output", "-o", metavar="FILE", help="Write JSON to file (default: stdout)")
    p.add_argument("--timeout", type=int, default=30, help="Request timeout in seconds (default: 30)")
    args = p.parse_args()

    base = args.base_url.rstrip("/")
    if not base.startswith(("http://", "https://")):
        base = "http://" + base

    headers = {}
    if args.bearer:
        headers["Authorization"] = f"Bearer {args.bearer}"
    if args.api_key:
        headers[args.api_key_header] = args.api_key

    session = requests.Session()
    session.headers.update(headers)
    timeout = args.timeout

    result = {"base_url": base}
    paths_to_fetch = list(args.paths or [])

    if args.discover:
        discovery = []
        for path in DISCOVERY_PATHS:
            url = urljoin(base + "/", path.lstrip("/"))
            try:
                r = session.get(url, timeout=timeout)
                try:
                    data = r.json() if r.content else None
                except Exception:
                    data = r.text[:2000] if r.text else None
                discovery.append({"path": path, "status_code": r.status_code, "data": data})
                if path == "/api/tables" and r.status_code == 200 and isinstance(data, dict) and "tables" in data:
                    result["tables"] = data.get("tables", [])
            except requests.RequestException as e:
                discovery.append({"path": path, "error": str(e)})
        result["discovery"] = discovery
        if not paths_to_fetch:
            return _write(result, args.output)

    if not paths_to_fetch:
        return _write(result, args.output)

    path_results = []
    for path in paths_to_fetch:
        url = urljoin(base + "/", path.lstrip("/"))
        try:
            r = session.get(url, timeout=timeout)
            try:
                data = r.json() if r.content else None
            except Exception:
                data = r.text[:5000] if r.text else None
            path_results.append({"path": path, "status_code": r.status_code, "data": data})
        except requests.RequestException as e:
            path_results.append({"path": path, "error": str(e)})

    if len(path_results) == 1:
        result.update(path_results[0])
    else:
        result["paths"] = path_results

    return _write(result, args.output)


def _write(obj: dict, output: str | None) -> int:
    out = json.dumps(obj, indent=2, default=str)
    if output:
        with open(output, "w", encoding="utf-8") as f:
            f.write(out)
        return 0
    print(out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
