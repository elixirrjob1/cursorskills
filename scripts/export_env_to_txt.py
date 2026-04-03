#!/usr/bin/env python3
"""Write selected environment variables to a text file (KEY=value per line).

Output may contain secrets. Do not commit the file; use a path outside git or rely on .gitignore.

Examples:
  export FOO=bar BAZ=qux
  python3 scripts/export_env_to_txt.py /tmp/out.txt FOO BAZ

  python3 scripts/export_env_to_txt.py --names-file /path/to/varnames.txt -o out.txt

  # names file: one variable name per line, # for comments
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


def _read_names_file(path: Path) -> list[str]:
    names: list[str] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        names.append(line)
    return names


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "output",
        nargs="?",
        default="-",
        help="Output file path, or '-' for stdout (default: stdout)",
    )
    p.add_argument(
        "vars",
        nargs="*",
        metavar="NAME",
        help="Environment variable names to export",
    )
    p.add_argument(
        "--names-file",
        "-f",
        type=Path,
        metavar="PATH",
        help="File with one variable name per line (# comments allowed)",
    )
    p.add_argument(
        "--skip-missing",
        action="store_true",
        help="Omit variables that are not set in the environment",
    )
    p.add_argument(
        "--export-shell",
        action="store_true",
        help="Prefix each line with 'export ' (bash-friendly)",
    )
    args = p.parse_args()

    names: list[str] = list(args.vars)
    if args.names_file:
        names.extend(_read_names_file(args.names_file))
    # de-dupe preserving order
    seen: set[str] = set()
    unique: list[str] = []
    for n in names:
        if n in seen:
            continue
        seen.add(n)
        unique.append(n)
    names = unique

    if not names:
        p.error("No variable names: pass NAME ... and/or --names-file")

    lines: list[str] = []
    prefix = "export " if args.export_shell else ""
    for name in names:
        if not name:
            continue
        if "=" in name:
            print(f"warning: skipping invalid name (contains '='): {name!r}", file=sys.stderr)
            continue
        if name not in os.environ:
            if args.skip_missing:
                continue
            lines.append(f"{prefix}{name}=")
        else:
            val = os.environ[name]
            # minimal escaping for .env-like single line: if newline, repr
            if "\n" in val or "\r" in val:
                safe = repr(val)
            else:
                safe = val
            lines.append(f"{prefix}{name}={safe}")

    body = "\n".join(lines)
    if body and not body.endswith("\n"):
        body += "\n"

    if args.output == "-":
        sys.stdout.write(body)
    else:
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(body, encoding="utf-8")
        print(f"Wrote {len(lines)} line(s) to {out}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
