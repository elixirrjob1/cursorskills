"""Minimal .env loader (no python-dotenv dependency)."""

from __future__ import annotations

from pathlib import Path


def load_env_file(path: Path | str, override: bool = False) -> None:
    import os

    p = Path(path)
    if not p.is_file():
        return
    for raw in p.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, val = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        val = val.strip().strip('"').strip("'")
        if override or key not in os.environ:
            os.environ[key] = val
