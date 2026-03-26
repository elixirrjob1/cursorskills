"""Render snowflake_fivetran_drip_bronze_erp.sql from repo .env (see placeholders {{...}})."""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def _ensure_scripts_on_path() -> None:
    scripts_dir = REPO_ROOT / "scripts"
    if str(scripts_dir) not in sys.path:
        sys.path.insert(0, str(scripts_dir))


def _load_env() -> None:
    _ensure_scripts_on_path()
    try:
        from dotenv import load_dotenv

        load_dotenv(REPO_ROOT / ".env")
    except ImportError:
        pass
    try:
        from _envfile import load_env_file

        load_env_file(REPO_ROOT / ".env")
    except ImportError:
        pass
    try:
        from keyvault_loader import load_env

        load_env()
    except ImportError:
        pass


def _sql_escape(value: str) -> str:
    return value.replace("'", "''")


def _missing_template_vars(text: str, *, require_password: bool) -> list[str]:
    keys = sorted(set(re.findall(r"\{\{([A-Z][A-Z0-9_]*)\}\}", text)))
    missing: list[str] = []
    for key in keys:
        if key == "SNOWFLAKE_FIVETRAN_PASSWORD" and not require_password:
            continue
        if not os.environ.get(key, "").strip():
            missing.append(key)
    return missing


def render_template(template_path: Path | str, *, require_password: bool = True) -> str:
    """
    Substitute {{VAR}} from environment. Values are escaped for use inside SQL single-quoted strings.
    """
    _load_env()
    p = Path(template_path)
    text = p.read_text(encoding="utf-8")

    missing = _missing_template_vars(text, require_password=require_password)
    if missing:
        vars_text = ", ".join(missing)
        raise SystemExit(
            "Missing required Snowflake setup env vars: "
            f"{vars_text}. Set them in .env or Key Vault-backed env loading before rendering."
        )

    def repl(m: re.Match[str]) -> str:
        key = m.group(1)
        raw = os.environ.get(key, "").strip()
        return _sql_escape(raw)

    return re.sub(r"\{\{([A-Z][A-Z0-9_]*)\}\}", repl, text)
