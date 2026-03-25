"""Render snowflake_fivetran_drip_bronze_erp.sql from repo .env (see placeholders {{...}})."""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

_DEFAULTS: dict[str, str] = {
    "SNOWFLAKE_FIVETRAN_ROLE": "FIVETRAN_DRIP_ROLE",
    "SNOWFLAKE_FIVETRAN_USER": "FIVETRAN_DRIP_USER",
    "SNOWFLAKE_FIVETRAN_WAREHOUSE": "FIVETRAN_DRIP_WH",
    "SNOWFLAKE_DRIP_DATABASE": "DRIP_DATA_INTELLIGENCE",
    "SNOWFLAKE_BRONZE_SCHEMA": "bronze_erp",
}


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


def _sql_escape(value: str) -> str:
    return value.replace("'", "''")


def render_template(template_path: Path | str, *, require_password: bool = True) -> str:
    """
    Substitute {{VAR}} from environment. Values are escaped for use inside SQL single-quoted strings.
    """
    _load_env()
    p = Path(template_path)
    text = p.read_text(encoding="utf-8")

    if require_password and "{{SNOWFLAKE_FIVETRAN_PASSWORD}}" in text:
        if not os.environ.get("SNOWFLAKE_FIVETRAN_PASSWORD", "").strip():
            raise SystemExit(
                "SNOWFLAKE_FIVETRAN_PASSWORD is required (set in .env) to render this template."
            )

    def repl(m: re.Match[str]) -> str:
        key = m.group(1)
        if key == "SNOWFLAKE_FIVETRAN_PASSWORD":
            raw = os.environ.get(key, "").strip()
            return _sql_escape(raw)
        default = _DEFAULTS.get(key, "")
        raw = os.environ.get(key, default).strip() or default
        return _sql_escape(raw)

    return re.sub(r"\{\{([A-Z][A-Z0-9_]*)\}\}", repl, text)
