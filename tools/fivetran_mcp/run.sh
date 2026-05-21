#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

_load_dotenv() {
  local dir="$ROOT_DIR"
  while [[ "$dir" != "/" ]]; do
    if [[ -f "$dir/.env" ]]; then
      set -a
      # shellcheck source=/dev/null
      source "$dir/.env"
      set +a
      return 0
    fi
    dir="$(dirname "$dir")"
  done
}
_load_dotenv

VENDOR_DIR="$ROOT_DIR/vendor"
# shellcheck source=mcp_resolve_python.inc.sh
source "$ROOT_DIR/mcp_resolve_python.inc.sh"
PYTHON_EXE="$(mcp_resolve_python "$VENDOR_DIR")" || {
  echo "Fivetran MCP: no Python could import vendor deps under $VENDOR_DIR" >&2
  echo "Fix: install vendor deps for this package (see README), or set MCP_SERVER_PYTHON=/path/to/python" >&2
  exit 1
}
PYTHONPATH="${VENDOR_DIR}${PYTHONPATH:+:$PYTHONPATH}" exec "$PYTHON_EXE" "$ROOT_DIR/server.py"
