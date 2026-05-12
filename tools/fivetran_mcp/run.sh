#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$ROOT_DIR/../.." && pwd)"

# Load FIVETRAN_API_KEY and FIVETRAN_API_SECRET from .env (no hardcoding in mcp.json)
if [[ -f "$PROJECT_ROOT/.env" ]]; then
  set -a
  # shellcheck source=/dev/null
  source "$PROJECT_ROOT/.env"
  set +a
fi

VENDOR_DIR="$ROOT_DIR/vendor"
# shellcheck source=/dev/null
source "$ROOT_DIR/../mcp_resolve_python.inc.sh"
PYTHON_EXE="$(mcp_resolve_python "$VENDOR_DIR")" || {
  echo "Fivetran MCP: no Python could import vendor deps under $VENDOR_DIR" >&2
  echo "Fix: run  bash scripts/install_fivetran_mcp_deps.sh  (use same Python you want Cursor to run), or set MCP_SERVER_PYTHON=/path/to/python" >&2
  exit 1
}
PYTHONPATH="${VENDOR_DIR}${PYTHONPATH:+:$PYTHONPATH}" exec "$PYTHON_EXE" "$ROOT_DIR/server.py"
