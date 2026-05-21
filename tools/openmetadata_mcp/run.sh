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
FALLBACK_VENDOR_DIR=""
if [[ -d "$ROOT_DIR/../fivetran_mcp/vendor" ]]; then
  FALLBACK_VENDOR_DIR="$ROOT_DIR/../fivetran_mcp/vendor"
fi
# shellcheck source=mcp_resolve_python.inc.sh
source "$ROOT_DIR/mcp_resolve_python.inc.sh"

_pick_and_run() {
  local vdir="$1"
  local py
  py="$(mcp_resolve_python "$vdir")" || return 1
  PYTHONPATH="${vdir}${PYTHONPATH:+:$PYTHONPATH}" exec "$py" "$ROOT_DIR/server.py"
}

if [[ -d "$VENDOR_DIR" ]]; then
  _pick_and_run "$VENDOR_DIR" || true
fi

if [[ -n "$FALLBACK_VENDOR_DIR" && -d "$FALLBACK_VENDOR_DIR" ]]; then
  echo "OpenMetadata MCP: using Fivetran vendor fallback — run install_openmetadata_mcp_deps for a dedicated vendor tree" >&2
  _pick_and_run "$FALLBACK_VENDOR_DIR" || true
fi

echo "OpenMetadata MCP: no working vendor. Install deps for this package (see README)." >&2
exit 1
