#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$ROOT_DIR/../.." && pwd)"

if [[ -f "$PROJECT_ROOT/.env" ]]; then
  set -a
  # shellcheck source=/dev/null
  source "$PROJECT_ROOT/.env"
  set +a
fi

VENDOR_DIR="$ROOT_DIR/vendor"
FALLBACK_VENDOR_DIR="$PROJECT_ROOT/tools/fivetran_mcp/vendor"
# shellcheck source=/dev/null
source "$ROOT_DIR/../mcp_resolve_python.inc.sh"

_pick_and_run() {
  local vdir="$1"
  local py
  py="$(mcp_resolve_python "$vdir")" || return 1
  PYTHONPATH="${vdir}${PYTHONPATH:+:$PYTHONPATH}" exec "$py" "$ROOT_DIR/server.py"
}

if [[ -d "$VENDOR_DIR" ]]; then
  _pick_and_run "$VENDOR_DIR" || true
fi

if [[ -d "$FALLBACK_VENDOR_DIR" ]]; then
  echo "OpenMetadata MCP: using Fivetran vendor fallback — run  bash scripts/install_openmetadata_mcp_deps.sh  for a dedicated vendor tree" >&2
  _pick_and_run "$FALLBACK_VENDOR_DIR" || true
fi

echo "OpenMetadata MCP: no working vendor. Run:" >&2
echo "  bash scripts/install_openmetadata_mcp_deps.sh" >&2
exit 1
