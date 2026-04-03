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

if [[ -d "$VENDOR_DIR" ]]; then
  PYTHONPATH="$VENDOR_DIR${PYTHONPATH:+:$PYTHONPATH}" exec python3 "$ROOT_DIR/server.py"
fi

PYTHONPATH="$FALLBACK_VENDOR_DIR${PYTHONPATH:+:$PYTHONPATH}" exec python3 "$ROOT_DIR/server.py"
