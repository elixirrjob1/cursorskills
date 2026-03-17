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
PYTHONPATH="${VENDOR_DIR}${PYTHONPATH:+:$PYTHONPATH}" exec python3 "$ROOT_DIR/server.py"
