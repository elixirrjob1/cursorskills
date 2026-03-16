#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENDOR_DIR="$ROOT_DIR/vendor"
PYTHONPATH="${VENDOR_DIR}${PYTHONPATH:+:$PYTHONPATH}" exec python3 "$ROOT_DIR/server.py"
