#!/usr/bin/env bash
# Install Python dependencies for the local OpenMetadata MCP server into a repo-local vendor dir.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_DIR="$ROOT_DIR/tools/openmetadata_mcp/vendor"

mkdir -p "$TARGET_DIR"
python3 -m pip install --target "$TARGET_DIR" -r "$ROOT_DIR/tools/openmetadata_mcp/requirements.txt"
echo "Installed OpenMetadata MCP dependencies into $TARGET_DIR"
