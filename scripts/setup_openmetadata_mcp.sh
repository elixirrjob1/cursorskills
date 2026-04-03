#!/usr/bin/env bash
# Ensure the local OpenMetadata MCP server is present in Cursor's mcp.json.
# Run from repo root: ./scripts/setup_openmetadata_mcp.sh
# Override path: CURSOR_MCP_JSON=/path/to/mcp.json ./scripts/setup_openmetadata_mcp.sh

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MCP_JSON="${CURSOR_MCP_JSON:-$HOME/.cursor/mcp.json}"
LAUNCHER_PATH="$ROOT_DIR/tools/openmetadata_mcp/run.sh"

mkdir -p "$(dirname "$MCP_JSON")"

SERVER_JSON=$(cat <<EOF
{"command":"$LAUNCHER_PATH"}
EOF
)

if [[ ! -f "$MCP_JSON" ]]; then
  printf '%s\n' "{
  \"mcpServers\": {
    \"openmetadata\": $SERVER_JSON
  }
}" > "$MCP_JSON"
  echo "Created $MCP_JSON with openmetadata."
  echo "Set OPENMETADATA_BASE_URL, OPENMETADATA_EMAIL, and OPENMETADATA_PASSWORD in project .env."
  exit 0
fi

if command -v jq >/dev/null 2>&1; then
  tmp=$(mktemp)
  jq --argjson server "$SERVER_JSON" '.mcpServers = (.mcpServers // {}) | .mcpServers["openmetadata"] = $server' "$MCP_JSON" > "$tmp"
  mv "$tmp" "$MCP_JSON"
  echo "Updated $MCP_JSON with openmetadata."
  echo "Set OPENMETADATA_BASE_URL, OPENMETADATA_EMAIL, and OPENMETADATA_PASSWORD in project .env."
  exit 0
fi

echo "jq not found. Add the following under mcpServers in $MCP_JSON manually:"
echo ""
echo '  "openmetadata": {'
echo "    \"command\": \"$LAUNCHER_PATH\""
echo '  }'
echo ""
echo "Credentials are loaded from project .env by run.sh."
