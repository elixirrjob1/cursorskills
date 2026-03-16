#!/usr/bin/env bash
# Ensure the local Fivetran MCP server is present in Cursor's mcp.json.
# Run from repo root: ./scripts/setup_fivetran_mcp.sh
# Override path: CURSOR_MCP_JSON=/path/to/mcp.json ./scripts/setup_fivetran_mcp.sh

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MCP_JSON="${CURSOR_MCP_JSON:-$HOME/.cursor/mcp.json}"
LAUNCHER_PATH="$ROOT_DIR/tools/fivetran_mcp/run.sh"

mkdir -p "$(dirname "$MCP_JSON")"

SERVER_JSON=$(cat <<EOF
{"command":"$LAUNCHER_PATH","env":{"FIVETRAN_API_KEY":"YOUR_FIVETRAN_API_KEY","FIVETRAN_API_SECRET":"YOUR_FIVETRAN_API_SECRET"}}
EOF
)

if [[ ! -f "$MCP_JSON" ]]; then
  printf '%s\n' "{
  \"mcpServers\": {
    \"fivetran-example\": $SERVER_JSON
  }
}" > "$MCP_JSON"
  echo "Created $MCP_JSON with fivetran-example."
  echo "Set real FIVETRAN_API_KEY and FIVETRAN_API_SECRET values before using it."
  exit 0
fi

if command -v jq >/dev/null 2>&1; then
  tmp=$(mktemp)
  jq --argjson server "$SERVER_JSON" '.mcpServers = (.mcpServers // {}) | .mcpServers["fivetran-example"] = $server' "$MCP_JSON" > "$tmp"
  mv "$tmp" "$MCP_JSON"
  echo "Updated $MCP_JSON with fivetran-example."
  echo "Set real FIVETRAN_API_KEY and FIVETRAN_API_SECRET values before using it."
  exit 0
fi

echo "jq not found. Add the following under mcpServers in $MCP_JSON manually:"
echo ""
echo '  "fivetran-example": {'
echo "    \"command\": \"$LAUNCHER_PATH\","
echo '    "env": {'
echo '      "FIVETRAN_API_KEY": "YOUR_FIVETRAN_API_KEY",'
echo '      "FIVETRAN_API_SECRET": "YOUR_FIVETRAN_API_SECRET"'
echo '    }'
echo '  }'
