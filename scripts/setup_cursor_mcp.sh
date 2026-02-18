#!/usr/bin/env bash
# Ensure Azure MCP Server is present in Cursor's mcp.json (creates file or merges).
# Run from repo root: ./scripts/setup_cursor_mcp.sh
# Override path: CURSOR_MCP_JSON=/path/to/mcp.json ./scripts/setup_cursor_mcp.sh

set -e

MCP_JSON="${CURSOR_MCP_JSON:-$HOME/.cursor/mcp.json}"
AZURE_MCP='{"command":"npx","args":["-y","@azure/mcp@latest","server","start"]}'

mkdir -p "$(dirname "$MCP_JSON")"

if [[ ! -f "$MCP_JSON" ]]; then
  echo "Creating $MCP_JSON with Azure MCP Server."
  printf '%s\n' "{
  \"mcpServers\": {
    \"Azure MCP Server\": $AZURE_MCP
  }
}" > "$MCP_JSON"
  echo "Done. Restart Cursor or reload MCP if needed."
  exit 0
fi

if command -v jq &>/dev/null; then
  if jq -e '.mcpServers["Azure MCP Server"]' "$MCP_JSON" &>/dev/null; then
    echo "Azure MCP Server already present in $MCP_JSON"
    exit 0
  fi
  echo "Adding Azure MCP Server to $MCP_JSON"
  tmp=$(mktemp)
  jq --argjson azure "$AZURE_MCP" '.mcpServers["Azure MCP Server"] = $azure' "$MCP_JSON" > "$tmp" && mv "$tmp" "$MCP_JSON"
  echo "Done. Restart Cursor or reload MCP if needed."
else
  echo "jq not found. Add the following to $MCP_JSON manually:"
  echo ""
  echo '  "Azure MCP Server": {'
  echo '    "command": "npx",'
  echo '    "args": ["-y", "@azure/mcp@latest", "server", "start"]'
  echo '  }'
  echo ""
  echo "under mcpServers. See README Installation > Cursor MCP (optional)."
  exit 1
fi
