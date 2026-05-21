# shellcheck shell=bash
# Sourced by run.sh — pick a Python interpreter that can import vendored mcp + pydantic_core.

mcp_resolve_python() {
  local vendor_dir="$1"
  if [[ ! -d "$vendor_dir" ]]; then
    return 1
  fi
  local candidates=()
  [[ -n "${MCP_SERVER_PYTHON:-}" ]] && candidates+=("${MCP_SERVER_PYTHON}")
  candidates+=(python3 python3.13 python3.12 python3.14 python3.11)
  local py exe
  for py in "${candidates[@]}"; do
    [[ -z "$py" ]] && continue
    if [[ "$py" == /* ]]; then
      exe="$py"
    elif command -v "$py" &>/dev/null; then
      exe="$(command -v "$py")"
    else
      continue
    fi
    if PYTHONPATH="$vendor_dir" "$exe" -c "import pydantic_core; import mcp" 2>/dev/null; then
      printf '%s' "$exe"
      return 0
    fi
  done
  return 1
}
