#!/usr/bin/env bash
# Publish self-hosted MCP servers to dedicated private GitHub repos
# on the PAT2 account. Uses the same snapshot-push pattern as publish-skill.sh.
#
# Usage:
#   scripts/publish-mcp.sh <mcp-name>           # e.g. fivetran, openmetadata
#   scripts/publish-mcp.sh --all
#   scripts/publish-mcp.sh --create-only ...
#
# Layout assumption:
#   tools/<mcp-name>_mcp/                       in this repo
#   github.com/${PAT2_USER}/mcp-<mcp-name>      on the PAT2 account

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./_publish_common.sh
. "$SCRIPT_DIR/_publish_common.sh"

_load_env
_setup_askpass

TOOLS_DIR="tools"
REPO_PREFIX="mcp-"
FOLDER_SUFFIX="_mcp"
CREATE_ONLY=0

list_mcps() {
  local d
  for d in "$TOOLS_DIR"/*"$FOLDER_SUFFIX"/; do
    [[ -d "$d" ]] || continue
    local base
    base="$(basename "$d")"
    echo "${base%$FOLDER_SUFFIX}"
  done
}

publish_one() {
  local mcp="$1"
  local prefix="$TOOLS_DIR/${mcp}${FOLDER_SUFFIX}"
  local repo_name="${REPO_PREFIX}${mcp}"
  local owner url
  owner="$(_repo_owner)"
  url="https://${PAT2_USER}@github.com/${owner}/${repo_name}.git"

  echo "• $mcp"
  if ! _create_private_repo "$repo_name" "MCP server: $mcp"; then
    FAILED+=("$mcp (create)")
    return 0
  fi
  if (( CREATE_ONLY == 0 )); then
    if ! _push_snapshot "$prefix" "$url" "mcp: $mcp snapshot"; then
      FAILED+=("$mcp (push)")
    fi
  fi
}

ALL=0
MCPS=()
while (( $# > 0 )); do
  case "$1" in
    --all)          ALL=1 ;;
    --create-only)  CREATE_ONLY=1 ;;
    -h|--help)
      grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *)              MCPS+=("$1") ;;
  esac
  shift
done

if (( ALL == 1 )); then
  mapfile -t MCPS < <(list_mcps)
fi

if (( ${#MCPS[@]} == 0 )); then
  echo "usage: $0 <mcp-name> | --all [--create-only]" >&2
  echo "available under $TOOLS_DIR/*$FOLDER_SUFFIX:"
  list_mcps | sed 's/^/  /' >&2
  exit 1
fi

echo "Owner:   $(_repo_owner)$( [[ -n "${PAT2_ORG:-}" ]] && echo ' (org)' || echo ' (user)' )"
echo "MCPs:    ${MCPS[*]}"
echo

FAILED=()
for m in "${MCPS[@]}"; do
  publish_one "$m"
done

echo
if (( ${#FAILED[@]} > 0 )); then
  echo "Done with failures:"
  for f in "${FAILED[@]}"; do echo "  - $f"; done
  exit 1
fi
echo "Done."
