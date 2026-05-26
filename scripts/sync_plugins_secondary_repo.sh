#!/usr/bin/env bash
# Push the local plugins/ mirror (docs/, and later skills/rules/mcps) to responsum-team/plugins.
# Requires PAT2 in .env. Never force-push.
#
# Usage:
#   ./scripts/sync_onboarding_docs.sh   # refresh docs copies first
#   ./scripts/sync_plugins_secondary_repo.sh

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PLUGINS_DIR="${ROOT_DIR}/plugins"
TARGET_REPO="responsum-team/plugins"

if [[ -f "${ROOT_DIR}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${ROOT_DIR}/.env"
  set +a
fi

: "${PAT2:?Set PAT2 in .env}"

if [[ ! -d "${PLUGINS_DIR}/docs" ]]; then
  echo "Missing ${PLUGINS_DIR}/docs — run ./scripts/sync_onboarding_docs.sh first" >&2
  exit 1
fi

rm -rf /tmp/plugins-sync-target
git clone "https://${PAT2}@github.com/${TARGET_REPO}.git" /tmp/plugins-sync-target

rsync -av \
  "${PLUGINS_DIR}/docs/" \
  /tmp/plugins-sync-target/docs/

cd /tmp/plugins-sync-target
git add docs/
git diff --cached --quiet || git commit -m "sync: update docs from elixirrjob1/cursorskills main"
git push

echo "Pushed docs/ to ${TARGET_REPO}"
