#!/usr/bin/env bash
# Copy platform docs from docs/ into dbtproject and plugins mirror folders.
# Run from repo root after editing docs/ONBOARDING.md or other docs/*.md:
#   ./scripts/sync_onboarding_docs.sh
#
# Then sync secondary repos:
#   ./scripts/sync_dbt_secondary_repo.sh      # if present, or manual rsync per dbt-secondary-repo-sync rule
#   ./scripts/sync_plugins_secondary_repo.sh    # pushes plugins/docs/ to responsum-team/plugins

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SRC="${ROOT_DIR}/docs"
DBT_DOCS="${ROOT_DIR}/dbt_project/drip_transformations/docs"
PLUGINS_DOCS="${ROOT_DIR}/plugins/docs"

if [[ ! -f "${SRC}/ONBOARDING.md" ]]; then
  echo "Missing ${SRC}/ONBOARDING.md" >&2
  exit 1
fi

mkdir -p "${DBT_DOCS}" "${PLUGINS_DOCS}"

rsync -av --delete \
  --exclude='.gitkeep' \
  "${SRC}/" "${DBT_DOCS}/"

rsync -av --delete \
  --exclude='.gitkeep' \
  "${SRC}/" "${PLUGINS_DOCS}/"

echo "Synced docs/ -> ${DBT_DOCS}"
echo "Synced docs/ -> ${PLUGINS_DOCS}"
