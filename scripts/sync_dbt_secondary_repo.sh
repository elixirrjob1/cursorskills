#!/usr/bin/env bash
# Push dbt_project/drip_transformations/ to responsum-team/dbtproject (what dbt Cloud reads).
# Requires PAT2 in .env. Never force-push.
#
# Usage:
#   ./scripts/sync_onboarding_docs.sh   # if docs changed
#   ./scripts/sync_dbt_secondary_repo.sh

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DBT_SRC="${ROOT_DIR}/dbt_project/drip_transformations"
TARGET_REPO="responsum-team/dbtproject"

if [[ -f "${ROOT_DIR}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${ROOT_DIR}/.env"
  set +a
fi

: "${PAT2:?Set PAT2 in .env}"

rm -rf /tmp/dbt-sync-target
git clone "https://${PAT2}@github.com/${TARGET_REPO}.git" /tmp/dbt-sync-target

rsync -av --exclude='.git' \
  "${DBT_SRC}/" \
  /tmp/dbt-sync-target/

cd /tmp/dbt-sync-target
git add -A
git diff --cached --quiet || git commit -m "sync: update from elixirrjob1/cursorskills main"
git push

echo "Pushed drip_transformations/ to ${TARGET_REPO}"
