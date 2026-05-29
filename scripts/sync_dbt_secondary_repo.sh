#!/usr/bin/env bash
# Push the full base bundle to responsum-team/dbtproject.
#
# Syncs:
#   dbt project (models, tests, macros, STMs, MCP config) — repo root in dbtproject
#   scripts/         — platform Python + shell scripts
#   requirements.txt — Python dependencies
#   .cursor/skills/  — Cursor agent skills (test snapshots, runs, __pycache__ excluded)
#   .cursor/rules/   — agent guardrails
#
# Requires PAT2 in .env. Never force-push.
#
# Usage:
#   ./scripts/sync_onboarding_docs.sh   # run first if docs changed
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

_RSYNC_CACHE_EXCLUDES=(
  --exclude='__pycache__'
  --exclude='*.pyc'
)

# dbt project files → repo root (dbt Cloud requires dbt_project.yml at root)
rsync -av --exclude='.git' "${_RSYNC_CACHE_EXCLUDES[@]}" \
  "${DBT_SRC}/" \
  /tmp/dbt-sync-target/

# Platform scripts (--delete-excluded drops stale __pycache__ already on dbtproject)
rsync -av --delete --delete-excluded --exclude='.git' "${_RSYNC_CACHE_EXCLUDES[@]}" \
  "${ROOT_DIR}/scripts/" \
  /tmp/dbt-sync-target/scripts/

# Python dependencies
cp "${ROOT_DIR}/requirements.txt" /tmp/dbt-sync-target/requirements.txt

# Cursor skills — exclude bulky test snapshots, run artefacts, and bytecode caches
rsync -av --delete --delete-excluded \
  --exclude='.git' \
  --exclude='tests/results/snapshots/' \
  --exclude='tests/results/runs/' \
  "${_RSYNC_CACHE_EXCLUDES[@]}" \
  "${ROOT_DIR}/.cursor/skills/" \
  /tmp/dbt-sync-target/.cursor/skills/

# Cursor rules
rsync -av --delete --delete-excluded --exclude='.git' "${_RSYNC_CACHE_EXCLUDES[@]}" \
  "${ROOT_DIR}/.cursor/rules/" \
  /tmp/dbt-sync-target/.cursor/rules/

cd /tmp/dbt-sync-target
git add -A
git diff --cached --quiet || git commit -m "sync: update from elixirrjob1/cursorskills main"
git push

echo "Pushed base bundle to ${TARGET_REPO}"
