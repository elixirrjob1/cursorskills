#!/usr/bin/env bash
# Push dbt_project/drip_transformations/ as a subtree to the secondary dbt repo.
# Auth comes from PAT2 / PAT2_USER in .env (never hardcoded).
#
# Usage:
#   scripts/push-dbt-remote.sh              # push current branch → main
#   scripts/push-dbt-remote.sh <branch>     # push to a named branch
#
# Called automatically by .git/hooks/post-push after every git push.

set -euo pipefail

REPO_ROOT="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"
ENV_FILE="$REPO_ROOT/.env"

# ── Load .env ────────────────────────────────────────────────────────────────
if [[ -f "$ENV_FILE" ]]; then
  # Export only lines that look like KEY=VALUE (skip comments and blanks)
  set -o allexport
  # shellcheck disable=SC1090
  source <(grep -E '^[A-Z_][A-Z0-9_]*=' "$ENV_FILE")
  set +o allexport
fi

# ── Validate required vars ───────────────────────────────────────────────────
PAT="${PAT2:-}"
PAT_USER="${PAT2_USER:-}"
REMOTE_URL="https://github.com/responsum-team/dbtproject"
PREFIX="dbt_project/drip_transformations"
TARGET_BRANCH="${1:-main}"

if [[ -z "$PAT" || -z "$PAT_USER" ]]; then
  echo "[dbt-remote] ERROR: PAT2 and PAT2_USER must be set in .env" >&2
  exit 1
fi

AUTHED_URL="https://${PAT_USER}:${PAT}@github.com/responsum-team/dbtproject"

echo "[dbt-remote] Pushing ${PREFIX}/ → ${REMOTE_URL} (${TARGET_BRANCH}) ..."
cd "$REPO_ROOT"

# Split the subtree into a temporary local branch (cached across runs — much
# faster than a full subtree push on subsequent calls).
SPLIT_BRANCH="_dbt-remote-split"
git subtree split --prefix="$PREFIX" -b "$SPLIT_BRANCH" > /dev/null

# Force-push so divergent remote history (e.g. initial snapshot commit) is
# cleanly replaced. All subsequent pushes will also be fast-forward via the
# cached split branch, so --force is idempotent and safe here.
git push --force "$AUTHED_URL" "${SPLIT_BRANCH}:${TARGET_BRANCH}"

# Clean up local split branch
git branch -D "$SPLIT_BRANCH" > /dev/null 2>&1 || true

echo "[dbt-remote] Done."
