#!/usr/bin/env bash
# Push dbt_project/drip_transformations/ as a standalone repo to the secondary
# dbt repository, with the subdirectory promoted to the repo root.
# Auth comes from PAT2 / PAT2_USER in .env (never hardcoded).
#
# Usage:
#   scripts/push-dbt-remote.sh              # push current branch → main
#   scripts/push-dbt-remote.sh <branch>     # push to a named branch
#
# Called automatically by .git/hooks/post-push after every git push.
#
# Strategy: clone the main repo into a temp directory, run git filter-repo to
# promote the subdirectory to the root (rewriting full history), then push.
# This is more reliable than git subtree split which can leave an extra path
# prefix in the output depending on commit history.

set -euo pipefail

REPO_ROOT="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"
ENV_FILE="$REPO_ROOT/.env"

# ── Load .env ────────────────────────────────────────────────────────────────
if [[ -f "$ENV_FILE" ]]; then
  set -o allexport
  # shellcheck disable=SC1090
  source <(grep -E '^[A-Z_][A-Z0-9_]*=' "$ENV_FILE")
  set +o allexport
fi

# ── Validate required vars ───────────────────────────────────────────────────
PAT="${PAT2:-}"
PAT_USER="${PAT2_USER:-}"
REMOTE_LABEL="responsum-team/dbtproject"
AUTHED_URL="https://${PAT_USER}:${PAT}@github.com/responsum-team/dbtproject"
PREFIX="dbt_project/drip_transformations"
TARGET_BRANCH="${1:-main}"
WORK_DIR="$(mktemp -d)"

if [[ -z "$PAT" || -z "$PAT_USER" ]]; then
  echo "[dbt-remote] ERROR: PAT2 and PAT2_USER must be set in .env" >&2
  exit 1
fi

if ! command -v git-filter-repo &>/dev/null; then
  echo "[dbt-remote] ERROR: git-filter-repo not found. Install with:" >&2
  echo "  pip install git-filter-repo --break-system-packages" >&2
  exit 1
fi

cleanup() { rm -rf "$WORK_DIR"; }
trap cleanup EXIT

echo "[dbt-remote] Syncing ${PREFIX}/ → ${REMOTE_LABEL} (${TARGET_BRANCH}) ..."

# ── 1. Clone main repo locally (no-local to get a real independent clone) ────
git clone --quiet "$REPO_ROOT" "$WORK_DIR/split" --no-local
cd "$WORK_DIR/split"

# ── 2. Rewrite history: promote subdirectory to repo root ────────────────────
git filter-repo --subdirectory-filter "$PREFIX" --force --quiet

# ── 3. Fetch current state of secondary repo ─────────────────────────────────
git remote add secondary "$AUTHED_URL"
git fetch --quiet secondary "${TARGET_BRANCH}" 2>/dev/null || true
REMOTE_SHA=$(git rev-parse "refs/remotes/secondary/${TARGET_BRANCH}" 2>/dev/null || echo "")

LOCAL_SHA=$(git rev-parse HEAD)

# ── 4. Check if remote has commits we don't have ─────────────────────────────
if [[ -n "$REMOTE_SHA" ]] && ! git merge-base --is-ancestor "$REMOTE_SHA" "$LOCAL_SHA"; then
  echo "[dbt-remote] Remote has new commits — merging back into ${PREFIX}/ ..."

  # Bring the remote commit into main repo as a subtree pull
  cd "$REPO_ROOT"
  if ! git subtree pull --prefix="$PREFIX" "$AUTHED_URL" "$TARGET_BRANCH" \
       --squash -m "chore: merge changes from ${REMOTE_LABEL}" 2>/dev/null; then
    echo "" >&2
    echo "[dbt-remote] ─────────────────────────────────────────────────────" >&2
    echo "[dbt-remote] MERGE CONFLICT — resolve manually then re-push:"     >&2
    echo "    1. Fix conflicts in ${PREFIX}/"                                >&2
    echo "    2. git add <resolved files>"                                   >&2
    echo "    3. git commit && git push"                                     >&2
    echo "[dbt-remote] ─────────────────────────────────────────────────────" >&2
    git merge --abort 2>/dev/null || true
    exit 1
  fi

  # Re-run split after merge
  rm -rf "$WORK_DIR/split"
  git clone --quiet "$REPO_ROOT" "$WORK_DIR/split" --no-local
  cd "$WORK_DIR/split"
  git filter-repo --subdirectory-filter "$PREFIX" --force --quiet
  git remote add secondary "$AUTHED_URL"
fi

# ── 5. Push ──────────────────────────────────────────────────────────────────
echo "[dbt-remote] Pushing ..."
git push --force secondary "HEAD:${TARGET_BRANCH}"
echo "[dbt-remote] Done."
