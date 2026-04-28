#!/usr/bin/env bash
# Push dbt_project/drip_transformations/ as a subtree to the secondary dbt repo.
# Auth comes from PAT2 / PAT2_USER in .env (never hardcoded).
#
# Usage:
#   scripts/push-dbt-remote.sh              # push current branch → main
#   scripts/push-dbt-remote.sh <branch>     # push to a named branch
#
# Called automatically by .git/hooks/post-push after every git push.
#
# If the secondary repo has commits we don't have (someone pushed there
# directly), this script will pull those changes back into the main repo's
# subtree first, then push — so nothing is ever silently overwritten.

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
PREFIX="dbt_project/drip_transformations"
TARGET_BRANCH="${1:-main}"
SPLIT_BRANCH="_dbt-remote-split"
FETCH_REF="refs/remotes/_dbt-remote/${TARGET_BRANCH}"

if [[ -z "$PAT" || -z "$PAT_USER" ]]; then
  echo "[dbt-remote] ERROR: PAT2 and PAT2_USER must be set in .env" >&2
  exit 1
fi

AUTHED_URL="https://${PAT_USER}:${PAT}@github.com/responsum-team/dbtproject"

echo "[dbt-remote] Syncing ${PREFIX}/ ↔ ${REMOTE_LABEL} (${TARGET_BRANCH}) ..."
cd "$REPO_ROOT"

# ── 1. Fetch latest state of the secondary repo ──────────────────────────────
git fetch --quiet "$AUTHED_URL" "${TARGET_BRANCH}:${FETCH_REF}" 2>/dev/null || {
  echo "[dbt-remote] WARNING: could not fetch from ${REMOTE_LABEL} — skipping sync." >&2
  exit 0
}

REMOTE_SHA=$(git rev-parse "$FETCH_REF")

# ── 2. Build our local subtree split ─────────────────────────────────────────
git subtree split --prefix="$PREFIX" -b "$SPLIT_BRANCH" > /dev/null
LOCAL_SHA=$(git rev-parse "$SPLIT_BRANCH")

# ── 3. Check relationship between local and remote ───────────────────────────
if git merge-base --is-ancestor "$REMOTE_SHA" "$LOCAL_SHA"; then
  # Remote is at or behind our split — simple fast-forward push
  echo "[dbt-remote] Remote is up to date or behind. Pushing ..."
  git push "$AUTHED_URL" "${SPLIT_BRANCH}:${TARGET_BRANCH}"

else
  # Remote has commits we don't have — pull them into the main repo first
  echo "[dbt-remote] Remote has new commits. Pulling changes into ${PREFIX}/ ..."
  git branch -D "$SPLIT_BRANCH" > /dev/null 2>&1 || true   # clean up before pull

  if git subtree pull --prefix="$PREFIX" "$AUTHED_URL" "$TARGET_BRANCH" \
       --squash -m "chore: merge changes from ${REMOTE_LABEL}"; then

    echo "[dbt-remote] Merged successfully. Pushing combined result ..."
    # Re-split after the merge and push
    git subtree split --prefix="$PREFIX" -b "$SPLIT_BRANCH" > /dev/null
    git push "$AUTHED_URL" "${SPLIT_BRANCH}:${TARGET_BRANCH}"

  else
    echo "" >&2
    echo "[dbt-remote] ──────────────────────────────────────────────────────" >&2
    echo "[dbt-remote] MERGE CONFLICT: changes in ${REMOTE_LABEL} conflict"   >&2
    echo "[dbt-remote] with local changes in ${PREFIX}/."                     >&2
    echo ""                                                                    >&2
    echo "  Resolve manually:"                                                 >&2
    echo "    1. Fix the conflicts shown above"                                >&2
    echo "    2. git add <resolved files>"                                     >&2
    echo "    3. git commit"                                                   >&2
    echo "    4. git push  (the hook will re-run and push both repos)"        >&2
    echo "[dbt-remote] ──────────────────────────────────────────────────────" >&2
    git merge --abort 2>/dev/null || true
    exit 1
  fi
fi

# ── 4. Clean up ──────────────────────────────────────────────────────────────
git branch -D "$SPLIT_BRANCH"        > /dev/null 2>&1 || true
git branch -D "_dbt-remote-split"    > /dev/null 2>&1 || true
git update-ref -d "$FETCH_REF"       > /dev/null 2>&1 || true

echo "[dbt-remote] Done."
