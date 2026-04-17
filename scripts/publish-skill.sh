#!/usr/bin/env bash
# Publish Cursor skill folder(s) to their dedicated private GitHub repos
# on the PAT2 account (filipmaricatelixirr by default).
#
# Usage:
#   scripts/publish-skill.sh <skill-name>      # one skill
#   scripts/publish-skill.sh --all             # every folder under .cursor/skills/
#   scripts/publish-skill.sh --create-only ... # create repos, skip push
#
# Reads from .env (gitignored):
#   PAT2        GitHub personal access token with `repo` scope
#   PAT2_USER   GitHub username owning the skill repos
#
# Push strategy: SNAPSHOT. Each invocation copies tracked files under
# .cursor/skills/<name>/ into a throwaway repo, commits them, and force-pushes
# to main. This bypasses `git subtree split` history walking, which is brittle
# when folders were moved in master's history (subtree can end up pushing the
# wrong tree).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./_publish_common.sh
. "$SCRIPT_DIR/_publish_common.sh"

_load_env
_setup_askpass

SKILLS_DIR=".cursor/skills"
REPO_PREFIX="skill-"
CREATE_ONLY=0

publish_one() {
  local skill="$1"
  local prefix="$SKILLS_DIR/$skill"
  local repo_name="${REPO_PREFIX}${skill}"
  local url="https://${PAT2_USER}@github.com/${PAT2_USER}/${repo_name}.git"

  echo "• $skill"
  _create_private_repo "$repo_name" "Cursor skill: $skill"
  if (( CREATE_ONLY == 0 )); then
    _push_snapshot "$prefix" "$url" "skill: $skill snapshot"
  fi
}

ALL=0
SKILLS=()
while (( $# > 0 )); do
  case "$1" in
    --all)          ALL=1 ;;
    --create-only)  CREATE_ONLY=1 ;;
    -h|--help)
      grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *)              SKILLS+=("$1") ;;
  esac
  shift
done

if (( ALL == 1 )); then
  SKILLS=()
  for d in "$SKILLS_DIR"/*/; do
    SKILLS+=("$(basename "$d")")
  done
fi

if (( ${#SKILLS[@]} == 0 )); then
  echo "usage: $0 <skill-name> | --all [--create-only]" >&2
  exit 1
fi

echo "Account: ${PAT2_USER}"
echo "Skills:  ${SKILLS[*]}"
echo

for s in "${SKILLS[@]}"; do
  publish_one "$s"
done

echo
echo "Done."
