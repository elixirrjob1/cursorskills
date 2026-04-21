#!/usr/bin/env bash
# Shared helpers for publish-skill.sh and publish-mcp.sh.
# Intended to be sourced, not executed directly.

# --- env / auth ---------------------------------------------------------------
_load_env() {
  if [[ ! -f .env ]]; then
    echo ".env not found (run from repo root)" >&2
    exit 1
  fi
  # shellcheck disable=SC1091
  set -a; . ./.env; set +a
  : "${PAT2:?PAT2 not set in .env}"
  : "${PAT2_USER:?PAT2_USER not set in .env}"
  # PAT2_ORG is optional; if unset, we push to the user account
  export PAT2 PAT2_USER
  export PAT2_ORG="${PAT2_ORG:-}"
}

# Returns the GitHub owner (org if set, else user) as stdout.
_repo_owner() {
  if [[ -n "${PAT2_ORG:-}" ]]; then
    printf '%s' "$PAT2_ORG"
  else
    printf '%s' "$PAT2_USER"
  fi
}

# Creates a throwaway GIT_ASKPASS script that feeds PAT2 to git at push time.
# Registers an EXIT trap to delete it. Must be called after _load_env.
_setup_askpass() {
  local askpass
  askpass="$(mktemp)"
  chmod 700 "$askpass"
  cat > "$askpass" <<'ASK'
#!/usr/bin/env bash
case "$1" in
  Username*) printf '%s' "${PAT2_USER}" ;;
  Password*) printf '%s' "${PAT2}" ;;
esac
ASK
  export GIT_ASKPASS="$askpass"
  export GIT_TERMINAL_PROMPT=0
  # shellcheck disable=SC2064
  trap "rm -f '$askpass'" EXIT
}

# --- repo lifecycle -----------------------------------------------------------
# Create a private repo under the effective owner (org if PAT2_ORG set,
# else user), if it does not already exist.
# Args: repo_name, description
# Returns 0 for created/exists, 2 for NAME_RESERVED (422 but repo not fetchable),
# nonzero otherwise.
_create_private_repo() {
  local name="$1" desc="$2"
  local owner endpoint
  owner="$(_repo_owner)"
  if [[ -n "${PAT2_ORG:-}" ]]; then
    endpoint="https://api.github.com/orgs/${PAT2_ORG}/repos"
  else
    endpoint="https://api.github.com/user/repos"
  fi
  local body http_code
  body=$(mktemp)
  http_code=$(curl -sS -o "$body" -w '%{http_code}' \
    -X POST \
    -H "Authorization: Bearer ${PAT2}" \
    -H "Accept: application/vnd.github+json" \
    -H "X-GitHub-Api-Version: 2022-11-28" \
    "$endpoint" \
    -d "$(printf '{"name":"%s","private":true,"description":"%s","auto_init":false}' "$name" "$desc")")
  case "$http_code" in
    201)
      echo "  created github.com/${owner}/${name}"
      rm -f "$body"; return 0 ;;
    422)
      # GitHub returns 422 for "already exists" AND for "name reserved / soft-
      # deleted". Distinguish by probing the repo via GET.
      local probe
      probe=$(curl -sS -o /dev/null -w '%{http_code}' \
        -H "Authorization: Bearer ${PAT2}" \
        "https://api.github.com/repos/${owner}/${name}")
      if [[ "$probe" == "200" ]]; then
        echo "  exists  github.com/${owner}/${name}"
        rm -f "$body"; return 0
      else
        echo "  SKIPPED github.com/${owner}/${name} — name reserved (422 but GET returns ${probe}); likely a prior soft-deleted repo. Use a different name or clear the reservation via GitHub UI." >&2
        cat "$body" >&2
        rm -f "$body"; return 2
      fi ;;
    *)
      echo "  FAILED create $name (HTTP $http_code):" >&2
      cat "$body" >&2
      rm -f "$body"
      return 1
      ;;
  esac
}

# --- snapshot push ------------------------------------------------------------
# Force-push the current working-tree content of a folder to a remote's main
# branch, as a single new commit. Bypasses subtree-split's history traversal
# (which breaks when files were moved between paths in master's history).
#
# Args:
#   $1 = local folder (relative to cwd), e.g. .cursor/skills/foo or tools/foo_mcp
#   $2 = remote URL (may include username but NOT password; GIT_ASKPASS handles auth)
#   $3 = commit message (optional; default: "Snapshot from master @ <short-sha>")
_push_snapshot() {
  local src="$1" url="$2" msg="${3:-}"
  if [[ ! -d "$src" ]]; then
    echo "  skip — $src not found" >&2
    return 1
  fi

  local master_sha
  master_sha="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
  : "${msg:=Snapshot from master @ ${master_sha}}"

  local tmpdir
  tmpdir="$(mktemp -d)"
  # Copy only files git tracks under $src — this honours .gitignore,
  # excludes __pycache__, compiled artefacts, etc.
  local count=0
  while IFS= read -r f; do
    local rel="${f#"$src"/}"
    mkdir -p "$tmpdir/$(dirname "$rel")"
    cp "$f" "$tmpdir/$rel"
    count=$((count + 1))
  done < <(git ls-files -- "$src")

  if (( count == 0 )); then
    echo "  skip — no tracked files under $src" >&2
    rm -rf "$tmpdir"
    return 1
  fi

  (
    cd "$tmpdir"
    git init -q -b main
    git -c user.email="publish-bot@local" -c user.name="publish-bot" add -A
    git -c user.email="publish-bot@local" -c user.name="publish-bot" \
        commit -q -m "$msg"
    # Force-push because each snapshot replaces remote main entirely.
    git push --force -q "$url" HEAD:main
  )
  local rc=$?
  rm -rf "$tmpdir"
  if (( rc != 0 )); then
    echo "  FAILED push to $url" >&2
    return $rc
  fi
  echo "  pushed ($count files) -> $url"
}
