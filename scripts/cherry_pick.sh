#!/usr/bin/env bash
#
# cherry_pick.sh
#
# This script will:
#   1. Fetch the release tag/branch and the cherry-pick commit.
#   2. Create or reuse the release branch off the given release tag.
#   3. Cherry-pick the given commit onto a cherry-pick branch.
#   4. Push cherry-pick branch and open a GitHub compare link against the release branch.
#
# On conflict:
#   If a cherry-pick conflicts, prompt user to open another shell to resolve the
#   conflicts and stage them with `git add`, then come back to this prompt and
#   press Enter to resume.
#
# Usage:
#   scripts/cherry_pick.sh <release-tag> <commit>
#
# Example:
#   scripts/cherry_pick.sh v0.36.25 2325c82ba89bd93b4070ab6294872430410f5256
#
# Options:
#   -h, --help          Show this help

set -Eeuo pipefail

# ----------------------------------------------------------------------------
# configuration & state
# ----------------------------------------------------------------------------

readonly REMOTE="origin"

RELEASE_TAG=""     # release tag passed on the CLI (e.g. v0.36.25)
CP_COMMIT=""       # commit to cherry-pick
RELEASE_BASE=""    # commit the release tag points at
RELEASE_HEAD=""    # ref the cherry-pick branch is forked from
RELEASE_BRANCH=""  # release/$RELEASE_TAG
CP_BRANCH=""       # throwaway cherry-pick branch
RESTORE_BRANCH=""  # original branch to restore to

# ----------------------------------------------------------------------------
# output helpers
# ----------------------------------------------------------------------------

log()  { printf '\033[1;34m==>\033[0m [cherry-pick] %s\n' "$*"; }
warn() { printf '\033[1;33mWARN:\033[0m [cherry-pick] %s\n' "$*" >&2; }
err()  { printf '\033[1;31mERROR:\033[0m [cherry-pick] %s\n' "$*" >&2; }
die()  { err "$*"; exit 1; }

# ----------------------------------------------------------------------------
# utilities
# ----------------------------------------------------------------------------

usage() {
  # Print the leading comment block as help text
  local line
  { read -r _ # discard the shebang
    while IFS= read -r line && [[ $line == "#"* ]]; do
      line="${line#\#}"; printf '%s\n' "${line# }"
    done
  } < "$0"
  exit "${1:-0}"
}

open_url() {
  local u="$1"
  # MacOS
  if command -v open >/dev/null 2>&1; then
    open "$u" >/dev/null 2>&1 || true
  # Linux
  elif command -v xdg-open >/dev/null 2>&1; then
    xdg-open "$u" >/dev/null 2>&1 || true
  fi
}

# ----------------------------------------------------------------------------
# git state probes
# ----------------------------------------------------------------------------

cherry_pick_in_progress() {
  git rev-parse -q --verify CHERRY_PICK_HEAD >/dev/null 2>&1
}

unmerged_files() {
  git diff --name-only --diff-filter=U 2>/dev/null
}

working_tree_dirty() {
  ! git diff --quiet || ! git diff --cached --quiet
}

# ----------------------------------------------------------------------------
# cleanup & traps
# ----------------------------------------------------------------------------

restore_branch() {
  cherry_pick_in_progress && return 0
  [[ -n "$RESTORE_BRANCH" ]] || return 0
  [[ "$(git symbolic-ref -q --short HEAD || true)" == "$RESTORE_BRANCH" ]] && return 0
  git switch "$RESTORE_BRANCH" >/dev/null 2>&1 || warn "could not switch back to '$RESTORE_BRANCH'"
}

on_exit() {
  local code=$?
  [[ "$code" -eq 0 ]] && return 0
  if cherry_pick_in_progress; then
    warn "Exited with a cherry-pick still in progress on branch '$(git symbolic-ref -q --short HEAD || echo DETACHED)'."
    warn "Recover with: git cherry-pick --abort (then: git switch ${RESTORE_BRANCH:-<your branch>})"
  fi
}

# ----------------------------------------------------------------------------
# conflict resolution
# ----------------------------------------------------------------------------

# Block until the user has resolved + staged every conflict and confirmed.
wait_for_resolution() {
  while true; do
    printf '\nResolved and staged? Press Enter to continue (Ctrl-C to cancel): '
    read -r _ < /dev/tty 2>/dev/null || die "no terminal available; cannot continue interactively."
    [[ -z "$(unmerged_files)" ]] && break
    warn "Still unresolved: $(unmerged_files | tr '\n' ' '). 'git add' them, then press Enter again."
  done
}

# Finish an in-progress cherry-pick: skip if empty, else pause for conflict resolution.
finish_cherry_pick() {
  cherry_pick_in_progress || return 0

  # Empty cherry-pick: the commit's changes are already present on the release branch.
  if [[ -z "$(unmerged_files)" ]]; then
    warn "Commit's changes are already on '$RELEASE_BRANCH' (empty cherry-pick); skipping."
    git cherry-pick --skip
    return 0
  fi

  printf '\n'
  err "Cherry-pick stopped on a merge conflict."
  cat <<'EOF'

Resolve the conflicts in another shell, then stage them:
    git add <files>
When everything is staged, come back here and press Enter to continue.
(To cancel, press Ctrl-C, then run: git cherry-pick --abort)
EOF

  wait_for_resolution

  # Commit the resolution; if it turned out to net no change, skip it.
  if GIT_EDITOR=true git cherry-pick --continue; then
    return 0
  fi
  if cherry_pick_in_progress; then
    warn "Resolution left no changes (empty); skipping."
    git cherry-pick --skip
  fi
}

# ----------------------------------------------------------------------------
# pipeline steps
# ----------------------------------------------------------------------------

parse_args() {
  [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]] && usage 0
  [[ $# -eq 2 ]] || { err "expected exactly <release-tag> and one <commit>"; usage 1; }
  RELEASE_TAG="$1"
  CP_COMMIT="$2"
  RELEASE_BRANCH="release/$RELEASE_TAG"
}

preflight_checks() {
  git rev-parse --is-inside-work-tree >/dev/null 2>&1 || die "not inside a git repository"

  ! cherry_pick_in_progress \
    || die "a cherry-pick is already in progress. Finish it (git cherry-pick --continue) or abort it (git cherry-pick --abort) first."

  ! working_tree_dirty \
    || die "working tree has uncommitted changes. Commit or stash them first."
}

fetch_refs() {
  # Fetch the release tag
  git fetch --quiet --prune --tags "$REMOTE" || die "git fetch from '$REMOTE' failed"

  RELEASE_BASE="$(git rev-parse -q --verify "refs/tags/$RELEASE_TAG^{commit}" 2>/dev/null || true)"
  [[ -n "$RELEASE_BASE" ]] || die "release tag '$RELEASE_TAG' not found on '$REMOTE'."

  # Fetch the cherry-pick commit
  git fetch --quiet "$REMOTE" "$CP_COMMIT" 2>/dev/null || die "commit '$CP_COMMIT' not found on '$REMOTE'."
  CP_COMMIT="$(git rev-parse -q --verify "${CP_COMMIT}^{commit}")"
}

# Ensure release/<tag> exists: reuse it if present, else fork it off the tag.
ensure_release_branch() {
  if git show-ref --verify --quiet "refs/remotes/$REMOTE/$RELEASE_BRANCH"; then
    log "Reusing existing release branch '$RELEASE_BRANCH'."
    RELEASE_HEAD="refs/remotes/$REMOTE/$RELEASE_BRANCH"
  else
    log "Creating release branch '$RELEASE_BRANCH' off tag '$RELEASE_TAG'..."
    git switch --no-track -c "$RELEASE_BRANCH" "$RELEASE_BASE" >/dev/null
    git push -u "$REMOTE" "$RELEASE_BRANCH" || die "could not push release branch '$RELEASE_BRANCH'"
    RELEASE_HEAD="$RELEASE_BRANCH"
  fi
}

# Create the throwaway cherry-pick branch and apply the commit onto it.
apply_cherry_pick() {
  CP_BRANCH="cherry-pick-$(date +%s)"
  log "Creating branch '$CP_BRANCH' off '$RELEASE_BRANCH'..."
  git switch --no-track -c "$CP_BRANCH" "$RELEASE_HEAD" >/dev/null

  log "Cherry-picking $(git rev-parse --short "$CP_COMMIT") $(git show -s --format=%s "$CP_COMMIT")..."
  GIT_EDITOR=true git cherry-pick -x "$CP_COMMIT" || true
  finish_cherry_pick
}

# Push the branch and surface a PR compare link — or bail if there's nothing new.
open_pr() {
  if (( $(git rev-list --count "$RELEASE_HEAD..HEAD") == 0 )); then
    warn "'$CP_COMMIT' is already on '$RELEASE_BRANCH'; no new commits to push. Nothing to do."
    restore_branch
    git branch -D "$CP_BRANCH" >/dev/null 2>&1 || true
    exit 0
  fi

  log "Pushing '$CP_BRANCH' to '$REMOTE'..."
  git push -u "$REMOTE" "$CP_BRANCH" || die "git push failed"

  local url="https://github.com/PeerDB-io/peerdb/compare/$RELEASE_BRANCH...$CP_BRANCH?expand=1"
  restore_branch
  log "Open the PR ($CP_BRANCH → $RELEASE_BRANCH): $url"
  open_url "$url"
  log "After it's reviewed + merged, create a Release with target = '$RELEASE_BRANCH' to tag + deploy."
}

main() {
  parse_args "$@"
  preflight_checks
  fetch_refs

  RESTORE_BRANCH="$(git symbolic-ref -q --short HEAD || true)"
  trap on_exit EXIT

  ensure_release_branch
  apply_cherry_pick
  open_pr
}

main "$@"
