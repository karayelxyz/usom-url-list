#!/usr/bin/env bash
#
# Guard the GitHub Pages deployment of the commit that was just pushed.
#
# The legacy Pages pipeline runs "pages build and deployment" for every push to
# the publishing branch. When that run fails for a transient reason - observed:
#   Failed to FinalizeArtifact: (403) Forbidden, error from intermediary
# the published site stays stale and the Pages build is left in the "building"
# state, and GitHub does not retry it. This guard waits for that run and re-runs
# it once, so a transient failure heals itself instead of waiting for the next
# push (which may be hours away, or never, when the feed has nothing new).
#
# Environment:
#   GH_TOKEN           token with actions: write
#   GITHUB_REPOSITORY  owner/repo
#   TARGET_SHA         commit that was pushed (optional; the newest Pages run is
#                      used when it is empty or when no run matches it)
#   MAX_WAIT_SECONDS   per-attempt wait limit (default 360)
#   POLL_SECONDS       poll interval (default 15)
#   APPEAR_TIMEOUT     how long to wait for the run to appear (default 120)
#
set -euo pipefail

MAX_WAIT_SECONDS="${MAX_WAIT_SECONDS:-360}"
POLL_SECONDS="${POLL_SECONDS:-15}"
APPEAR_TIMEOUT="${APPEAR_TIMEOUT:-120}"
TARGET_SHA="${TARGET_SHA:-}"
GITHUB_REPOSITORY="${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is required}"
PAGES_WORKFLOW_PATH="dynamic/pages/pages-build-deployment"

log() { printf '[pages-guard] %s\n' "$*"; }

# Print "<run id> <run attempt>" for the Pages build of TARGET_SHA, or for the
# newest Pages build when no commit matches.
find_run() {
  local query json
  if [ -n "$TARGET_SHA" ]; then
    query="head_sha=${TARGET_SHA}&event=dynamic&per_page=30"
  else
    query="event=dynamic&per_page=30"
  fi
  json="$(gh api "repos/${GITHUB_REPOSITORY}/actions/runs?${query}")"
  jq -r --arg path "$PAGES_WORKFLOW_PATH" '
    [.workflow_runs[] | select(.path == $path)]
    | if length > 0 then "\(.[0].id) \(.[0].run_attempt)" else "" end
  ' <<<"$json"
}

# Fetch one run into the RUN_* globals.
read_run() {
  local json
  json="$(gh api "repos/${GITHUB_REPOSITORY}/actions/runs/$1")"
  RUN_STATUS="$(jq -r '.status' <<<"$json")"
  RUN_CONCLUSION="$(jq -r '.conclusion // "none"' <<<"$json")"
  RUN_ATTEMPT="$(jq -r '.run_attempt' <<<"$json")"
}

# Wait until the run is completed at or above the given attempt. Prints the
# conclusion, or "timeout".
wait_completed() {
  local id="$1" min_attempt="$2" waited=0
  while [ "$waited" -lt "$MAX_WAIT_SECONDS" ]; do
    read_run "$id"
    if [ "$RUN_STATUS" = "completed" ] && [ "$RUN_ATTEMPT" -ge "$min_attempt" ]; then
      printf '%s\n' "$RUN_CONCLUSION"
      return 0
    fi
    sleep "$POLL_SECONDS"
    waited=$((waited + POLL_SECONDS))
  done
  printf 'timeout\n'
}

info=""
waited=0
while [ -z "$info" ] && [ "$waited" -lt "$APPEAR_TIMEOUT" ]; do
  if ! info="$(find_run)"; then
    log "notice: could not query Pages runs; skipping the guard"
    exit 0
  fi
  if [ -z "$info" ]; then
    sleep 10
    waited=$((waited + 10))
  fi
done

if [ -z "$info" ]; then
  log "no Pages build found for ${TARGET_SHA:-<newest>}; nothing to guard"
  exit 0
fi

run_id="${info%% *}"
run_attempt="${info##* }"
log "watching Pages build run ${run_id} (attempt ${run_attempt})"

conclusion="$(wait_completed "$run_id" "$run_attempt")"
log "Pages build run ${run_id} finished: ${conclusion}"

case "$conclusion" in
  success)
    exit 0
    ;;
  timeout)
    log "ERROR: Pages build did not finish within ${MAX_WAIT_SECONDS}s"
    exit 1
    ;;
esac

log "Pages build reported '${conclusion}'; re-running it once"
gh api -X POST "repos/${GITHUB_REPOSITORY}/actions/runs/${run_id}/rerun" >/dev/null
sleep 10

conclusion="$(wait_completed "$run_id" $((run_attempt + 1)))"
log "Pages build re-run finished: ${conclusion}"

if [ "$conclusion" != "success" ]; then
  log "ERROR: Pages build is still not successful after one retry"
  exit 1
fi
exit 0
