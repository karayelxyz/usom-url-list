#!/usr/bin/env bash
#
# USOM address feed sync engine (offset-free capture, mirror-exact output).
#
# Capture strategy:
#   * Every request is scoped to a publication-date window (date_gte / date_lte,
#     documented in the API OpenAPI spec at /api/openapi.yaml) and every response
#     is verified: the API must return exactly min(totalCount, per-page, left)
#     records. A short page is retried and can never be accepted silently.
#   * The feed is walked downwards by shrinking date_lte to the oldest
#     publication date of the page just received. No offset is used, so records
#     inserted while the sync runs cannot shift a page boundary: nothing is
#     skipped and nothing but the boundary group is seen twice.
#   * Boundary records are part of both windows by design and are removed by
#     de-duplicating on the record id, which is unique.
#   * The only place offsets are used is a single timestamp group larger than one
#     page, which cannot be split by date. Such a group is frozen: ordinary
#     inserts always carry a newer date.
#
# Completeness proof for the full sync:
#   The number of distinct ids must cover the API's own totalCount before raw.txt
#   is replaced, and the enumeration is retried up to three times. A partial or
#   flaky enumeration fails loudly and leaves the published list as is.
#
# Usage: sync.sh <incremental|full>
#   incremental  add records published since the stored watermark
#   full         rebuild raw.txt as an exact mirror of the API
#
# Environment: PER_PAGE (default 9999), DRY_RUN=1 to skip publishing.
#
set -euo pipefail

MODE="${1:-incremental}"
API_URL="${API_URL:-https://siberguvenlik.gov.tr/api/address/index}"
PER_PAGE="${PER_PAGE:-9999}"            # documented API maximum
REQUEST_DELAY="${REQUEST_DELAY:-0.3}"   # politeness delay between API calls
WATERMARK_OVERLAP=3600                  # re-read the tail of the window; de-duplicated
BOOTSTRAP_FROM="${BOOTSTRAP_FROM:-2017-01-01}"
MAX_REQUESTS=2000                       # hard stop against pathological loops
MAX_PUSH_ATTEMPTS=5
MAX_ENUM_ATTEMPTS=3
PAGE_ATTEMPTS=3
REMOVAL_TOLERANCE=25
MIN_PLAUSIBLE_URLS=1000

RAW="raw.txt"
UBO="ubo.txt"
STATE="state.json"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

log() { printf '[%s] %s\n' "$MODE" "$*"; }
urlencode() { jq -sRr @uri <<<"$1"; }
to_epoch() { date -u -d "${1%%.*}" +%s; }

# Ask the workflow to schedule a full reconciliation (mirror rebuild).
flag_reconcile() {
  NEED_FULL=1
  log "reconciliation requested: $*"
  if [ -n "${GITHUB_OUTPUT:-}" ]; then
    printf 'reconcile=true\n' >> "$GITHUB_OUTPUT"
  fi
}

REQUEST_COUNT=0
API_TOTAL=0
WIN_COUNT=0

# Fetch one page of one window. Verifies the page length before accepting it.
# Sets API_TOTAL and WIN_COUNT, and writes the records to $WORK/win.tsv.
probe() {
  local gte="$1" lte="$2" page="$3" qs json expect remaining attempt
  qs="per-page=${PER_PAGE}&page=${page}&date_gte=$(urlencode "$gte")"
  if [ -n "$lte" ]; then
    qs="${qs}&date_lte=$(urlencode "$lte")"
  fi

  attempt=1
  while :; do
    REQUEST_COUNT=$((REQUEST_COUNT + 1))
    if [ "$REQUEST_COUNT" -gt "$MAX_REQUESTS" ]; then
      log "ERROR: request budget (${MAX_REQUESTS}) exhausted"
      return 1
    fi

    json="$(curl -fsSL --retry 5 --retry-all-errors --retry-delay 5 \
                 --retry-max-time 300 --max-time 120 -A "usom-sync/1.0" \
                 "${API_URL}?${qs}")"
    jq -e '.models' <<<"$json" >/dev/null
    API_TOTAL="$(jq -r '.totalCount' <<<"$json")"
    WIN_COUNT="$(jq -r '.count' <<<"$json")"

    remaining=$(( API_TOTAL - (page - 1) * PER_PAGE ))
    expect="$PER_PAGE"
    if [ "$remaining" -lt "$PER_PAGE" ]; then expect="$remaining"; fi
    if [ "$expect" -lt 0 ]; then expect=0; fi

    if [ "$WIN_COUNT" -eq "$expect" ]; then break; fi
    if [ "$attempt" -ge "$PAGE_ATTEMPTS" ]; then
      log "ERROR: inconsistent page (gte=${gte} lte=${lte:-open} page=${page}): got ${WIN_COUNT}, expected ${expect} of ${API_TOTAL}"
      return 1
    fi
    log "inconsistent page (got ${WIN_COUNT}, expected ${expect}); retrying"
    attempt=$((attempt + 1))
    sleep $((attempt * 2))
  done

  jq -r '.models[] | [.id, .date, .url] | @tsv' <<<"$json" > "$WORK/win.tsv"
  sleep "$REQUEST_DELAY"
}

# Global API size, independent of any filter.
global_total() {
  local json
  json="$(curl -fsSL --retry 5 --retry-all-errors --retry-delay 5 \
               --retry-max-time 300 --max-time 120 -A "usom-sync/1.0" \
               "${API_URL}?per-page=1&page=1")"
  jq -e '.models' <<<"$json" >/dev/null
  jq -r '.totalCount' <<<"$json"
}

append_window() {
  cat "$WORK/win.tsv" >> "$WORK/records.tsv"
  SEEN=$((SEEN + WIN_COUNT))
}

# One exact timestamp group. The only place where offsets are used, because such
# a group cannot be split by date. The API reports its exact size, and the sum of
# the pages must match it.
scan_timestamp_group() {
  local t="$1" page=1 sum=0 expected=0
  while :; do
    probe "$t" "$t" "$page"
    if [ "$page" -eq 1 ]; then expected="$API_TOTAL"; fi
    if [ "$WIN_COUNT" -eq 0 ]; then break; fi
    append_window
    sum=$((sum + WIN_COUNT))
    if [ "$WIN_COUNT" -lt "$PER_PAGE" ]; then break; fi
    page=$((page + 1))
  done
  if [ "$sum" -ne "$expected" ]; then
    log "ERROR: timestamp group ${t} enumerated ${sum} of ${expected} records"
    return 1
  fi
  log "timestamp group ${t}: ${sum} records"
}

# Walk the feed downwards from $1 by shrinking date_lte. No offsets are used.
scan_feed() {
  local gte="$1" lte="" oldest
  while :; do
    probe "$gte" "$lte" 1
    if [ "$WIN_COUNT" -eq 0 ]; then
      log "feed from ${gte}: exhausted"
      return 0
    fi
    append_window

    if [ "$WIN_COUNT" -lt "$PER_PAGE" ]; then
      return 0
    fi

    oldest="$(cut -f2 "$WORK/win.tsv" | sort | head -1)"
    if [ -z "$oldest" ]; then
      log "ERROR: page has no oldest date (gte=${gte} lte=${lte:-open})"
      return 1
    fi
    if [ -n "$lte" ] && [ "$oldest" = "$lte" ]; then
      scan_timestamp_group "$oldest"
      return $?
    fi
    if [ "$oldest" = "$gte" ]; then
      log "ERROR: no downward progress at ${gte}"
      return 1
    fi
    lte="$oldest"
  done
}

# Enumerate [from .. now] and de-duplicate on the record id.
enumerate() {
  : > "$WORK/records.tsv"
  SEEN=0
  scan_feed "$1"

  sort -t "$(printf '\t')" -k1,1n -u "$WORK/records.tsv" \
    | sort -t "$(printf '\t')" -k1,1nr > "$WORK/unique.tsv"
  UNIQUE_IDS="$(wc -l < "$WORK/unique.tsv" | tr -d ' ')"
  LAST_DATE="$(cut -f2 "$WORK/unique.tsv" | sort | tail -1)"
  LAST_ID="$(cut -f1 "$WORK/unique.tsv" | sort -n | tail -1)"
  log "enumerated=${SEEN} distinct_ids=${UNIQUE_IDS} api_total=${GLOBAL_TOTAL} requests=${REQUEST_COUNT}"
}

# ---------------------------------------------------------------------------

[ -f "$RAW" ] || : > "$RAW"
: > "$WORK/records.tsv"
[ -f "$WORK/unique.tsv" ] || : > "$WORK/unique.tsv"
SEEN=0
UNIQUE_IDS=0
LAST_DATE=""
LAST_ID=0
NEED_FULL=0
NEW_COUNT=0

GLOBAL_TOTAL="$(global_total)"
log "api global total: ${GLOBAL_TOTAL}"

if [ "$MODE" = "full" ]; then
  enum_ok=0
  attempt=1
  while [ "$attempt" -le "$MAX_ENUM_ATTEMPTS" ]; do
    if enumerate "$BOOTSTRAP_FROM" && [ "$UNIQUE_IDS" -ge "$GLOBAL_TOTAL" ]; then
      enum_ok=1
      break
    fi
    log "enumeration attempt ${attempt}/${MAX_ENUM_ATTEMPTS} incomplete (${UNIQUE_IDS} of ${GLOBAL_TOTAL})"
    attempt=$((attempt + 1))
  done
  if [ "$enum_ok" -ne 1 ]; then
    log "ERROR: could not enumerate the feed completely; ${RAW} left untouched"
    exit 1
  fi

elif [ "$MODE" = "incremental" ]; then
  FROM="$(jq -r '.last_date // empty' "$STATE" 2>/dev/null || true)"
  if [ -n "$FROM" ]; then
    FROM="$(date -u -d "@$(( $(to_epoch "$FROM") - WATERMARK_OVERLAP ))" '+%Y-%m-%d %H:%M:%S')"
    log "window starts at ${FROM}"
  else
    FROM="1970-01-01"
    log "no watermark in ${STATE}: enumerating the newest page only"
    BOOTSTRAP_FROM="$FROM"
  fi
  enumerate "$FROM"

else
  log "ERROR: unknown mode '${MODE}' (expected: incremental|full)"
  exit 2
fi

if [ "$UNIQUE_IDS" -eq 0 ]; then
  log "no new records; nothing to publish"
  exit 0
fi

cut -f3 "$WORK/unique.tsv" | awk 'NF && !seen[$0]++' > "$WORK/api_urls.txt"

if [ "$MODE" = "full" ]; then
  cp "$WORK/api_urls.txt" "$WORK/next_raw.txt"
else
  awk 'NR==FNR { seen[$0]=1; next } !($0 in seen) { print; seen[$0]=1 }' \
      "$RAW" "$WORK/api_urls.txt" > "$WORK/new_urls.txt"
  NEW_COUNT="$(wc -l < "$WORK/new_urls.txt" | tr -d ' ')"
  cat "$WORK/new_urls.txt" "$RAW" > "$WORK/next_raw.txt"
  log "new urls: ${NEW_COUNT}"
fi

awk 'NF { print "||" $0 "^" }' "$WORK/next_raw.txt" > "$WORK/next_ubo.txt"

RAW_LINES="$(awk 'NF' "$WORK/next_raw.txt" | wc -l | tr -d ' ')"
UBO_LINES="$(grep -c '^||' "$WORK/next_ubo.txt" || true)"
if [ "$RAW_LINES" != "$UBO_LINES" ]; then
  log "ERROR: integrity check failed (raw=${RAW_LINES} ubo=${UBO_LINES})"
  exit 1
fi
if [ "$RAW_LINES" -lt "$MIN_PLAUSIBLE_URLS" ]; then
  log "ERROR: refusing to publish an implausibly small list (${RAW_LINES} urls)"
  exit 1
fi

# The published list outgrowing the API means records were removed upstream.
if [ $(( RAW_LINES - GLOBAL_TOTAL )) -gt "$REMOVAL_TOLERANCE" ]; then
  flag_reconcile "published=${RAW_LINES} exceeds api_total=${GLOBAL_TOTAL}"
fi

case "${DRY_RUN:-0}" in
  1|true|yes)
    log "DRY_RUN: would publish ${RAW_LINES} urls (+${NEW_COUNT} new, reconcile=${NEED_FULL})"
    exit 0
    ;;
esac

mv "$WORK/next_raw.txt" "$RAW"
mv "$WORK/next_ubo.txt" "$UBO"

PREV_DATE="$(jq -r '.last_date // empty' "$STATE" 2>/dev/null || true)"
PREV_COUNT="$(jq -r '.url_count // empty' "$STATE" 2>/dev/null || true)"
if [ "$PREV_DATE" = "$LAST_DATE" ] && [ "$PREV_COUNT" = "$RAW_LINES" ]; then
  log "watermark unchanged (${LAST_DATE}); state file left untouched"
else
  jq -n --arg mode "$MODE" --arg last_date "$LAST_DATE" --argjson last_id "$LAST_ID" \
        --argjson url_count "$RAW_LINES" --argjson api_total "$GLOBAL_TOTAL" \
        --arg updated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
        '{mode:$mode,last_date:$last_date,last_id:$last_id,url_count:$url_count,
          api_total:$api_total,updated_at:$updated_at}' > "$STATE"
fi

git add "$RAW" "$UBO" "$STATE"

attempt=0
while :; do
  if git diff --cached --quiet; then
    log "nothing to publish (${RAW_LINES} urls)"
    exit 0
  fi
  git commit -q -m "chore: ${MODE} sync - ${RAW_LINES} urls"

  if git push -q origin HEAD:main; then
    log "pushed: ${RAW_LINES} urls (mode ${MODE}, +${NEW_COUNT} new)"
    exit 0
  fi

  attempt=$((attempt + 1))
  if [ "$attempt" -ge "$MAX_PUSH_ATTEMPTS" ]; then
    log "ERROR: push rejected ${MAX_PUSH_ATTEMPTS} times"
    exit 1
  fi
  log "push rejected; refreshing remote and re-applying (attempt ${attempt})"
  git fetch -q --depth 1 origin main
  git reset -q --hard origin/main
  sleep $((attempt * 5))

  if [ "$MODE" = "full" ]; then
    cp "$WORK/api_urls.txt" "$WORK/next_raw.txt"
  else
    awk 'NR==FNR { seen[$0]=1; next } !($0 in seen) { print; seen[$0]=1 }' \
        "$RAW" "$WORK/api_urls.txt" > "$WORK/new_urls.txt"
    cat "$WORK/new_urls.txt" "$RAW" > "$WORK/next_raw.txt"
  fi
  awk 'NF { print "||" $0 "^" }' "$WORK/next_raw.txt" > "$WORK/next_ubo.txt"
  mv "$WORK/next_raw.txt" "$RAW"
  mv "$WORK/next_ubo.txt" "$UBO"
  git add "$RAW" "$UBO" "$STATE"
done
