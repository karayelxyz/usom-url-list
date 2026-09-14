#!/usr/bin/env bash
#
# USOM address feed sync engine (id-ordered capture, mirror-exact output).
#
# Why the record id, not offsets and not dates:
#   * The feed is ordered by the record id, which is unique and assigned in
#     insertion order. With descending id pagination a newly inserted record can
#     only push an unseen record to a later page (a repeat, removed by
#     de-duplicating on the id); it can never push it past a page that is still
#     to be read. No cursor arithmetic is involved at all.
#   * The publication date is NOT a usable cursor: dates are far out of order
#     relative to ids (one page mixes 2015 and 2026 dates), so a date window can
#     permanently skip records. Measured: date-window capture enumerated 491,849
#     of 492,319 ids while descending pages reach all of them.
#   * Every page is verified: the API must return exactly
#     min(totalCount, per-page, records left on that page). A short page is
#     retried and can never be accepted silently (measured: one such page used to
#     drop 466 records unnoticed).
#
# Completeness proof for the full sync:
#   The number of distinct ids must cover the API's own totalCount before raw.txt
#   is replaced, and the enumeration is retried up to three times. A partial or
#   flaky enumeration fails loudly and leaves the published list as is.
#
# Usage: sync.sh <incremental|full>
#   incremental  add records inserted after the stored last_id
#   full         rebuild raw.txt as an exact mirror of the API
#
# Environment: PER_PAGE (default 9999), DRY_RUN=1 to skip publishing.
#
set -euo pipefail

MODE="${1:-incremental}"
API_URL="${API_URL:-https://siberguvenlik.gov.tr/api/address/index}"
PER_PAGE="${PER_PAGE:-9999}"            # documented API maximum
REQUEST_DELAY="${REQUEST_DELAY:-0.3}"   # politeness delay between API calls
MAX_REQUESTS=2000                       # hard stop against pathological loops
MAX_INCREMENTAL_PAGES=20                # catch-up cap for a long outage
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

# Fetch one page ordered by descending id. Verified before being accepted.
probe() {
  local page="$1" json expect remaining attempt
  attempt=1
  while :; do
    REQUEST_COUNT=$((REQUEST_COUNT + 1))
    if [ "$REQUEST_COUNT" -gt "$MAX_REQUESTS" ]; then
      log "ERROR: request budget (${MAX_REQUESTS}) exhausted"
      return 1
    fi

    json="$(curl -fsSL --retry 5 --retry-all-errors --retry-delay 5 \
                 --retry-max-time 300 --max-time 120 -A "usom-sync/1.0" \
                 "${API_URL}?per-page=${PER_PAGE}&page=${page}&sort=-id")"
    jq -e '.models' <<<"$json" >/dev/null
    API_TOTAL="$(jq -r '.totalCount' <<<"$json")"
    WIN_COUNT="$(jq -r '.count' <<<"$json")"

    remaining=$(( API_TOTAL - (page - 1) * PER_PAGE ))
    expect="$PER_PAGE"
    if [ "$remaining" -lt "$PER_PAGE" ]; then expect="$remaining"; fi
    if [ "$expect" -lt 0 ]; then expect=0; fi

    if [ "$WIN_COUNT" -eq "$expect" ]; then break; fi
    if [ "$attempt" -ge "$PAGE_ATTEMPTS" ]; then
      log "ERROR: inconsistent page ${page}: got ${WIN_COUNT}, expected ${expect} of ${API_TOTAL}"
      return 1
    fi
    log "inconsistent page ${page} (got ${WIN_COUNT}, expected ${expect}); retrying"
    attempt=$((attempt + 1))
    sleep $((attempt * 2))
  done

  jq -r '.models[] | [.id, .date, .url] | @tsv' <<<"$json" > "$WORK/win.tsv"
  sleep "$REQUEST_DELAY"
}

global_total() {
  local json
  json="$(curl -fsSL --retry 5 --retry-all-errors --retry-delay 5 \
               --retry-max-time 300 --max-time 120 -A "usom-sync/1.0" \
               "${API_URL}?per-page=1&page=1")"
  jq -e '.models' <<<"$json" >/dev/null
  jq -r '.totalCount' <<<"$json"
}

append_page() {
  cat "$WORK/win.tsv" >> "$WORK/records.tsv"
  SEEN=$((SEEN + WIN_COUNT))
}

# Walk descending pages. stop_id > 0 stops once the page reaches records that
# were already published; stop_id == 0 walks to the end of the feed.
enumerate() {
  local stop_id="$1" page=1 min_id limit
  : > "$WORK/records.tsv"
  SEEN=0
  limit="$MAX_REQUESTS"
  if [ "$MODE" = "incremental" ]; then limit="$MAX_INCREMENTAL_PAGES"; fi

  while :; do
    probe "$page"
    append_page

    if [ "$stop_id" -gt 0 ]; then
      min_id="$(cut -f1 "$WORK/win.tsv" | sort -n | head -1)"
      if [ -n "$min_id" ] && [ "$min_id" -le "$stop_id" ]; then
        log "page ${page}: reached previously published ids (min id ${min_id} <= ${stop_id})"
        break
      fi
    fi

    if [ "$WIN_COUNT" -lt "$PER_PAGE" ]; then
      log "page ${page}: end of feed reached"
      break
    fi

    page=$((page + 1))
    if [ "$page" -gt "$limit" ]; then
      log "ERROR: page limit (${limit}) reached before the walk completed"
      return 1
    fi
  done

  sort -t "$(printf '\t')" -k1,1n -u "$WORK/records.tsv" \
    | sort -t "$(printf '\t')" -k1,1nr > "$WORK/unique.tsv"
  UNIQUE_IDS="$(wc -l < "$WORK/unique.tsv" | tr -d ' ')"
  LAST_DATE="$(cut -f2 "$WORK/unique.tsv" | sort | tail -1)"
  LAST_ID="$(cut -f1 "$WORK/unique.tsv" | sort -n | tail -1)"
  log "pages=${page} fetched=${SEEN} distinct_ids=${UNIQUE_IDS} api_total=${GLOBAL_TOTAL} requests=${REQUEST_COUNT}"
}

# ---------------------------------------------------------------------------

[ -f "$RAW" ] || : > "$RAW"
: > "$WORK/records.tsv"
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
    # Records inserted while the walk runs are legitimately not part of this
    # snapshot, so the snapshot is checked against the total read beforehand.
    if enumerate 0 && [ "$UNIQUE_IDS" -ge "$GLOBAL_TOTAL" ]; then
      enum_ok=1
      break
    fi
    log "enumeration attempt ${attempt}/${MAX_ENUM_ATTEMPTS} short: ${UNIQUE_IDS} of ${GLOBAL_TOTAL}"
    attempt=$((attempt + 1))
  done
  if [ "$enum_ok" -ne 1 ]; then
    log "ERROR: could not enumerate the feed completely; ${RAW} left untouched"
    exit 1
  fi

elif [ "$MODE" = "incremental" ]; then
  STOP_ID="$(jq -r '.last_id // 0' "$STATE" 2>/dev/null || echo 0)"
  if [ -z "$STOP_ID" ] || [ "$STOP_ID" = "null" ]; then STOP_ID=0; fi
  log "walking descending pages until id <= ${STOP_ID}"
  enumerate "$STOP_ID"
else
  log "ERROR: unknown mode '${MODE}' (expected: incremental|full)"
  exit 2
fi

if [ "$UNIQUE_IDS" -eq 0 ]; then
  log "no records; nothing to publish"
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

PREV_ID="$(jq -r '.last_id // empty' "$STATE" 2>/dev/null || true)"
PREV_COUNT="$(jq -r '.url_count // empty' "$STATE" 2>/dev/null || true)"
if [ "$PREV_ID" = "$LAST_ID" ] && [ "$PREV_COUNT" = "$RAW_LINES" ]; then
  log "watermark unchanged (last_id ${LAST_ID}); state file left untouched"
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
