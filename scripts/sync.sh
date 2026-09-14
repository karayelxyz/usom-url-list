#!/usr/bin/env bash
#
# USOM address feed sync engine (drift-free capture, mirror-exact output).
#
# Capture strategy -- no offset paging over a live feed:
#   * Every request is scoped to a publication-date window (date_gte / date_lte,
#     documented in the API OpenAPI spec at /api/openapi.yaml).
#   * A CLOSED window (fully in the past) cannot receive new records, so offset
#     paging inside it cannot drift, and the API reports its exact record count,
#     which fixes the number of pages up front.
#   * The OPEN window (watermark .. now) is enumerated by splitting on the date
#     axis: the newest page is used only to pick a pivot, the closed part below
#     the pivot is enumerated, and the remaining newer part is handled by the
#     next iteration. Records inserted meanwhile carry a newer date and are seen
#     by a later iteration.
#   * The incremental run never splits: if its window is larger than one page it
#     asks for a full sync instead of risking a shifted offset.
#   * Boundary records of a split are counted twice by design; de-duplication is
#     done on the record id, which is unique.
#
# Completeness proof for the full sync:
#   The number of distinct ids must cover the API's own totalCount before raw.txt
#   is replaced, and the enumeration is retried up to three times. A partial or
#   flaky enumeration therefore fails loudly and leaves the published list as is.
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
PER_PAGE="${PER_PAGE:-9999}"      # documented API maximum
REQUEST_DELAY="${REQUEST_DELAY:-0.3}"   # politeness delay between API calls
OVERLAP_SECONDS=3600              # re-read the tail of the window; de-duplicated
BOOTSTRAP_FROM="${BOOTSTRAP_FROM:-2017-01-01}"
MAX_SPLIT_DEPTH=200
MAX_PUSH_ATTEMPTS=5
MAX_ENUM_ATTEMPTS=3
REMOVAL_TOLERANCE=25              # published-vs-api drift that triggers reconciliation
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
urlencode() { jq -sRr @uri <<<"$1"; }
to_epoch() { date -u -d "${1%%.*}" +%s; }

# Fetch one page of one window. Sets API_TOTAL (window totalCount) and WIN_COUNT,
# and writes the page records (id, date, url) to $WORK/win.tsv.
probe() {
  local gte="$1" lte="$2" page="$3" qs json
  qs="per-page=${PER_PAGE}&page=${page}&date_gte=$(urlencode "$gte")"
  if [ -n "$lte" ]; then
    qs="${qs}&date_lte=$(urlencode "$lte")"
  fi
  json="$(curl -fsSL --retry 5 --retry-all-errors --retry-delay 5 \
               --retry-max-time 300 --max-time 120 -A "usom-sync/1.0" \
               "${API_URL}?${qs}")"
  jq -e '.models' <<<"$json" >/dev/null
  API_TOTAL="$(jq -r '.totalCount' <<<"$json")"
  WIN_COUNT="$(jq -r '.count' <<<"$json")"
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

# Enumerate a closed (past) window completely: the page count is known exactly.
scan_closed() {
  local gte="$1" lte="$2" pages page
  probe "$gte" "$lte" 1
  if [ "$WIN_COUNT" -eq 0 ]; then return 0; fi
  append_window
  pages=$(( (API_TOTAL + PER_PAGE - 1) / PER_PAGE ))
  page=2
  while [ "$page" -le "$pages" ]; do
    probe "$gte" "$lte" "$page"
    append_window
    page=$((page + 1))
  done
}

# Enumerate an open window by offset. Only used when a single timestamp group is
# larger than one page, where date splitting cannot make progress. A shortfall is
# detected by the caller's id assertion.
page_open_window() {
  local gte="$1" page=1
  while :; do
    probe "$gte" "" "$page"
    if [ "$WIN_COUNT" -eq 0 ]; then break; fi
    append_window
    if [ "$WIN_COUNT" -lt "$PER_PAGE" ]; then break; fi
    page=$((page + 1))
  done
  log "open window from ${gte} enumerated by offset (page ${page})"
}

# Enumerate an open window (start .. now) by splitting on the date axis.
scan_open() {
  local cur="$1" depth=0 pivot
  while :; do
    if [ "$depth" -gt "$MAX_SPLIT_DEPTH" ]; then
      log "ERROR: split depth exceeded at [${cur} .. now]"
      return 1
    fi

    probe "$cur" "" 1
    if [ "$WIN_COUNT" -eq 0 ]; then
      log "open window from ${cur}: no records"
      return 0
    fi
    if [ "$API_TOTAL" -le "$PER_PAGE" ]; then
      append_window
      return 0
    fi

    pivot="$(cut -f2 "$WORK/win.tsv" | sort | head -1)"
    if [ -z "$pivot" ]; then
      log "ERROR: no split point found above ${cur}"
      return 1
    fi
    if [ "$(to_epoch "$pivot")" -le "$(to_epoch "$cur")" ]; then
      log "notice: timestamp group at ${cur} exceeds one page (${API_TOTAL} records)"
      page_open_window "$cur"
      return 0
    fi

    scan_closed "$cur" "$pivot"
    cur="$pivot"
    depth=$((depth + 1))
  done
}

# Full enumeration with de-duplication by id and a completeness assertion.
enumerate_full() {
  : > "$WORK/records.tsv"
  SEEN=0
  scan_open "$BOOTSTRAP_FROM"

  sort -t "$(printf '\t')" -k1,1n -u "$WORK/records.tsv" \
    | sort -t "$(printf '\t')" -k1,1nr > "$WORK/unique.tsv"
  UNIQUE_IDS="$(wc -l < "$WORK/unique.tsv" | tr -d ' ')"
  LAST_DATE="$(cut -f2 "$WORK/unique.tsv" | sort | tail -1)"
  LAST_ID="$(cut -f1 "$WORK/unique.tsv" | sort -n | tail -1)"
  log "enumerated=${SEEN} distinct_ids=${UNIQUE_IDS} api_total=${GLOBAL_TOTAL}"

  if [ "$UNIQUE_IDS" -lt "$GLOBAL_TOTAL" ]; then
    log "enumeration short: ${UNIQUE_IDS} of ${GLOBAL_TOTAL} records"
    return 1
  fi
  return 0
}

# ---------------------------------------------------------------------------

[ -f "$RAW" ] || : > "$RAW"
: > "$WORK/records.tsv"
SEEN=0
API_TOTAL=0
WIN_COUNT=0
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
    if enumerate_full; then enum_ok=1; break; fi
    log "enumeration attempt ${attempt}/${MAX_ENUM_ATTEMPTS} incomplete; retrying"
    attempt=$((attempt + 1))
  done
  if [ "$enum_ok" -ne 1 ]; then
    log "ERROR: could not enumerate the feed completely; ${RAW} left untouched"
    exit 1
  fi

elif [ "$MODE" = "incremental" ]; then
  FROM="$(jq -r '.last_date // empty' "$STATE" 2>/dev/null || true)"
  if [ -n "$FROM" ]; then
    FROM="$(date -u -d "@$(( $(to_epoch "$FROM") - OVERLAP_SECONDS ))" '+%Y-%m-%d %H:%M:%S')"
    log "window starts at ${FROM}"
  else
    FROM="1970-01-01"
    log "no watermark in ${STATE}: reading the newest page"
  fi

  probe "$FROM" "" 1
  if [ "$API_TOTAL" -gt "$PER_PAGE" ]; then
    flag_reconcile "${API_TOTAL} records pending, more than one page"
  elif [ "$WIN_COUNT" -gt 0 ]; then
    append_window
    sort -t "$(printf '\t')" -k1,1n -u "$WORK/records.tsv" \
      | sort -t "$(printf '\t')" -k1,1nr > "$WORK/unique.tsv"
    UNIQUE_IDS="$(wc -l < "$WORK/unique.tsv" | tr -d ' ')"
    LAST_DATE="$(cut -f2 "$WORK/unique.tsv" | sort | tail -1)"
    LAST_ID="$(cut -f1 "$WORK/unique.tsv" | sort -n | tail -1)"
    log "window returned ${WIN_COUNT} records (${API_TOTAL} expected)"
  else
    log "no new records in the window"
  fi
else
  log "ERROR: unknown mode '${MODE}' (expected: incremental|full)"
  exit 2
fi

if [ "$SEEN" -eq 0 ] && [ "$UNIQUE_IDS" -eq 0 ]; then
  log "nothing to publish"
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

  # Re-apply on top of whatever the remote now holds.
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
