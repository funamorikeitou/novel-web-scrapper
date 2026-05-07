#!/bin/bash
# Daily scraper pipeline — pipelines controlled by [sync] in config.toml

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

LOG_FILE="$SCRIPT_DIR/sync.log"
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

[ -f "$SCRIPT_DIR/.env.local" ] && source "$SCRIPT_DIR/.env.local"

BOLD='\033[1m'
DIM='\033[2m'
GREEN='\033[32m'
YELLOW='\033[33m'
RED='\033[31m'
RESET='\033[0m'

[ -t 1 ] && INTERACTIVE=true || INTERACTIVE=false

print() { [ "$INTERACTIVE" = true ] && echo -e "$1"; }
log()   { echo "[$TIMESTAMP] $1" >> "$LOG_FILE"; }

# --- Load [sync] section from config.toml / config.local.toml ---
eval "$(uv run python -c "
import tomllib, os
cfg = {}
for f in ['config.toml', 'config.local.toml']:
    if os.path.exists(f):
        with open(f, 'rb') as fh:
            c = tomllib.load(fh)
            if 'sync' in c: cfg.update(c['sync'])
b = lambda k, d: 'true' if cfg.get(k, d) else 'false'
print('SYNC_NOVELS='    + b('novels',    True))
print('SYNC_BLOGS='     + b('blogs',     True))
print('SYNC_MEDIUM='    + b('medium',    True))
print('SYNC_RAINDROP='  + b('raindrop',  True))
print('SYNC_DOCS='      + b('docs',      True))
print('SYNC_SNOWFLAKE=' + b('snowflake', False))
sites = cfg.get('doc_sites', ['databricks', 'claude-code'])
print('DOC_SITES=\"' + ' '.join(sites) + '\"')
" 2>/dev/null)"

log "=== Pipeline started ==="
print "${BOLD}Scraper Pipeline${RESET}  ${DIM}$TIMESTAMP${RESET}"
print ""

# Temp files (one per pipeline + manifest of enabled ones)
NOVELS_OUT=$(mktemp)   BLOGS_OUT=$(mktemp)   MEDIUM_OUT=$(mktemp)
RAINDROP_OUT=$(mktemp) DOCS_OUT=$(mktemp)    SNOWFLAKE_OUT=$(mktemp)
MANIFEST=$(mktemp)   # "Label:outfile" lines written only for enabled pipelines

# --- Helpers ---

run_step() {
  local outfile=$1; shift
  local output exit_code
  output=$("$@" 2>&1); exit_code=$?
  echo "$output" >> "$outfile"
  echo "$output" | grep -qi "not configured\|vault path not configured\|token not configured" && \
    { echo "CONFIG_ERROR" >> "$outfile"; return 1; }
  return $exit_code
}

status_icon() {
  case "$1" in
    synced|done)  echo -e "${GREEN}✓${RESET}" ;;
    no_new)       echo -e "${YELLOW}–${RESET}" ;;
    config_error) echo -e "${RED}!${RESET}" ;;
    "")           echo -e "${DIM}…${RESET}" ;;
    *)            echo -e "${RED}✗${RESET}" ;;
  esac
}

format_status() {
  local label=$1 status=$2 elapsed=$3 icon color status_text
  case "$status" in
    synced)       icon="✓"; color="$GREEN"; status_text="synced" ;;
    done)         icon="✓"; color="$GREEN"; status_text="done" ;;
    no_new)       icon="–"; color="$YELLOW"; status_text="no new chapters" ;;
    config_error) icon="!"; color="$RED";   status_text="missing config" ;;
    *)            icon="✗"; color="$RED";   status_text="failed" ;;
  esac
  printf "  ${color}${icon}${RESET}  %-12s ${color}%-18s${RESET} ${DIM}%ss${RESET}\n" \
    "$label" "$status_text" "${elapsed:-?}"
}

# --- Pipeline functions ---

run_novels() {
  local start=$SECONDS check_output
  check_output=$(uv run python scrape_novels.py check --json 2>&1)
  echo "$check_output" > "$NOVELS_OUT"
  if echo "$check_output" | grep -qi "not configured"; then
    echo "STATUS:config_error" >> "$NOVELS_OUT"
  elif echo "$check_output" | grep -q '"has_new": true'; then
    if run_step "$NOVELS_OUT" uv run python scrape_novels.py sync --all && \
       run_step "$NOVELS_OUT" uv run python scrape_novels.py move --all; then
      echo "STATUS:synced" >> "$NOVELS_OUT"
    else
      echo "STATUS:failed" >> "$NOVELS_OUT"
    fi
  else
    echo "STATUS:no_new" >> "$NOVELS_OUT"
  fi
  echo "ELAPSED:$(( SECONDS - start ))" >> "$NOVELS_OUT"
}

run_blogs() {
  local start=$SECONDS
  if run_step "$BLOGS_OUT" uv run python scrape_blogs.py discover && \
     run_step "$BLOGS_OUT" uv run python scrape_blogs.py scrape --parallel && \
     run_step "$BLOGS_OUT" uv run python scrape_blogs.py move --all; then
    echo "STATUS:done" >> "$BLOGS_OUT"
  elif grep -q "^CONFIG_ERROR" "$BLOGS_OUT"; then
    echo "STATUS:config_error" >> "$BLOGS_OUT"
  else
    echo "STATUS:failed" >> "$BLOGS_OUT"
  fi
  echo "ELAPSED:$(( SECONDS - start ))" >> "$BLOGS_OUT"
}

run_medium() {
  local start=$SECONDS
  if run_step "$MEDIUM_OUT" uv run python scrape_medium.py discover && \
     run_step "$MEDIUM_OUT" uv run python scrape_medium.py scrape --parallel && \
     run_step "$MEDIUM_OUT" uv run python scrape_medium.py move --all; then
    echo "STATUS:done" >> "$MEDIUM_OUT"
  elif grep -q "^CONFIG_ERROR" "$MEDIUM_OUT"; then
    echo "STATUS:config_error" >> "$MEDIUM_OUT"
  else
    echo "STATUS:failed" >> "$MEDIUM_OUT"
  fi
  echo "ELAPSED:$(( SECONDS - start ))" >> "$MEDIUM_OUT"
}

run_raindrop() {
  local start=$SECONDS
  if run_step "$RAINDROP_OUT" uv run python scrape_raindrop.py discover && \
     run_step "$RAINDROP_OUT" uv run python scrape_raindrop.py scrape --parallel && \
     run_step "$RAINDROP_OUT" uv run python scrape_raindrop.py move --all; then
    echo "STATUS:done" >> "$RAINDROP_OUT"
  elif grep -q "^CONFIG_ERROR" "$RAINDROP_OUT"; then
    echo "STATUS:config_error" >> "$RAINDROP_OUT"
  else
    echo "STATUS:failed" >> "$RAINDROP_OUT"
  fi
  echo "ELAPSED:$(( SECONDS - start ))" >> "$RAINDROP_OUT"
}

run_docs() {
  local start=$SECONDS failed=0
  for site in $DOC_SITES; do
    run_step "$DOCS_OUT" uv run python scrape_docs.py discover --site "$site" && \
    run_step "$DOCS_OUT" uv run python scrape_docs.py scrape --parallel --site "$site" && \
    run_step "$DOCS_OUT" uv run python scrape_docs.py move --all --site "$site" || failed=1
  done
  if grep -q "^CONFIG_ERROR" "$DOCS_OUT"; then
    echo "STATUS:config_error" >> "$DOCS_OUT"
  elif [ $failed -eq 0 ]; then
    echo "STATUS:done" >> "$DOCS_OUT"
  else
    echo "STATUS:failed" >> "$DOCS_OUT"
  fi
  echo "ELAPSED:$(( SECONDS - start ))" >> "$DOCS_OUT"
}

run_snowflake() {
  local start=$SECONDS
  if run_step "$SNOWFLAKE_OUT" uv run python scrape_snowflake.py discover && \
     run_step "$SNOWFLAKE_OUT" uv run python scrape_snowflake.py scrape --parallel && \
     run_step "$SNOWFLAKE_OUT" uv run python scrape_snowflake.py move --all; then
    echo "STATUS:done" >> "$SNOWFLAKE_OUT"
  elif grep -q "^CONFIG_ERROR" "$SNOWFLAKE_OUT"; then
    echo "STATUS:config_error" >> "$SNOWFLAKE_OUT"
  else
    echo "STATUS:failed" >> "$SNOWFLAKE_OUT"
  fi
  echo "ELAPSED:$(( SECONDS - start ))" >> "$SNOWFLAKE_OUT"
}

# --- Spinner ---

spinner() {
  local manifest=$1
  local frames=('⠋' '⠙' '⠹' '⠸' '⠼' '⠴' '⠦' '⠧' '⠇' '⠏')
  local i=0
  while true; do
    local line=""
    while IFS=: read -r label outfile; do
      local st
      st=$(grep "^STATUS:" "$outfile" 2>/dev/null | cut -d: -f2)
      line+="  $(status_icon "$st") ${label}"
    done < "$manifest"
    printf "\r  ${frames[$i]}${line}  " >&2
    i=$(( (i + 1) % ${#frames[@]} ))
    sleep 0.15
  done
}

# --- Launch enabled pipelines ---

TOTAL_START=$SECONDS
PIDS=()

[ "$SYNC_NOVELS"    = true ] && { run_novels    & PIDS+=($!); echo "Novels:$NOVELS_OUT"       >> "$MANIFEST"; }
[ "$SYNC_BLOGS"     = true ] && { run_blogs     & PIDS+=($!); echo "Blogs:$BLOGS_OUT"         >> "$MANIFEST"; }
[ "$SYNC_MEDIUM"    = true ] && { run_medium    & PIDS+=($!); echo "Medium:$MEDIUM_OUT"       >> "$MANIFEST"; }
[ "$SYNC_RAINDROP"  = true ] && { run_raindrop  & PIDS+=($!); echo "Raindrop:$RAINDROP_OUT"   >> "$MANIFEST"; }
[ "$SYNC_DOCS"      = true ] && { run_docs      & PIDS+=($!); echo "Docs:$DOCS_OUT"           >> "$MANIFEST"; }
[ "$SYNC_SNOWFLAKE" = true ] && { run_snowflake & PIDS+=($!); echo "Snowflake:$SNOWFLAKE_OUT" >> "$MANIFEST"; }

if [ ${#PIDS[@]} -eq 0 ]; then
  log "No pipelines enabled — check [sync] in config.toml"
  print "${RED}No pipelines enabled.${RESET} Edit [sync] section in config.toml to enable some."
  rm -f "$NOVELS_OUT" "$BLOGS_OUT" "$MEDIUM_OUT" "$RAINDROP_OUT" "$DOCS_OUT" "$SNOWFLAKE_OUT" "$MANIFEST"
  exit 0
fi

if [ "$INTERACTIVE" = true ]; then
  spinner "$MANIFEST" &
  SPINNER_PID=$!
  wait "${PIDS[@]}"
  kill "$SPINNER_PID" 2>/dev/null
  wait "$SPINNER_PID" 2>/dev/null
  printf "\r\033[2K" >&2
else
  wait "${PIDS[@]}"
fi

TOTAL_ELAPSED=$(( SECONDS - TOTAL_START ))

# --- Summarize ---

summary=""
while IFS=: read -r label outfile; do
  st=$(grep "^STATUS:" "$outfile" | cut -d: -f2)
  [ -n "$summary" ] && summary+=" | "
  summary+="${label}: ${st:-failed}"
done < "$MANIFEST"
log "$summary"

if [ "$INTERACTIVE" = true ]; then
  while IFS=: read -r label outfile; do
    st=$(grep "^STATUS:" "$outfile" | cut -d: -f2)
    el=$(grep "^ELAPSED:" "$outfile" | cut -d: -f2)
    format_status "$label" "${st:-failed}" "$el"
  done < "$MANIFEST"
  print ""
  print "${DIM}Done in ${TOTAL_ELAPSED}s — logged to sync.log${RESET}"
fi

# Append full output to log
while IFS=: read -r label outfile; do
  echo "=== $label ===" >> "$LOG_FILE"
  cat "$outfile" >> "$LOG_FILE"
done < "$MANIFEST"

# --- Discord webhook (optional) ---
if [ -n "$DISCORD_WEBHOOK_URL" ]; then
  curl -s -H "Content-Type: application/json" \
    -d "{\"content\":\"**Scraper Pipeline** ($TIMESTAMP)\\n$summary\"}" \
    "$DISCORD_WEBHOOK_URL" > /dev/null 2>&1
fi

rm -f "$NOVELS_OUT" "$BLOGS_OUT" "$MEDIUM_OUT" "$RAINDROP_OUT" "$DOCS_OUT" "$SNOWFLAKE_OUT" "$MANIFEST"
log "=== Pipeline finished ==="
