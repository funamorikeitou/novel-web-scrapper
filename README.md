# Web Scraper Suite

Six scrapers that pull content from various sources, track state in DuckDB, and sync files to an Obsidian vault. All commands run via `uv run python <script> <command>`.

## Scrapers

| Script | Source | Obsidian folder | Tags |
|--------|--------|-----------------|------|
| `scrape_novels.py` | Web novels (3 sites) | Novels vault | `book/novel` |
| `scrape_blogs.py` | Databricks blog + glossary | Clippings vault | `clippings`, `databricks` |
| `scrape_medium.py` | Medium RSS by username | Clippings vault | `clippings`, `medium` |
| `scrape_raindrop.py` | Raindrop.io bookmarks | Clippings vault | `clippings`, `raindrop` |
| `scrape_snowflake.py` | Snowflake blog | Clippings vault | `clippings`, `snowflake` |
| `scrape_docs.py` | Docs sites (Databricks, Claude Code, Snowflake, Soda, AWS) | Docs vault | `clippings`, `documentation` |

---

## Requirements

- Python 3.11+
- [uv](https://github.com/astral-sh/uv) package manager

```bash
uv sync
```

---

## Configuration

All settings live in `config.toml`. Machine-specific overrides (vault paths, API tokens) go in `config.local.toml`, which is gitignored.

### Set vault paths

```bash
uv run python scrape_novels.py    config set obsidian_vault "/path/to/vault/Novels"
uv run python scrape_blogs.py     config set obsidian_vault "/path/to/vault/Clippings"
uv run python scrape_medium.py    config set obsidian_vault "/path/to/vault/Clippings"
uv run python scrape_raindrop.py  config set obsidian_vault "/path/to/vault/Clippings"
uv run python scrape_snowflake.py config set obsidian_vault "/path/to/vault/Clippings"
uv run python scrape_docs.py      config set obsidian_vault "/path/to/vault/Docs"
```

### Set API tokens

```bash
# Raindrop — create test token at https://app.raindrop.io/settings/integrations
uv run python scrape_raindrop.py config set test_token "TOKEN"

# Medium (for member-only posts) — copy sid/uid from browser cookies
uv run python scrape_medium.py config set sid "SID"
uv run python scrape_medium.py config set uid "UID"
```

---

## Controlling which pipelines run

The `[sync]` section in `config.toml` (or `config.local.toml`) controls what `sync_all.sh` runs:

```toml
[sync]
novels    = true
blogs     = true
medium    = true
raindrop  = true
docs      = true
snowflake = false

# Which doc sites to include (options: snowflake, soda, aws, databricks, claude-code)
doc_sites = ["databricks", "claude-code"]
```

Set any pipeline to `false` to skip it. Add or remove entries from `doc_sites` to control which documentation sources are scraped.

---

## Novels (`scrape_novels.py`)

Supports **freewebnovel.com**, **novelbin.com**, and **lightnovelstranslations.com**.

```bash
# Add a novel
uv run python scrape_novels.py add --url "https://freewebnovel.com/my-vampire-system.html" --name "My Vampire System"

# Check for new chapters
uv run python scrape_novels.py check

# Download + sync new chapters to Obsidian
uv run python scrape_novels.py sync --all
uv run python scrape_novels.py move --all

# List tracked novels
uv run python scrape_novels.py list

# Import novels already in Obsidian
uv run python scrape_novels.py scan-obsidian
```

---

## Databricks Blog (`scrape_blogs.py`)

```bash
uv run python scrape_blogs.py discover          # find new posts
uv run python scrape_blogs.py scrape --parallel # download
uv run python scrape_blogs.py move --all        # copy to Obsidian
uv run python scrape_blogs.py status
uv run python scrape_blogs.py retry             # reset failed → pending
```

---

## Medium (`scrape_medium.py`)

```bash
uv run python scrape_medium.py add-user USERNAME
uv run python scrape_medium.py discover
uv run python scrape_medium.py scrape --parallel
uv run python scrape_medium.py move --all
uv run python scrape_medium.py status
```

---

## Raindrop (`scrape_raindrop.py`)

Medium URLs are automatically routed to `medium.db`. YouTube URLs are saved as transcripts under a `YouTube/` subfolder.

```bash
uv run python scrape_raindrop.py discover
uv run python scrape_raindrop.py scrape --parallel
uv run python scrape_raindrop.py move --all
uv run python scrape_raindrop.py status
uv run python scrape_raindrop.py fix            # diagnose failures
uv run python scrape_raindrop.py retry
```

---

## Snowflake Blog (`scrape_snowflake.py`)

```bash
uv run python scrape_snowflake.py discover
uv run python scrape_snowflake.py scrape --parallel
uv run python scrape_snowflake.py move --all
uv run python scrape_snowflake.py status
```

---

## Docs (`scrape_docs.py`)

Supported sites: `snowflake`, `soda`, `aws`, `databricks`, `claude-code`.

> **Warning:** `aws` without specific paths will discover 100k+ pages. Always set `aws_paths` in config first.

```bash
uv run python scrape_docs.py discover [--site KEY]
uv run python scrape_docs.py scrape --parallel [--site KEY]
uv run python scrape_docs.py move --all [--site KEY]
uv run python scrape_docs.py status [--site KEY]

# Limit AWS to specific services
uv run python scrape_docs.py config set aws_paths '["AmazonS3/latest/userguide"]'
```

---

## Daily Sync (`sync_all.sh`)

Runs all enabled pipelines in parallel. Novels are skipped if no new chapters are found.

```bash
./sync_all.sh
```

Output:

```
Scraper Pipeline  2026-05-07 03:00:00

  ✓  Novels       synced              42s
  ✓  Blogs        done                18s
  ✓  Medium       done                12s
  ✓  Raindrop     done                31s
  ✓  Docs         done               180s

Done in 184s — logged to sync.log
```

### Logs

| File | Contents |
|------|----------|
| `sync.log` | Timestamped summary + full pipeline output |
| `sync_launchd.log` | stdout/stderr from scheduled runs |

### Discord notifications (optional)

Create `.env.local` in the project directory:

```bash
DISCORD_WEBHOOK_URL="https://discord.com/api/webhooks/..."
```

---

## Scheduling on macOS (launchd)

A launchd plist is included that runs `sync_all.sh` daily at **2:00 AM**.

### Load (enable)

```bash
cp com.scraper.sync.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.scraper.sync.plist
```

### Verify it is scheduled

```bash
launchctl list | grep com.scraper.sync
```

### Run immediately (without waiting for 3AM)

```bash
launchctl start com.scraper.sync
```

### Unload (disable)

```bash
launchctl unload ~/Library/LaunchAgents/com.scraper.sync.plist
```

### Change the schedule

Edit `com.scraper.sync.plist` and update `StartCalendarInterval`:

```xml
<key>StartCalendarInterval</key>
<dict>
    <key>Hour</key>
    <integer>2</integer>    <!-- 0–23 -->
    <key>Minute</key>
    <integer>0</integer>
</dict>
```

Then reload:

```bash
launchctl unload ~/Library/LaunchAgents/com.scraper.sync.plist
cp com.scraper.sync.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.scraper.sync.plist
```

> **Note:** launchd only runs when your Mac is awake. If the Mac is asleep at 3AM, the job is skipped until the next day. For reliable scheduling, consider keeping the Mac awake overnight or adjusting the time.

---

## Troubleshooting

```bash
# Check pipeline status
uv run python scrape_novels.py list
uv run python scrape_blogs.py status
uv run python scrape_raindrop.py status
uv run python scrape_docs.py status

# Retry failed items
uv run python scrape_<name>.py retry

# Medium cookies expired — refresh from browser (F12 → Application → Cookies → medium.com)
uv run python scrape_medium.py config set sid "NEW_SID"
uv run python scrape_medium.py config set uid "NEW_UID"

# Diagnose Raindrop failures
uv run python scrape_raindrop.py fix --json
```

---

## Files

| File | Description |
|------|-------------|
| `scrape_novels.py` | Novel scraper (3 sites) |
| `scrape_blogs.py` | Databricks blog scraper |
| `scrape_medium.py` | Medium RSS scraper |
| `scrape_raindrop.py` | Raindrop.io bookmark scraper |
| `scrape_snowflake.py` | Snowflake blog scraper |
| `scrape_docs.py` | Multi-site documentation scraper |
| `sync_all.sh` | Parallel daily sync pipeline |
| `com.scraper.sync.plist` | macOS launchd schedule (3AM daily) |
| `config.toml` | Default configuration (committed) |
| `config.local.toml` | Machine-specific overrides (gitignored) |
| `novels.db` | Novel tracking database (committed) |

---

For personal use only. Respect the original content creators and website terms of service.
