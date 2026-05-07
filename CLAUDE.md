# Web Scraper Suite

Six scrapers that track content with DuckDB and sync to Obsidian. Always run via `uv run python <script> <command>`.

## Scrapers

| Script | Source | DB | Tags |
|--------|--------|-----|------|
| `scrape_novels.py` | Web novels (3 sites) | `novels.db` | `book/novel` |
| `scrape_blogs.py` | Databricks blog + glossary | `blogs.db` | `clippings`, `databricks` |
| `scrape_medium.py` | Medium RSS by username | `medium.db` | `clippings`, `medium` |
| `scrape_raindrop.py` | Raindrop.io bookmarks | `raindrop.db` | `clippings`, `raindrop` |
| `scrape_snowflake.py` | Snowflake blog | `snowflake.db` | `clippings`, `snowflake` |
| `scrape_docs.py` | Docs sites (Snowflake, Soda, AWS, Databricks, Claude Code) | `docs.db` | `clippings`, `documentation` |

Each DB is separate because DuckDB is single-writer. Exception: Raindrop→Medium routing writes pending rows into `medium.db`.

---

## Novels (`scrape_novels.py`)

### Supported sites
| Site | URL pattern |
|------|-------------|
| lightnovelstranslations.com | `.../novel/<slug>/` |
| freewebnovel.com | `.../<slug>.html` |
| novelbin.com | `.../b/<slug>/` |

### Commands
```bash
uv run python scrape_novels.py add --url "<URL>" --name "Novel Name"
uv run python scrape_novels.py remove "Novel Name"
uv run python scrape_novels.py list [--json]
uv run python scrape_novels.py check [--name "Novel"] [--json]
uv run python scrape_novels.py sync --all [--sequential]
uv run python scrape_novels.py sync --name "Novel Name"
uv run python scrape_novels.py move --all
uv run python scrape_novels.py scan-obsidian          # import from Obsidian folder
uv run python scrape_novels.py config show
uv run python scrape_novels.py config set obsidian_vault "/path"
uv run python scrape_novels.py config set scraper.max_workers 6
```

### Config (`config.toml` → `[paths]` + `[scraper]`)
```toml
[paths]
staging_dir = "novels_obsidian"
obsidian_vault = "/path/to/vault/Novels"
database = "novels.db"

[scraper]
delay = 1.5
max_workers = 4
max_retries = 3
parallel_delay_multiplier = 2.0
```

### DB schema
```sql
novels (id, name, slug, url, site, status, total_chapters, ...)
chapters (id, novel_id, chapter_num, title, file_path, in_obsidian, ...)
sync_logs (id, novel_id, checked_at, latest_available, new_chapters_found, ...)
```

### Adding new sites
1. Subclass `NovelScraper`, set `SITE` + `BASE_URL`
2. Implement `get_chapter_list(slug)`, `scrape_chapter_by_url(url)`, `get_novel_status(slug)`
3. Add to `SCRAPERS` dict + update `extract_slug_from_url()`

---

## Databricks Blog (`scrape_blogs.py`)

Fetches Gatsby page-data JSON directly — no browser needed. Covers blog posts and glossary pages.

### Commands
```bash
uv run python scrape_blogs.py discover
uv run python scrape_blogs.py scrape [--parallel] [--workers 8] [--limit 50] [--slug "slug"]
uv run python scrape_blogs.py list [--status pending|downloaded|failed] [--json]
uv run python scrape_blogs.py status
uv run python scrape_blogs.py move --all
uv run python scrape_blogs.py retry                   # reset failed → pending
uv run python scrape_blogs.py config show
uv run python scrape_blogs.py config set obsidian_vault "/path"
```

### Config (`[blogs]`)
```toml
staging_dir = "blogs_obsidian"
obsidian_vault = ""
database = "blogs.db"
delay = 1.0
max_retries = 3
max_workers = 4
```

### DB schema
```sql
blog_posts (id, slug, url, title, author, publish_date, categories,
            word_count, char_count, file_path, status, downloaded_at,
            in_obsidian, moved_at, created_at, content_type)
blog_sync_logs (id, synced_at, total_in_sitemap, new_posts_found,
                posts_downloaded, posts_failed, status)
```
Statuses: `pending` → `downloaded` | `failed`. `content_type`: `blog` | `glossary`.

---

## Medium (`scrape_medium.py`)

Uses RSS feed (`medium.com/feed/@username`). Discover stores full HTML in DB; scrape converts locally (no network needed).

### Commands
```bash
uv run python scrape_medium.py add-user USERNAME
uv run python scrape_medium.py remove-user USERNAME
uv run python scrape_medium.py discover [--user USERNAME]
uv run python scrape_medium.py scrape [--parallel] [--workers 8] [--limit N] [--slug "slug"]
uv run python scrape_medium.py list [--status ...] [--user USERNAME] [--json]
uv run python scrape_medium.py status [--user USERNAME]
uv run python scrape_medium.py move --all
uv run python scrape_medium.py retry
uv run python scrape_medium.py config set sid "COOKIE"    # for member-only posts
uv run python scrape_medium.py config set uid "COOKIE"
```

### Config (`[medium]`)
```toml
staging_dir = "medium_obsidian"
obsidian_vault = ""
database = "medium.db"
delay = 1.0
max_retries = 3
max_workers = 4
users = []          # tracked usernames (without @)
```

### DB schema
```sql
medium_posts (id, slug, username, url, title, author, description,
              publish_date, updated_date, categories, content_html,
              word_count, char_count, file_path, status, downloaded_at,
              in_obsidian, moved_at, created_at)
medium_sync_logs (id, synced_at, username, total_in_feed, new_posts_found,
                  posts_downloaded, posts_failed, status)
```

---

## Raindrop (`scrape_raindrop.py`)

Fetches via Raindrop REST API. Medium URLs → routed to `medium.db`. YouTube URLs → transcript via `youtube-transcript-api`, saved to `YouTube/` subfolder with `youtube` tag.

### Commands
```bash
uv run python scrape_raindrop.py discover [--no-route-medium]
uv run python scrape_raindrop.py scrape [--parallel] [--workers 8] [--limit N] [--id ID]
uv run python scrape_raindrop.py list [--status pending|downloaded|failed|skipped_medium] [--json]
uv run python scrape_raindrop.py status
uv run python scrape_raindrop.py move --all
uv run python scrape_raindrop.py retry
uv run python scrape_raindrop.py fix [--json] [--id ID] [--limit N]   # diagnose failures
uv run python scrape_raindrop.py config set test_token "TOKEN"
uv run python scrape_raindrop.py config set obsidian_vault "/path"
uv run python scrape_raindrop.py config set route_medium false
```

API token: https://app.raindrop.io/settings/integrations → Create new app → Test token.

### Config (`[raindrop]`)
```toml
test_token = ""
staging_dir = "raindrop_obsidian"
obsidian_vault = ""
database = "raindrop.db"
delay = 1.0
max_retries = 3
max_workers = 4
route_medium = true
medium_domains = []    # extra Medium custom publication domains
```

### DB schema
```sql
raindrop_bookmarks (id, raindrop_id, url, title, domain, excerpt, note,
                    author, tags, bookmark_type, raindrop_created,
                    raindrop_updated, cover_url, word_count, char_count,
                    file_path, status, routed_to, downloaded_at,
                    in_obsidian, moved_at, created_at)
raindrop_sync_logs (id, synced_at, total_in_api, new_bookmarks_found,
                    bookmarks_downloaded, bookmarks_failed,
                    bookmarks_routed_medium, status)
```
Statuses: `pending` → `downloaded` | `failed` | `skipped_medium`.

---

## Snowflake Blog (`scrape_snowflake.py`)

Discovers from Snowflake XML sitemap, scrapes HTML via BeautifulSoup.

### Commands
```bash
uv run python scrape_snowflake.py discover
uv run python scrape_snowflake.py scrape [--parallel] [--workers 8] [--limit N]
uv run python scrape_snowflake.py list [--status ...] [--json]
uv run python scrape_snowflake.py status
uv run python scrape_snowflake.py move --all
uv run python scrape_snowflake.py retry
uv run python scrape_snowflake.py config set obsidian_vault "/path"
```

### Config (`[snowflake]`)
```toml
staging_dir = "snowflake_obsidian"
obsidian_vault = ""
database = "snowflake.db"
delay = 1.5
max_retries = 3
max_workers = 4
```

### DB schema
```sql
snowflake_posts (id, slug, url, title, author, publish_date, categories,
                 word_count, char_count, file_path, status, downloaded_at,
                 in_obsidian, moved_at, created_at)
snowflake_sync_logs (id, synced_at, total_in_sitemap, new_posts_found,
                     posts_downloaded, posts_failed, status)
```

---

## Docs (`scrape_docs.py`)

Shared DB across all doc sites. Site key distinguishes records.

### Supported sites
| Key | Label | URL | Discovery |
|-----|-------|-----|-----------|
| `snowflake` | Snowflake Docs | `https://docs.snowflake.com` | Next.js JSON |
| `soda` | Soda | `https://docs.soda.io` | Sitemap XML |
| `aws` | AWS | `https://docs.aws.amazon.com` | Sitemap XML (configurable paths) |
| `databricks` | Databricks Docs | `https://docs.databricks.com` | Sitemap XML |
| `claude-code` | Claude Code Docs | `https://code.claude.com/docs` | Sitemap XML |

### Commands
```bash
uv run python scrape_docs.py discover [--site KEY]
uv run python scrape_docs.py scrape [--parallel] [--workers 8] [--site KEY] [--limit N] [--slug PATH]
uv run python scrape_docs.py list [--site KEY] [--status ...] [--json]
uv run python scrape_docs.py status [--site KEY]
uv run python scrape_docs.py move --all [--site KEY]
uv run python scrape_docs.py retry [--site KEY]
uv run python scrape_docs.py config set obsidian_vault "/path"
uv run python scrape_docs.py config set aws_paths '["AmazonS3/latest/userguide"]'
uv run python scrape_docs.py config set databricks_paths '["aws/en"]'
```

### Config (`[docs]`)
```toml
staging_dir = "docs_obsidian"
obsidian_vault = ""
database = "docs.db"
delay = 1.0
max_retries = 3
max_workers = 4
aws_paths = []              # IMPORTANT: empty = 100k+ pages; set specific paths
databricks_paths = ["aws/en"]
```

### DB schema
```sql
doc_pages (id, site, slug, url, title, parent_path, word_count, char_count,
           file_path, status, error_reason, error_detail, downloaded_at,
           in_obsidian, moved_at, created_at)
doc_sync_logs (id, synced_at, site, total_discovered, new_pages_found,
               pages_downloaded, pages_failed, status)
```

### Adding new sites
1. Subclass `BaseSiteScraper`, decorate `@register_site('key')`
2. Set `SITE_KEY`, `SITE_LABEL`, `BASE_URL`
3. Implement `discover_pages()` → yields `{'slug', 'url', 'title'}`
4. Override `extra_tags(page)` for additional tags

---

## Daily Sync (`sync_all.sh`)

Runs all 6 pipelines in parallel via `bash & + wait`. Replaces Docker/n8n.

### Pipeline strategy
| Pipeline | Strategy | Scrape mode |
|----------|----------|-------------|
| Novels | Conditional — `check --json` first, skip if no new | Sequential |
| All others | Unconditional — discover + scrape + move every run | Parallel (4 workers) |

### Setup
```bash
# Set vault paths (save in config.local.toml)
uv run python scrape_novels.py config set obsidian_vault "/path/to/Novels"
uv run python scrape_blogs.py config set obsidian_vault "/path/to/Clippings"
uv run python scrape_medium.py config set obsidian_vault "/path/to/Clippings"
uv run python scrape_raindrop.py config set obsidian_vault "/path/to/Clippings"
uv run python scrape_snowflake.py config set obsidian_vault "/path/to/Clippings"
uv run python scrape_docs.py config set obsidian_vault "/path/to/Docs"

# API tokens
uv run python scrape_raindrop.py config set test_token "TOKEN"
uv run python scrape_medium.py config set sid "SID" && uv run python scrape_medium.py config set uid "UID"

# Test run
./sync_all.sh

# Schedule via launchd (daily 3AM)
cp com.scraper.sync.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.scraper.sync.plist

# Discord webhook (optional) — create .env.local:
# DISCORD_WEBHOOK_URL="https://discord.com/api/webhooks/..."
```

### Logs
| File | Contents |
|------|----------|
| `sync.log` | Timestamped summary + full pipeline output |
| `sync_launchd.log` | launchd stdout/stderr |

### Troubleshooting
```bash
# Per-pipeline status
uv run python scrape_novels.py list
uv run python scrape_blogs.py status
uv run python scrape_raindrop.py status
uv run python scrape_docs.py status

# Diagnose Raindrop failures
uv run python scrape_raindrop.py fix [--json]
uv run python scrape_raindrop.py retry

# Retry failed posts (any scraper)
uv run python scrape_<name>.py retry

# Medium cookies expired → refresh sid/uid from Chrome → F12 → Application → Cookies → medium.com
uv run python scrape_medium.py config set sid "NEW_SID"
uv run python scrape_medium.py config set uid "NEW_UID"
```

---

## Config files
- `config.toml` — defaults (committed)
- `config.local.toml` — machine overrides (gitignored)
- `novels.db` — tracked in git
- `blogs.db`, `medium.db`, `raindrop.db`, `docs.db`, `snowflake.db` — gitignored
