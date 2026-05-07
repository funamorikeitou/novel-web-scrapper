#!/usr/bin/env python3
"""
Raindrop.io Bookmark Scraper - Scrapes bookmarked web pages to Obsidian markdown.

Fetches bookmarks from Raindrop.io API, scrapes page content, and saves as
Obsidian markdown with tags: clippings, raindrop (plus user's Raindrop tags).
Medium URLs can be routed to the Medium scraper instead of being scraped directly.

Commands:
    discover    Fetch bookmarks from Raindrop.io API
    scrape      Download pending bookmarks as markdown
    list        List bookmarks by status
    status      Show summary statistics
    move        Move downloaded bookmarks to Obsidian vault
    retry       Reset failed bookmarks to pending
    fix         Show detailed failure report for LLM consumption
    config      View/set configuration
"""

import os
import re
import sys
import json
import time
import signal
import shutil
import tomllib
import argparse
import threading
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

import duckdb
import requests
from bs4 import BeautifulSoup

try:
    from youtube_transcript_api import YouTubeTranscriptApi
    from youtube_transcript_api._errors import TranscriptsDisabled, NoTranscriptFound, VideoUnavailable
    HAS_YOUTUBE_TRANSCRIPT = True
except ImportError:
    HAS_YOUTUBE_TRANSCRIPT = False


# =============================================================================
# Configuration
# =============================================================================

class Config:
    """Configuration manager for Raindrop scraper (reads [raindrop] section from TOML)"""

    DEFAULT_CONFIG = {
        'raindrop': {
            'test_token': '',
            'staging_dir': 'raindrop_obsidian',
            'obsidian_vault': '',
            'database': 'raindrop.db',
            'delay': 1.0,
            'max_retries': 3,
            'max_workers': 4,
            'route_medium': True,
            'medium_domains': [],
            'ollama_url': '',
            'ollama_model': '',
        }
    }

    def __init__(self, config_dir=None):
        self.config_dir = Path(config_dir) if config_dir else Path.cwd()
        self.config_file = self.config_dir / 'config.toml'
        self.local_config_file = self.config_dir / 'config.local.toml'
        self._config = None

    def load(self):
        """Load configuration from TOML files"""
        config = {}
        for key, val in self.DEFAULT_CONFIG.items():
            if isinstance(val, dict):
                config[key] = {}
                for k, v in val.items():
                    config[key][k] = v.copy() if isinstance(v, (dict, list)) else v
            else:
                config[key] = val

        if self.config_file.exists():
            with open(self.config_file, 'rb') as f:
                base = tomllib.load(f)
                self._merge_config(config, base)

        if self.local_config_file.exists():
            with open(self.local_config_file, 'rb') as f:
                local = tomllib.load(f)
                self._merge_config(config, local)

        self._config = config
        return config

    def _merge_config(self, base, override):
        """Deep merge override into base"""
        for key, value in override.items():
            if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                self._merge_config(base[key], value)
            else:
                base[key] = value

    def get(self, key, default=None):
        """Get a config value from [raindrop] section"""
        if self._config is None:
            self.load()
        return self._config.get('raindrop', {}).get(key, default)

    def set(self, key, value):
        """Set a config value in local config [raindrop] section"""
        local_config = {}
        if self.local_config_file.exists():
            with open(self.local_config_file, 'rb') as f:
                local_config = tomllib.load(f)

        if 'raindrop' not in local_config:
            local_config['raindrop'] = {}
        local_config['raindrop'][key] = value

        self._write_toml(self.local_config_file, local_config)
        self._config = None
        self.load()

    def _write_toml(self, path, config):
        """Write config dict to TOML file"""
        lines = []
        for section, values in config.items():
            if isinstance(values, dict):
                lines.append(f'[{section}]')
                for key, value in values.items():
                    if isinstance(value, str):
                        lines.append(f'{key} = "{value}"')
                    elif isinstance(value, bool):
                        lines.append(f'{key} = {str(value).lower()}')
                    elif isinstance(value, list):
                        items = ', '.join(f'"{v}"' for v in value)
                        lines.append(f'{key} = [{items}]')
                    else:
                        lines.append(f'{key} = {value}')
                lines.append('')

        with open(path, 'w') as f:
            f.write('\n'.join(lines))

    def get_medium_database(self):
        """Get the Medium database path from [medium] section"""
        if self._config is None:
            self.load()
        return self._config.get('medium', {}).get('database', 'medium.db')

    @property
    def test_token(self):
        return self.get('test_token', '')

    @property
    def staging_dir(self):
        return self.get('staging_dir', 'raindrop_obsidian')

    @property
    def obsidian_vault(self):
        return self.get('obsidian_vault', '')

    @property
    def database_path(self):
        return self.get('database', 'raindrop.db')

    @property
    def delay(self):
        return float(self.get('delay', 1.0))

    @property
    def max_retries(self):
        return int(self.get('max_retries', 3))

    @property
    def max_workers(self):
        return int(self.get('max_workers', 4))

    @property
    def route_medium(self):
        return bool(self.get('route_medium', True))

    @property
    def medium_domains(self):
        return self.get('medium_domains', [])


# =============================================================================
# Database
# =============================================================================

class RaindropDatabase:
    """DuckDB database manager for tracking Raindrop bookmarks"""

    SCHEMA = """
    CREATE TABLE IF NOT EXISTS raindrop_bookmarks (
        id INTEGER PRIMARY KEY,
        raindrop_id BIGINT NOT NULL UNIQUE,
        url VARCHAR NOT NULL,
        title VARCHAR,
        domain VARCHAR,
        excerpt VARCHAR,
        note VARCHAR,
        author VARCHAR,
        tags VARCHAR,
        bookmark_type VARCHAR,
        raindrop_created TIMESTAMP,
        raindrop_updated TIMESTAMP,
        cover_url VARCHAR,
        word_count INTEGER,
        char_count INTEGER,
        file_path VARCHAR,
        status VARCHAR DEFAULT 'pending',
        routed_to VARCHAR,
        downloaded_at TIMESTAMP,
        error_reason VARCHAR,
        error_detail VARCHAR,
        error_html TEXT,
        in_obsidian BOOLEAN DEFAULT FALSE,
        moved_at TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS raindrop_sync_logs (
        id INTEGER PRIMARY KEY,
        synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        total_in_api INTEGER,
        new_bookmarks_found INTEGER,
        bookmarks_downloaded INTEGER,
        bookmarks_failed INTEGER,
        bookmarks_routed_medium INTEGER,
        status VARCHAR
    );

    CREATE SEQUENCE IF NOT EXISTS raindrop_bookmarks_id_seq;
    CREATE SEQUENCE IF NOT EXISTS raindrop_sync_logs_id_seq;
    """

    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = None

    def connect(self):
        """Connect to database and initialize schema"""
        self.conn = duckdb.connect(self.db_path)
        for statement in self.SCHEMA.split(';'):
            statement = statement.strip()
            if statement:
                try:
                    self.conn.execute(statement)
                except Exception:
                    pass
        return self

    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            self.conn = None

    def __enter__(self):
        return self.connect()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def add_bookmark(self, raindrop_id, url, title=None, domain=None,
                     excerpt=None, note=None, tags=None, bookmark_type=None,
                     raindrop_created=None, raindrop_updated=None, cover_url=None):
        """Add a new bookmark (pending status)"""
        try:
            tags_str = json.dumps(tags) if tags else None
            self.conn.execute("""
                INSERT INTO raindrop_bookmarks (id, raindrop_id, url, title, domain,
                    excerpt, note, tags, bookmark_type, raindrop_created,
                    raindrop_updated, cover_url)
                VALUES (nextval('raindrop_bookmarks_id_seq'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [raindrop_id, url, title, domain, excerpt, note, tags_str,
                  bookmark_type, raindrop_created, raindrop_updated, cover_url])
            return True
        except duckdb.ConstraintException:
            return False

    def get_bookmark(self, raindrop_id):
        """Get a single bookmark by raindrop_id"""
        result = self.conn.execute(
            "SELECT * FROM raindrop_bookmarks WHERE raindrop_id = ?", [raindrop_id]
        ).fetchone()
        if result:
            columns = [
                'id', 'raindrop_id', 'url', 'title', 'domain', 'excerpt', 'note',
                'author', 'tags', 'bookmark_type', 'raindrop_created', 'raindrop_updated',
                'cover_url', 'word_count', 'char_count', 'file_path', 'status',
                'routed_to', 'error_reason', 'error_detail', 'error_html',
                'in_obsidian', 'moved_at', 'created_at'
            ]
            return dict(zip(columns, result))
        return None

    def get_bookmarks(self, status=None, limit=None):
        """Get bookmarks, optionally filtered by status"""
        query = "SELECT id, raindrop_id, url, title, domain, status, file_path, in_obsidian, routed_to FROM raindrop_bookmarks"
        params = []
        if status:
            query += " WHERE status = ?"
            params.append(status)
        query += " ORDER BY id"
        if limit:
            query += " LIMIT ?"
            params.append(limit)
        results = self.conn.execute(query, params).fetchall()
        columns = ['id', 'raindrop_id', 'url', 'title', 'domain', 'status', 'file_path', 'in_obsidian', 'routed_to']
        return [dict(zip(columns, row)) for row in results]

    def update_bookmark(self, raindrop_id, **kwargs):
        """Update bookmark fields"""
        valid_fields = [
            'title', 'author', 'word_count', 'char_count', 'file_path',
            'status', 'routed_to', 'downloaded_at', 'in_obsidian', 'moved_at',
            'error_reason', 'error_detail', 'error_html'
        ]
        updates = []
        values = []
        for field, value in kwargs.items():
            if field in valid_fields:
                updates.append(f"{field} = ?")
                values.append(value)
        if updates:
            values.append(raindrop_id)
            self.conn.execute(
                f"UPDATE raindrop_bookmarks SET {', '.join(updates)} WHERE raindrop_id = ?",
                values
            )

    def get_counts(self):
        """Get status counts"""
        results = self.conn.execute("""
            SELECT status, COUNT(*) FROM raindrop_bookmarks GROUP BY status
        """).fetchall()
        counts = {row[0]: row[1] for row in results}
        total = self.conn.execute("SELECT COUNT(*) FROM raindrop_bookmarks").fetchone()[0]
        counts['total'] = total
        return counts

    def get_unmoved_bookmarks(self):
        """Get downloaded bookmarks not yet in Obsidian"""
        results = self.conn.execute("""
            SELECT id, raindrop_id, url, title, domain, file_path
            FROM raindrop_bookmarks
            WHERE status = 'downloaded' AND in_obsidian = FALSE AND file_path IS NOT NULL
            ORDER BY id
        """).fetchall()
        columns = ['id', 'raindrop_id', 'url', 'title', 'domain', 'file_path']
        return [dict(zip(columns, row)) for row in results]

    def mark_moved(self, raindrop_ids):
        """Mark bookmarks as moved to Obsidian"""
        if not raindrop_ids:
            return
        placeholders = ','.join(['?'] * len(raindrop_ids))
        self.conn.execute(f"""
            UPDATE raindrop_bookmarks
            SET in_obsidian = TRUE, moved_at = CURRENT_TIMESTAMP
            WHERE raindrop_id IN ({placeholders})
        """, list(raindrop_ids))

    def reset_failed(self):
        """Reset failed bookmarks to pending and clear error fields"""
        self.conn.execute("""
            UPDATE raindrop_bookmarks
            SET status = 'pending', error_reason = NULL, error_detail = NULL, error_html = NULL
            WHERE status = 'failed'
        """)
        return self.conn.execute(
            "SELECT COUNT(*) FROM raindrop_bookmarks WHERE status = 'pending'"
        ).fetchone()[0]

    def get_failed_bookmarks(self, raindrop_id=None, limit=None):
        """Get failed bookmarks with error details"""
        query = """
            SELECT raindrop_id, url, title, domain, error_reason, error_detail, error_html
            FROM raindrop_bookmarks WHERE status = 'failed'
        """
        params = []
        if raindrop_id:
            query += " AND raindrop_id = ?"
            params.append(raindrop_id)
        query += " ORDER BY id"
        if limit:
            query += " LIMIT ?"
            params.append(limit)
        results = self.conn.execute(query, params).fetchall()
        columns = ['raindrop_id', 'url', 'title', 'domain', 'error_reason', 'error_detail', 'error_html']
        return [dict(zip(columns, row)) for row in results]

    def add_sync_log(self, total_api, new_found, downloaded, failed, routed_medium, status):
        """Add a sync log entry"""
        self.conn.execute("""
            INSERT INTO raindrop_sync_logs (id, total_in_api, new_bookmarks_found,
                                            bookmarks_downloaded, bookmarks_failed,
                                            bookmarks_routed_medium, status)
            VALUES (nextval('raindrop_sync_logs_id_seq'), ?, ?, ?, ?, ?, ?)
        """, [total_api, new_found, downloaded, failed, routed_medium, status])


# =============================================================================
# Raindrop Scraper
# =============================================================================

class RaindropScraper:
    """Handles Raindrop.io API and web page scraping.

    Fetches bookmarks from Raindrop.io REST API, then scrapes the actual
    web pages to extract article content as markdown.
    """

    API_BASE = 'https://api.raindrop.io/rest/v1'

    # Known Medium domains (besides medium.com and *.medium.com)
    DEFAULT_MEDIUM_DOMAINS = [
        'towardsdatascience.com',
        'betterprogramming.pub',
        'levelup.gitconnected.com',
        'javascript.plainenglish.io',
        'blog.devgenius.io',
        'python.plainenglish.io',
        'itnext.io',
        'blog.bitsrc.io',
        'medium.datadriveninvestor.com',
        'betterhumans.pub',
        'entrepreneurshandbook.co',
    ]

    def __init__(self, token, extra_medium_domains=None):
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {token}',
            'User-Agent': (
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            ),
        })
        self.medium_domains = set(self.DEFAULT_MEDIUM_DOMAINS)
        if extra_medium_domains:
            self.medium_domains.update(extra_medium_domains)

    # -------------------------------------------------------------------------
    # Raindrop.io API
    # -------------------------------------------------------------------------

    def fetch_all_bookmarks(self):
        """Fetch all bookmarks from Raindrop.io (paginates automatically).

        Uses GET /rest/v1/raindrops/0 which returns all unsorted bookmarks.
        Raindrop API uses collection_id=0 for "All bookmarks".
        """
        all_items = []
        page = 0
        per_page = 50

        while True:
            try:
                response = self.session.get(
                    f'{self.API_BASE}/raindrops/0',
                    params={'page': page, 'perpage': per_page},
                    timeout=30
                )
                response.raise_for_status()
                data = response.json()

                items = data.get('items', [])
                if not items:
                    break

                all_items.extend(items)
                page += 1

                if len(items) < per_page:
                    break

            except requests.exceptions.RequestException as e:
                print(f"  Error fetching page {page}: {e}")
                break

        return all_items

    # -------------------------------------------------------------------------
    # Medium URL detection
    # -------------------------------------------------------------------------

    def is_medium_url(self, url):
        """Check if a URL is a Medium post.

        Detects:
        - medium.com/@user/slug
        - *.medium.com/slug
        - Known Medium publication custom domains
        """
        try:
            parsed = urlparse(url)
            hostname = parsed.hostname or ''
        except Exception:
            return False

        if hostname == 'medium.com':
            return True
        if hostname.endswith('.medium.com'):
            return True
        if hostname in self.medium_domains:
            return True

        return False

    def _extract_medium_slug(self, url):
        """Extract a slug from a Medium URL for the medium_posts table."""
        try:
            parsed = urlparse(url)
            path = parsed.path.strip('/')
            # Medium URLs end with slug-<hex_id>
            # e.g. /some-article-title-abc123def456
            parts = path.split('/')
            slug = parts[-1] if parts else path
            # Remove .html suffix if present
            slug = re.sub(r'\.html$', '', slug)
            return slug or None
        except Exception:
            return None

    # -------------------------------------------------------------------------
    # YouTube URL detection and transcript fetching
    # -------------------------------------------------------------------------

    def is_youtube_url(self, url):
        """Check if a URL is a YouTube video.

        Detects: youtube.com, youtu.be, m.youtube.com
        """
        try:
            parsed = urlparse(url)
            hostname = parsed.hostname or ''
        except Exception:
            return False

        return hostname in ('youtube.com', 'www.youtube.com', 'm.youtube.com', 'youtu.be')

    def _extract_video_id(self, url):
        """Extract video ID from a YouTube URL.

        Handles /watch?v=ID, youtu.be/ID, /shorts/ID, /embed/ID
        """
        try:
            parsed = urlparse(url)
            hostname = parsed.hostname or ''

            if hostname == 'youtu.be':
                return parsed.path.strip('/')

            path = parsed.path
            if path.startswith('/watch'):
                from urllib.parse import parse_qs
                params = parse_qs(parsed.query)
                ids = params.get('v', [])
                return ids[0] if ids else None
            for prefix in ('/shorts/', '/embed/', '/v/'):
                if path.startswith(prefix):
                    return path[len(prefix):].strip('/').split('/')[0]

            return None
        except Exception:
            return None

    def fetch_youtube_content(self, url, retries=3):
        """Fetch YouTube video transcript and metadata.

        Returns dict on success: {title, author, date, description, content}
        Returns error dict on failure: {error: True, reason, detail, html}
        """
        video_id = self._extract_video_id(url)
        if not video_id:
            return {
                'error': True,
                'reason': 'parse_error',
                'detail': f'Could not extract video ID from {url}',
                'html': '',
            }

        # Fetch metadata via oEmbed (no API key needed)
        title = None
        author = None
        try:
            oembed_resp = requests.get(
                'https://www.youtube.com/oembed',
                params={'url': url, 'format': 'json'},
                timeout=15,
            )
            if oembed_resp.status_code == 200:
                oembed = oembed_resp.json()
                title = oembed.get('title')
                author = oembed.get('author_name')
        except Exception:
            pass

        # Fetch transcript
        last_error = None
        for attempt in range(retries):
            try:
                ytt = YouTubeTranscriptApi()
                try:
                    transcript = ytt.fetch(video_id, languages=['en', 'en-US', 'en-GB'])
                except (NoTranscriptFound,):
                    # Fallback: get first available transcript
                    transcript_list = ytt.list(video_id)
                    first = next(iter(transcript_list))
                    transcript = ytt.fetch(video_id, languages=[first.language_code])

                # Format transcript as plain text paragraphs
                snippets = [snippet.text for snippet in transcript.snippets]
                transcript_text = '\n\n'.join(snippets)

                # Build content
                content_parts = [
                    f'**Video:** [{title or "YouTube Video"}]({url})',
                ]
                if author:
                    content_parts.append(f'**Channel:** {author}')
                content_parts.append('')
                content_parts.append('## Transcript')
                content_parts.append('')
                content_parts.append(transcript_text)

                content = '\n'.join(content_parts)

                return {
                    'title': title or f'YouTube - {video_id}',
                    'author': author,
                    'date': None,
                    'description': f'Transcript of YouTube video {video_id}',
                    'content': content,
                }

            except (TranscriptsDisabled, VideoUnavailable) as e:
                # Non-retryable
                return {
                    'error': True,
                    'reason': 'no_content',
                    'detail': f'{type(e).__name__}: {e}',
                    'html': '',
                }

            except Exception as e:
                last_error = {
                    'error': True,
                    'reason': 'parse_error',
                    'detail': f'{type(e).__name__}: {e}',
                    'html': '',
                }
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    return last_error

        return last_error or {
            'error': True, 'reason': 'unknown', 'detail': 'All retries exhausted', 'html': ''
        }

    # -------------------------------------------------------------------------
    # Firebase Dynamic Link resolution
    # -------------------------------------------------------------------------

    FIREBASE_DYNAMIC_LINK_DOMAINS = {'search.app', 'share.google'}

    def _resolve_firebase_dynamic_link(self, url):
        """Resolve a Firebase Dynamic Link (search.app, share.google) to its destination URL.

        Firebase serves a 302 redirect when the request looks like curl (no browser
        fingerprinting headers). Using urllib.request with a curl User-Agent reliably
        follows the redirect chain to the real article URL.

        Returns the resolved destination URL, or the original URL on failure.
        """
        import urllib.request as _urllib_request
        try:
            parsed = urlparse(url)
            if parsed.hostname not in self.FIREBASE_DYNAMIC_LINK_DOMAINS:
                return url

            req = _urllib_request.Request(url, headers={'User-Agent': 'curl/7.88.1'})
            resp = _urllib_request.urlopen(req, timeout=20)
            final_url = resp.url

            parsed_final = urlparse(final_url)
            if parsed_final.hostname and parsed_final.hostname not in self.FIREBASE_DYNAMIC_LINK_DOMAINS:
                return final_url

            return url
        except Exception:
            return url

    # -------------------------------------------------------------------------
    # Page scraping
    # -------------------------------------------------------------------------

    def fetch_page_content(self, url, retries=3):
        """Fetch a web page and extract article content as markdown.

        Returns dict on success: {title, author, date, description, content}
        Returns error dict on failure: {error: True, reason, detail, html}
        """
        # Resolve Firebase Dynamic Links (search.app, share.google) before scraping
        resolved_url = self._resolve_firebase_dynamic_link(url)
        if resolved_url != url:
            return self.fetch_page_content(resolved_url, retries=retries)

        last_error = None
        raw_html = ''

        for attempt in range(retries):
            try:
                response = requests.get(
                    url,
                    headers={
                        'User-Agent': (
                            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                            'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                        ),
                    },
                    timeout=30,
                    allow_redirects=True,
                )
                raw_html = response.text[:5000]
                response.raise_for_status()

                soup = BeautifulSoup(response.text, 'html.parser')

                # Extract metadata from <meta> tags
                title = self._extract_meta(soup, 'og:title') or self._extract_title(soup)
                author = self._extract_meta(soup, 'author') or self._extract_meta(soup, 'article:author')
                date = self._extract_meta(soup, 'article:published_time') or self._extract_meta(soup, 'date')
                description = self._extract_meta(soup, 'og:description') or self._extract_meta(soup, 'description')

                # Parse date to YYYY-MM-DD
                if date:
                    match = re.match(r'(\d{4}-\d{2}-\d{2})', date)
                    date = match.group(1) if match else None

                # Extract article content
                article = (
                    soup.find('article') or
                    soup.find('main') or
                    soup.find('div', class_=re.compile(r'post|article|content|entry', re.I)) or
                    soup.find('div', role='main')
                )

                if not article:
                    article = soup.find('body')
                    if article:
                        for tag in article.find_all(['nav', 'footer', 'header', 'aside', 'script', 'style']):
                            tag.decompose()

                if not article:
                    return {
                        'error': True,
                        'reason': 'no_content',
                        'detail': f'No article/main/body element found at {url}',
                        'html': raw_html,
                    }

                content = self._html_to_markdown(str(article))

                if not content or len(content) < 100:
                    return {
                        'error': True,
                        'reason': 'no_content',
                        'detail': f'Article extraction yielded {len(content) if content else 0} chars (min 100)',
                        'html': raw_html,
                    }

                return {
                    'title': title,
                    'author': author,
                    'date': date,
                    'description': description,
                    'content': content,
                }

            except requests.exceptions.HTTPError as e:
                status_code = e.response.status_code if e.response is not None else 0
                last_error = {
                    'error': True,
                    'reason': f'http_{status_code}',
                    'detail': str(e),
                    'html': raw_html,
                }
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    return last_error

            except requests.exceptions.Timeout as e:
                last_error = {
                    'error': True,
                    'reason': 'timeout',
                    'detail': f'Request timed out for {url}: {e}',
                    'html': '',
                }
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    return last_error

            except requests.exceptions.ConnectionError as e:
                last_error = {
                    'error': True,
                    'reason': 'connection_error',
                    'detail': f'Connection failed for {url}: {e}',
                    'html': '',
                }
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    return last_error

            except requests.exceptions.RequestException as e:
                last_error = {
                    'error': True,
                    'reason': 'request_error',
                    'detail': str(e),
                    'html': raw_html,
                }
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    return last_error

            except Exception as e:
                last_error = {
                    'error': True,
                    'reason': 'parse_error',
                    'detail': f'{type(e).__name__}: {e}',
                    'html': raw_html,
                }
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    return last_error

        return last_error or {
            'error': True, 'reason': 'unknown', 'detail': 'All retries exhausted', 'html': ''
        }

    def _extract_meta(self, soup, name):
        """Extract content from a <meta> tag by name or property."""
        tag = soup.find('meta', attrs={'property': name})
        if tag and tag.get('content'):
            return tag['content'].strip()
        tag = soup.find('meta', attrs={'name': name})
        if tag and tag.get('content'):
            return tag['content'].strip()
        return None

    def _extract_title(self, soup):
        """Extract title from <title> tag."""
        tag = soup.find('title')
        return tag.get_text(strip=True) if tag else None

    # -------------------------------------------------------------------------
    # HTML to Markdown conversion
    # -------------------------------------------------------------------------

    def _html_to_markdown(self, html):
        """Convert body HTML string to markdown"""
        soup = BeautifulSoup(html, 'html.parser')
        lines = []
        for child in soup.children:
            if hasattr(child, 'name') and child.name:
                self._process_element(child, lines)
            elif child.string and child.string.strip():
                lines.append(child.string.strip())

        text = '\n'.join(lines)
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()

    def _process_element(self, elem, lines, depth=0):
        """Recursively process HTML elements to markdown"""
        if elem.name is None:
            text = elem.string
            if text and text.strip():
                lines.append(text.strip())
            return

        tag = elem.name.lower() if elem.name else ''

        skip_tags = {'script', 'style', 'nav', 'footer', 'header', 'iframe', 'noscript', 'svg'}
        if tag in skip_tags:
            return

        skip_classes = {'share', 'social', 'sidebar', 'related', 'comment', 'newsletter', 'cta', 'navigation'}
        elem_classes = ' '.join(elem.get('class', [])).lower()
        if any(c in elem_classes for c in skip_classes):
            return

        if tag in ('h1', 'h2', 'h3', 'h4', 'h5', 'h6'):
            level = int(tag[1])
            text = elem.get_text(strip=True)
            if text:
                lines.append('')
                lines.append(f"{'#' * level} {text}")
                lines.append('')

        elif tag == 'p':
            text = self._inline_to_markdown(elem)
            if text.strip():
                lines.append('')
                lines.append(text)

        elif tag == 'blockquote':
            text = elem.get_text(strip=True)
            if text:
                lines.append('')
                for line in text.split('\n'):
                    line = line.strip()
                    if line:
                        lines.append(f"> {line}")
                lines.append('')

        elif tag in ('ul', 'ol'):
            lines.append('')
            for i, li in enumerate(elem.find_all('li', recursive=False)):
                text = self._inline_to_markdown(li)
                if text.strip():
                    prefix = f"{i+1}." if tag == 'ol' else '-'
                    lines.append(f"{prefix} {text.strip()}")
            lines.append('')

        elif tag == 'pre':
            code = elem.find('code')
            if code:
                lang = ''
                for cls in code.get('class', []):
                    if cls.startswith('language-'):
                        lang = cls.replace('language-', '')
                        break
                lines.append('')
                lines.append(f'```{lang}')
                lines.append(code.get_text())
                lines.append('```')
                lines.append('')
            else:
                lines.append('')
                lines.append('```')
                lines.append(elem.get_text())
                lines.append('```')
                lines.append('')

        elif tag == 'img':
            src = elem.get('src', '')
            alt = elem.get('alt', '')
            if src:
                lines.append(f'![{alt}]({src})')

        elif tag == 'figure':
            img = elem.find('img')
            if img:
                src = img.get('src', '')
                alt = img.get('alt', '')
                if src:
                    lines.append('')
                    lines.append(f'![{alt}]({src})')
                caption = elem.find('figcaption')
                if caption:
                    lines.append(f'*{caption.get_text(strip=True)}*')
                lines.append('')
            else:
                for child in elem.children:
                    if hasattr(child, 'name') and child.name:
                        self._process_element(child, lines, depth + 1)

        elif tag == 'table':
            lines.append('')
            self._table_to_markdown(elem, lines)
            lines.append('')

        elif tag == 'hr':
            lines.append('')
            lines.append('---')
            lines.append('')

        elif tag in ('div', 'section', 'article', 'main', 'span', 'a'):
            for child in elem.children:
                if hasattr(child, 'name') and child.name:
                    self._process_element(child, lines, depth + 1)
                elif child.string and child.string.strip():
                    lines.append(child.string.strip())

        else:
            for child in elem.children:
                if hasattr(child, 'name') and child.name:
                    self._process_element(child, lines, depth + 1)
                elif child.string and child.string.strip():
                    lines.append(child.string.strip())

    def _inline_to_markdown(self, elem):
        """Convert inline HTML to markdown text"""
        parts = []
        for child in elem.children:
            if child.name is None:
                if child.string:
                    parts.append(child.string)
            elif child.name in ('strong', 'b'):
                text = child.get_text()
                if text.strip():
                    parts.append(f'**{text.strip()}**')
            elif child.name in ('em', 'i'):
                text = child.get_text()
                if text.strip():
                    parts.append(f'*{text.strip()}*')
            elif child.name == 'code':
                text = child.get_text()
                if text.strip():
                    parts.append(f'`{text.strip()}`')
            elif child.name == 'a':
                text = child.get_text(strip=True)
                href = child.get('href', '')
                if text and href:
                    parts.append(f'[{text}]({href})')
                elif text:
                    parts.append(text)
            elif child.name == 'br':
                parts.append('\n')
            elif child.name == 'img':
                src = child.get('src', '')
                alt = child.get('alt', '')
                if src:
                    parts.append(f'![{alt}]({src})')
            else:
                text = child.get_text()
                if text:
                    parts.append(text)
        return ''.join(parts)

    def _table_to_markdown(self, table, lines):
        """Convert HTML table to markdown table"""
        rows = []
        for tr in table.find_all('tr'):
            cells = [td.get_text(strip=True).replace('|', '\\|')
                     for td in tr.find_all(['th', 'td'])]
            if cells:
                rows.append(cells)

        if not rows:
            return

        max_cols = max(len(r) for r in rows)
        for row in rows:
            while len(row) < max_cols:
                row.append('')

        lines.append('| ' + ' | '.join(rows[0]) + ' |')
        lines.append('| ' + ' | '.join(['---'] * max_cols) + ' |')
        for row in rows[1:]:
            lines.append('| ' + ' | '.join(row) + ' |')


# =============================================================================
# Raindrop Manager (Orchestrator)
# =============================================================================

class RaindropManager:
    """Orchestrates Raindrop bookmark scraping operations"""

    def __init__(self, config=None):
        self.config = config or Config()
        self.config.load()
        self.db = RaindropDatabase(self.config.database_path)
        self._shutdown = False

    def _get_scraper(self):
        """Create scraper instance (requires token)"""
        token = self.config.test_token
        if not token:
            print("Error: Raindrop API token not configured")
            print("Run: scrape_raindrop.py config set test_token YOUR_TOKEN")
            print()
            print("To get a test token:")
            print("1. Go to https://app.raindrop.io/settings/integrations")
            print("2. Click 'Create new app'")
            print("3. Copy the 'Test token'")
            return None
        return RaindropScraper(token, self.config.medium_domains)

    def _setup_signal_handlers(self):
        """Setup graceful shutdown on Ctrl+C"""
        def handler(signum, frame):
            if self._shutdown:
                print("\nForce quit.")
                sys.exit(1)
            print("\nShutting down gracefully (press Ctrl+C again to force)...")
            self._shutdown = True
        signal.signal(signal.SIGINT, handler)

    def discover(self, route_medium=None):
        """Fetch bookmarks from Raindrop.io API and add to database.

        Medium URLs are optionally routed to the Medium scraper's database.
        """
        scraper = self._get_scraper()
        if not scraper:
            return

        if route_medium is None:
            route_medium = self.config.route_medium

        print("Fetching bookmarks from Raindrop.io...")
        items = scraper.fetch_all_bookmarks()

        if not items:
            print("No bookmarks found.")
            return

        print(f"Found {len(items)} bookmarks in Raindrop.io")

        new_count = 0
        routed_count = 0

        with self.db:
            for item in items:
                raindrop_id = item.get('_id')
                url = item.get('link', '')
                title = item.get('title', '')
                domain = item.get('domain', '')
                excerpt = item.get('excerpt', '')
                note = item.get('note', '')
                tags = item.get('tags', [])
                bookmark_type = item.get('type', '')
                cover_url = item.get('cover', '')

                created = item.get('created')
                updated = item.get('lastUpdate')

                if not raindrop_id or not url:
                    continue

                # Check if Medium URL and should be routed
                if route_medium and scraper.is_medium_url(url):
                    added = self.db.add_bookmark(
                        raindrop_id=raindrop_id, url=url, title=title,
                        domain=domain, excerpt=excerpt, note=note,
                        tags=tags, bookmark_type=bookmark_type,
                        raindrop_created=created, raindrop_updated=updated,
                        cover_url=cover_url,
                    )
                    if added:
                        self.db.update_bookmark(raindrop_id, status='skipped_medium', routed_to='medium')
                        self._route_to_medium(url, title, scraper)
                        routed_count += 1
                        new_count += 1
                    continue

                added = self.db.add_bookmark(
                    raindrop_id=raindrop_id, url=url, title=title,
                    domain=domain, excerpt=excerpt, note=note,
                    tags=tags, bookmark_type=bookmark_type,
                    raindrop_created=created, raindrop_updated=updated,
                    cover_url=cover_url,
                )
                if added:
                    new_count += 1

            counts = self.db.get_counts()
            self.db.add_sync_log(
                len(items), new_count, 0, 0, routed_count, 'discovered'
            )

        print(f"New bookmarks added: {new_count}")
        if routed_count:
            print(f"  Routed to Medium scraper: {routed_count}")
        print(f"Total in database: {counts.get('total', 0)}")
        print(f"  Pending: {counts.get('pending', 0)}")
        print(f"  Downloaded: {counts.get('downloaded', 0)}")
        print(f"  Failed: {counts.get('failed', 0)}")
        print(f"  Skipped (Medium): {counts.get('skipped_medium', 0)}")

    def _route_to_medium(self, url, title, scraper):
        """Insert a Medium URL into the medium_posts database."""
        medium_db_path = self.config.get_medium_database()
        slug = scraper._extract_medium_slug(url)
        if not slug:
            return

        try:
            parsed = urlparse(url)
            hostname = parsed.hostname or ''
            # Extract username from path for medium.com URLs
            path_parts = parsed.path.strip('/').split('/')
            if hostname == 'medium.com' and path_parts and path_parts[0].startswith('@'):
                username = path_parts[0].lstrip('@')
            else:
                username = '_raindrop_import'

            conn = duckdb.connect(medium_db_path)
            # Ensure table exists
            conn.execute("""
                CREATE TABLE IF NOT EXISTS medium_posts (
                    id INTEGER PRIMARY KEY,
                    slug VARCHAR NOT NULL UNIQUE,
                    username VARCHAR NOT NULL,
                    url VARCHAR NOT NULL,
                    title VARCHAR,
                    author VARCHAR,
                    description VARCHAR,
                    publish_date DATE,
                    updated_date DATE,
                    categories VARCHAR,
                    content_html TEXT,
                    word_count INTEGER,
                    char_count INTEGER,
                    file_path VARCHAR,
                    status VARCHAR DEFAULT 'pending',
                    downloaded_at TIMESTAMP,
                    in_obsidian BOOLEAN DEFAULT FALSE,
                    moved_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute("""
                CREATE SEQUENCE IF NOT EXISTS medium_posts_id_seq
            """)
            conn.execute("""
                INSERT INTO medium_posts (id, slug, username, url, title)
                VALUES (nextval('medium_posts_id_seq'), ?, ?, ?, ?)
            """, [slug, username, url, title])
            conn.close()
        except duckdb.ConstraintException:
            # Duplicate slug — already in Medium DB
            try:
                conn.close()
            except Exception:
                pass
        except Exception as e:
            print(f"  Warning: Could not route to Medium DB: {e}")
            try:
                conn.close()
            except Exception:
                pass

    def scrape(self, bookmark_id=None, limit=None, parallel=False):
        """Scrape web pages for pending bookmarks"""
        self._setup_signal_handlers()

        scraper = self._get_scraper()
        if not scraper:
            return

        with self.db:
            if bookmark_id:
                bookmark = self.db.get_bookmark(bookmark_id)
                if not bookmark:
                    print(f"Error: Bookmark with raindrop_id '{bookmark_id}' not found")
                    return
                bookmarks = [bookmark]
            else:
                bookmarks = self.db.get_bookmarks(status='pending', limit=limit)

        if not bookmarks:
            print("No pending bookmarks to scrape.")
            return

        if parallel and not bookmark_id:
            self._scrape_parallel(bookmarks, scraper)
        else:
            self._scrape_sequential(bookmarks, scraper)

    def _scrape_sequential(self, bookmarks, scraper):
        """Scrape bookmarks one at a time"""
        print(f"Scraping {len(bookmarks)} bookmarks (sequential)")
        print(f"Delay between requests: {self.config.delay}s")
        print()

        successful = 0
        failed = 0

        for i, bm in enumerate(bookmarks):
            if self._shutdown:
                print("\nStopping due to shutdown request.")
                break

            title_display = (bm.get('title') or bm['url'])[:60]
            print(f"[{i+1}/{len(bookmarks)}] {title_display}...", end=" ", flush=True)

            if HAS_YOUTUBE_TRANSCRIPT and scraper.is_youtube_url(bm['url']):
                result = scraper.fetch_youtube_content(
                    bm['url'], retries=self.config.max_retries
                )
            else:
                result = scraper.fetch_page_content(
                    bm['url'], retries=self.config.max_retries
                )

            if result and not result.get('error') and result.get('content') and len(result['content']) >= 100:
                filepath = self._save_bookmark(bm, result)

                word_count = len(result['content'].split())
                char_count = len(result['content'])

                with self.db:
                    self.db.update_bookmark(
                        bm['raindrop_id'],
                        title=result['title'] or bm.get('title'),
                        author=result.get('author'),
                        word_count=word_count,
                        char_count=char_count,
                        file_path=filepath,
                        status='downloaded',
                        downloaded_at=datetime.now(),
                    )

                print(f"OK ({word_count} words)")
                successful += 1
            else:
                error_reason = result.get('reason', 'unknown') if result else 'unknown'
                error_detail = result.get('detail', '') if result else ''
                error_html = result.get('html', '') if result else ''
                with self.db:
                    self.db.update_bookmark(
                        bm['raindrop_id'],
                        status='failed',
                        error_reason=error_reason,
                        error_detail=error_detail,
                        error_html=error_html,
                    )
                print(f"FAILED ({error_reason})")
                failed += 1

            if i < len(bookmarks) - 1 and not self._shutdown:
                time.sleep(self.config.delay)

        with self.db:
            status = 'success' if failed == 0 else 'partial'
            self.db.add_sync_log(0, 0, successful, failed, 0, status)

        print(f"\nComplete: {successful} downloaded, {failed} failed")

    def _scrape_parallel(self, bookmarks, scraper):
        """Scrape bookmarks in parallel using ThreadPoolExecutor"""
        max_workers = self.config.max_workers
        delay = self.config.delay

        print(f"Scraping {len(bookmarks)} bookmarks (parallel, {max_workers} workers)")
        print(f"Delay between requests: {delay}s per worker")
        print()

        successful = 0
        failed = 0
        counter_lock = threading.Lock()
        progress_lock = threading.Lock()
        completed = 0

        def _fetch_bookmark(bm):
            nonlocal completed

            if self._shutdown:
                return None

            if HAS_YOUTUBE_TRANSCRIPT and scraper.is_youtube_url(bm['url']):
                result = scraper.fetch_youtube_content(
                    bm['url'], retries=self.config.max_retries
                )
            else:
                result = scraper.fetch_page_content(
                    bm['url'], retries=self.config.max_retries
                )

            with counter_lock:
                completed += 1
                idx = completed

            title_display = (bm.get('title') or bm['url'])[:60]

            if result and not result.get('error') and result.get('content') and len(result['content']) >= 100:
                filepath = self._save_bookmark(bm, result)
                word_count = len(result['content'].split())
                char_count = len(result['content'])

                with progress_lock:
                    print(f"[{idx}/{len(bookmarks)}] {title_display}... OK ({word_count} words)")

                time.sleep(delay)
                return {
                    'raindrop_id': bm['raindrop_id'],
                    'success': True,
                    'result': result,
                    'title': bm.get('title'),
                    'filepath': filepath,
                    'word_count': word_count,
                    'char_count': char_count,
                }
            else:
                error_reason = result.get('reason', 'unknown') if result else 'unknown'
                with progress_lock:
                    print(f"[{idx}/{len(bookmarks)}] {title_display}... FAILED ({error_reason})")

                time.sleep(delay)
                return {
                    'raindrop_id': bm['raindrop_id'],
                    'success': False,
                    'error_reason': error_reason,
                    'error_detail': result.get('detail', '') if result else '',
                    'error_html': result.get('html', '') if result else '',
                }

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            for bm in bookmarks:
                if self._shutdown:
                    break
                future = executor.submit(_fetch_bookmark, bm)
                futures[future] = bm

            for future in as_completed(futures):
                if self._shutdown:
                    executor.shutdown(wait=False, cancel_futures=True)
                    break

                outcome = future.result()
                if outcome is None:
                    continue

                if outcome['success']:
                    r = outcome['result']
                    with self.db:
                        self.db.update_bookmark(
                            outcome['raindrop_id'],
                            title=r['title'] or outcome.get('title'),
                            author=r.get('author'),
                            word_count=outcome['word_count'],
                            char_count=outcome['char_count'],
                            file_path=outcome['filepath'],
                            status='downloaded',
                            downloaded_at=datetime.now(),
                        )
                    successful += 1
                else:
                    with self.db:
                        self.db.update_bookmark(
                            outcome['raindrop_id'],
                            status='failed',
                            error_reason=outcome.get('error_reason', 'unknown'),
                            error_detail=outcome.get('error_detail', ''),
                            error_html=outcome.get('error_html', ''),
                        )
                    failed += 1

        with self.db:
            status = 'success' if failed == 0 else 'partial'
            self.db.add_sync_log(0, 0, successful, failed, 0, status)

        print(f"\nComplete: {successful} downloaded, {failed} failed")

    def _is_youtube_bookmark(self, bookmark):
        """Check if a bookmark URL is a YouTube video."""
        url = bookmark.get('url', '')
        try:
            parsed = urlparse(url)
            hostname = parsed.hostname or ''
        except Exception:
            return False
        return hostname in ('youtube.com', 'www.youtube.com', 'm.youtube.com', 'youtu.be')

    def _save_bookmark(self, bookmark, result):
        """Save a bookmark as Obsidian markdown with merged tags."""
        is_youtube = self._is_youtube_bookmark(bookmark)
        subfolder = 'YouTube' if is_youtube else 'Raindrop'
        output_dir = Path(self.config.staging_dir) / subfolder
        output_dir.mkdir(parents=True, exist_ok=True)

        title = result.get('title') or bookmark.get('title') or 'Untitled'
        safe_title = re.sub(r'[<>:"/\\|?*]', '', title)
        safe_title = re.sub(r'\s+', ' ', safe_title).strip()
        if len(safe_title) > 200:
            safe_title = safe_title[:200]
        filepath = output_dir / f"{safe_title}.md"

        yaml_title = title.replace('"', '\\"')
        url = bookmark.get('url', '')
        date = result.get('date') or ''
        today = datetime.now().strftime('%Y-%m-%d')
        description = (result.get('description') or bookmark.get('excerpt') or '').replace('"', '\\"')
        author = result.get('author') or ''

        # Merge tags: base tags + Raindrop user tags
        base_tags = ['clippings', 'raindrop']
        if is_youtube:
            base_tags.append('youtube')
        user_tags_raw = bookmark.get('tags')
        user_tags = []
        if user_tags_raw:
            if isinstance(user_tags_raw, str):
                try:
                    user_tags = json.loads(user_tags_raw)
                except (json.JSONDecodeError, TypeError):
                    user_tags = []
            elif isinstance(user_tags_raw, list):
                user_tags = user_tags_raw

        all_tags = base_tags + [t for t in user_tags if t not in base_tags]

        # Build frontmatter
        fm = []
        fm.append('---')
        fm.append(f'title: "{yaml_title}"')
        fm.append(f'source: "{url}"')
        if author:
            fm.append('author:')
            fm.append(f'  - "[[{author}]]"')
        if date:
            fm.append(f'published: {date}')
        fm.append(f'created: {today}')
        if description:
            fm.append(f'description: "{description}"')
        fm.append('tags:')
        for tag in all_tags:
            fm.append(f'  - "{tag}"')
        fm.append('---')

        body_parts = ['\n'.join(fm)]
        body_parts.append('')
        body_parts.append(result['content'])
        body_parts.append('')

        with open(filepath, 'w', encoding='utf-8') as f:
            f.write('\n'.join(body_parts))

        return str(filepath)

    def list_bookmarks(self, status=None, json_output=False):
        """List bookmarks"""
        with self.db:
            bookmarks = self.db.get_bookmarks(status=status)

        if json_output:
            print(json.dumps(bookmarks, indent=2, default=str))
            return bookmarks

        if not bookmarks:
            print(f"No bookmarks found{f' with status={status}' if status else ''}.")
            return []

        print(f"\n{'ID':<6} {'Status':<16} {'Domain':<30} {'Title'}")
        print('-' * 100)
        for b in bookmarks:
            title = (b.get('title') or '')[:40]
            domain = (b.get('domain') or '')[:29]
            print(f"{b['id']:<6} {b['status']:<16} {domain:<30} {title}")
        print(f"\nTotal: {len(bookmarks)}")
        return bookmarks

    def show_status(self):
        """Show summary statistics"""
        with self.db:
            counts = self.db.get_counts()

        total = counts.get('total', 0)
        pending = counts.get('pending', 0)
        downloaded = counts.get('downloaded', 0)
        failed = counts.get('failed', 0)
        skipped = counts.get('skipped_medium', 0)

        print(f"\nRaindrop Bookmark Scraper Status")
        print(f"{'='*40}")
        print(f"Total bookmarks:  {total}")
        print(f"  Pending:        {pending}")
        print(f"  Downloaded:     {downloaded}")
        print(f"  Failed:         {failed}")
        print(f"  Skipped (Medium): {skipped}")

        if total > 0:
            pct = (downloaded / total) * 100
            print(f"\nProgress: {pct:.1f}%")

        print()

    def move_to_obsidian(self, all_bookmarks=False):
        """Move downloaded bookmarks to Obsidian vault"""
        obsidian_path = self.config.obsidian_vault
        if not obsidian_path:
            print("Error: Obsidian vault path not configured")
            print("Run: scrape_raindrop.py config set obsidian_vault /path/to/vault")
            return

        obsidian_path = Path(obsidian_path).expanduser()
        if not obsidian_path.exists():
            print(f"Error: Obsidian vault path does not exist: {obsidian_path}")
            return

        with self.db:
            bookmarks = self.db.get_unmoved_bookmarks()

        if not bookmarks:
            print("No bookmarks to move (all already in Obsidian or none downloaded).")
            return

        moved_ids = []
        for bm in bookmarks:
            src_file = Path(bm['file_path'])
            if src_file.exists():
                # Route YouTube files to YouTube subfolder, others to Raindrop
                if '/YouTube/' in bm['file_path']:
                    dst_dir = obsidian_path / 'YouTube'
                else:
                    dst_dir = obsidian_path / 'Raindrop'
                dst_dir.mkdir(parents=True, exist_ok=True)
                dst_file = dst_dir / src_file.name
                shutil.copy2(src_file, dst_file)
                moved_ids.append(bm['raindrop_id'])

        with self.db:
            self.db.mark_moved(moved_ids)

        print(f"Moved {len(moved_ids)} bookmarks to: {obsidian_path}")

    def retry_failed(self):
        """Reset failed bookmarks to pending"""
        with self.db:
            count = self.db.reset_failed()
        print(f"Reset failed bookmarks. Pending count: {count}")

    def show_fix_report(self, raindrop_id=None, limit=None, json_output=False, auto=False):
        """Show detailed report of failed bookmarks for LLM consumption"""
        if auto:
            ollama_url = self.config.get('ollama_url', '')
            if not ollama_url:
                print("Auto-fix via Ollama not yet configured.")
                print("Set ollama_url in config to enable: config set ollama_url http://localhost:11434")
                return
            print("Auto-fix via Ollama is not yet implemented.")
            return

        with self.db:
            failed = self.db.get_failed_bookmarks(raindrop_id=raindrop_id, limit=limit)

        if json_output:
            print(json.dumps(failed, indent=2, default=str))
            return

        if not failed:
            print("No failed bookmarks.")
            return

        print(f"Failed Bookmarks Report ({len(failed)} bookmarks)")
        print("=" * 50)
        print()

        for i, bm in enumerate(failed, 1):
            title = bm.get('title') or '(untitled)'
            print(f"#{i}: {title}")
            print(f"    URL: {bm['url']}")
            print(f"    Reason: {bm.get('error_reason') or 'unknown'}")
            if bm.get('error_detail'):
                print(f"    Detail: {bm['error_detail']}")
            if bm.get('error_html'):
                preview = bm['error_html'][:200]
                print(f"    HTML preview: {preview}")
            print("    ---")
            print()

    def show_config(self):
        """Display current configuration"""
        print(f"Config file: {self.config.config_file}")
        print(f"Local config: {self.config.local_config_file}")
        print()
        print(f"test_token:      {'(set)' if self.config.test_token else '(not set)'}")
        print(f"staging_dir:     {self.config.staging_dir}")
        print(f"obsidian_vault:  {self.config.obsidian_vault or '(not set)'}")
        print(f"database:        {self.config.database_path}")
        print(f"delay:           {self.config.delay}")
        print(f"max_retries:     {self.config.max_retries}")
        print(f"max_workers:     {self.config.max_workers}")
        print(f"route_medium:    {self.config.route_medium}")
        print(f"medium_domains:  {self.config.medium_domains or '(defaults only)'}")
        print(f"ollama_url:      {self.config.get('ollama_url') or '(not set)'}")
        print(f"ollama_model:    {self.config.get('ollama_model') or '(not set)'}")

    def set_config(self, key, value):
        """Set a configuration value"""
        if value.lower() in ('true', 'false'):
            value = value.lower() == 'true'
        else:
            try:
                if '.' in value:
                    value = float(value)
                else:
                    value = int(value)
            except ValueError:
                pass

        self.config.set(key, value)
        print(f"Set raindrop.{key} = {value}")


# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Raindrop.io Bookmark Scraper - Download bookmarked pages as Obsidian markdown',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Discover bookmarks from Raindrop.io
  %(prog)s discover

  # Discover without Medium routing
  %(prog)s discover --no-route-medium

  # Download all pending bookmarks
  %(prog)s scrape

  # Download a single bookmark by Raindrop ID
  %(prog)s scrape --id 123456

  # Download first 10 pending bookmarks
  %(prog)s scrape --limit 10

  # Download in parallel
  %(prog)s scrape --parallel

  # List bookmarks by status
  %(prog)s list --status downloaded

  # Show statistics
  %(prog)s status

  # Move to Obsidian
  %(prog)s move --all

  # Retry failed bookmarks
  %(prog)s retry

  # Show failure report (for LLM consumption)
  %(prog)s fix
  %(prog)s fix --json
  %(prog)s fix --id 123456
  %(prog)s fix --limit 5

  # Configuration
  %(prog)s config show
  %(prog)s config set test_token "YOUR_TOKEN"
  %(prog)s config set obsidian_vault "/path/to/vault"
        """
    )

    subparsers = parser.add_subparsers(dest='command', help='Commands')

    # Discover
    discover_parser = subparsers.add_parser('discover', help='Fetch bookmarks from Raindrop.io API')
    discover_parser.add_argument('--no-route-medium', action='store_true',
                                 help='Do not route Medium URLs to Medium scraper')

    # Scrape
    scrape_parser = subparsers.add_parser('scrape', help='Download pending bookmarks')
    scrape_parser.add_argument('--id', type=int, help='Download a specific bookmark by Raindrop ID')
    scrape_parser.add_argument('--limit', type=int, help='Limit number of bookmarks to download')
    scrape_parser.add_argument('--parallel', action='store_true',
                               help='Download in parallel (default 4 workers)')
    scrape_parser.add_argument('--sequential', action='store_true',
                               help='Download sequentially (default)')
    scrape_parser.add_argument('--workers', type=int,
                               help='Number of parallel workers (implies --parallel)')

    # List
    list_parser = subparsers.add_parser('list', help='List bookmarks')
    list_parser.add_argument('--status',
                             choices=['pending', 'downloaded', 'failed', 'skipped_medium'],
                             help='Filter by status')
    list_parser.add_argument('--json', action='store_true', help='Output as JSON')

    # Status
    subparsers.add_parser('status', help='Show summary statistics')

    # Move
    move_parser = subparsers.add_parser('move', help='Move downloaded bookmarks to Obsidian vault')
    move_parser.add_argument('--all', action='store_true', required=True,
                             help='Move all downloaded bookmarks')

    # Retry
    subparsers.add_parser('retry', help='Reset failed bookmarks to pending')

    # Fix
    fix_parser = subparsers.add_parser('fix', help='Show detailed failure report for LLM consumption')
    fix_parser.add_argument('--id', type=int, help='Show details for a single bookmark by Raindrop ID')
    fix_parser.add_argument('--limit', type=int, help='Limit number of failed bookmarks to show')
    fix_parser.add_argument('--json', action='store_true', help='Output as JSON')
    fix_parser.add_argument('--auto', action='store_true', help='Auto-fix via Ollama (requires ollama_url config)')

    # Config
    config_parser = subparsers.add_parser('config', help='View/set configuration')
    config_parser.add_argument('action', choices=['show', 'set'], help='Action')
    config_parser.add_argument('key', nargs='?', help='Config key')
    config_parser.add_argument('value', nargs='?', help='Value to set')

    args = parser.parse_args()

    config = Config()
    config.load()
    manager = RaindropManager(config)

    if args.command == 'discover':
        route_medium = not args.no_route_medium
        manager.discover(route_medium=route_medium)

    elif args.command == 'scrape':
        parallel = args.parallel or bool(args.workers)
        if args.sequential:
            parallel = False
        if args.workers:
            config.set('max_workers', args.workers)
            config.load()
        manager.scrape(bookmark_id=args.id, limit=args.limit, parallel=parallel)

    elif args.command == 'list':
        manager.list_bookmarks(status=args.status, json_output=args.json)

    elif args.command == 'status':
        manager.show_status()

    elif args.command == 'move':
        manager.move_to_obsidian(all_bookmarks=args.all)

    elif args.command == 'retry':
        manager.retry_failed()

    elif args.command == 'fix':
        manager.show_fix_report(
            raindrop_id=args.id, limit=args.limit,
            json_output=args.json, auto=args.auto,
        )

    elif args.command == 'config':
        if args.action == 'show':
            manager.show_config()
        elif args.action == 'set':
            if not args.key or args.value is None:
                print("Usage: config set <key> <value>")
                print("Example: config set test_token YOUR_TOKEN")
                return
            manager.set_config(args.key, args.value)

    else:
        parser.print_help()


if __name__ == '__main__':
    main()
