#!/usr/bin/env python3
"""
Medium Blog Scraper - Scrapes Medium blog posts to Obsidian markdown.

Fetches blog post URLs and content from Medium RSS feeds (no browser required).
Saves as Obsidian markdown with tags: clippings, medium.

Commands:
    add-user    Add a Medium username to track
    remove-user Remove a tracked Medium username
    discover    Fetch RSS feed and add new posts to database
    scrape      Convert pending posts to markdown files
    list        List blog posts by status
    status      Show summary statistics
    move        Move downloaded posts to Obsidian vault
    retry       Reset failed posts to pending
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
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import urlparse
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import duckdb
import requests
from bs4 import BeautifulSoup


# =============================================================================
# Configuration
# =============================================================================

class Config:
    """Configuration manager for Medium scraper (reads [medium] section from TOML)"""

    DEFAULT_CONFIG = {
        'medium': {
            'staging_dir': 'medium_obsidian',
            'obsidian_vault': '',
            'database': 'medium.db',
            'delay': 1.0,
            'max_retries': 3,
            'max_workers': 4,
            'users': [],
            'sid': '',
            'uid': '',
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
        """Get a config value from [medium] section"""
        if self._config is None:
            self.load()
        return self._config.get('medium', {}).get(key, default)

    def set(self, key, value):
        """Set a config value in local config [medium] section"""
        local_config = {}
        if self.local_config_file.exists():
            with open(self.local_config_file, 'rb') as f:
                local_config = tomllib.load(f)

        if 'medium' not in local_config:
            local_config['medium'] = {}
        local_config['medium'][key] = value

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

    @property
    def staging_dir(self):
        return self.get('staging_dir', 'medium_obsidian')

    @property
    def obsidian_vault(self):
        return self.get('obsidian_vault', '')

    @property
    def database_path(self):
        return self.get('database', 'medium.db')

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
    def users(self):
        return self.get('users', [])

    @property
    def sid(self):
        return self.get('sid', '')

    @property
    def uid(self):
        return self.get('uid', '')


# =============================================================================
# Database
# =============================================================================

class MediumDatabase:
    """DuckDB database manager for tracking Medium blog posts"""

    SCHEMA = """
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
    );

    CREATE TABLE IF NOT EXISTS medium_sync_logs (
        id INTEGER PRIMARY KEY,
        synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        username VARCHAR,
        total_in_feed INTEGER,
        new_posts_found INTEGER,
        posts_downloaded INTEGER,
        posts_failed INTEGER,
        status VARCHAR
    );

    CREATE SEQUENCE IF NOT EXISTS medium_posts_id_seq;
    CREATE SEQUENCE IF NOT EXISTS medium_sync_logs_id_seq;
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

    def add_post(self, slug, username, url, title=None, author=None,
                 description=None, publish_date=None, updated_date=None,
                 categories=None, content_html=None):
        """Add a new Medium post (pending status)"""
        try:
            self.conn.execute("""
                INSERT INTO medium_posts (id, slug, username, url, title, author,
                    description, publish_date, updated_date, categories, content_html)
                VALUES (nextval('medium_posts_id_seq'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [slug, username, url, title, author, description,
                  publish_date, updated_date, categories, content_html])
            return True
        except duckdb.ConstraintException:
            return False

    def get_post(self, slug):
        """Get a single post by slug"""
        result = self.conn.execute(
            "SELECT * FROM medium_posts WHERE slug = ?", [slug]
        ).fetchone()
        if result:
            columns = [
                'id', 'slug', 'username', 'url', 'title', 'author',
                'description', 'publish_date', 'updated_date', 'categories',
                'content_html', 'word_count', 'char_count', 'file_path',
                'status', 'downloaded_at', 'in_obsidian', 'moved_at', 'created_at'
            ]
            return dict(zip(columns, result))
        return None

    def get_posts(self, status=None, username=None, limit=None):
        """Get posts, optionally filtered by status and/or username"""
        query = ("SELECT id, slug, username, url, title, author, publish_date, "
                 "status, file_path, in_obsidian FROM medium_posts")
        params = []
        conditions = []
        if status:
            conditions.append("status = ?")
            params.append(status)
        if username:
            conditions.append("username = ?")
            params.append(username)
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY id"
        if limit:
            query += " LIMIT ?"
            params.append(limit)
        results = self.conn.execute(query, params).fetchall()
        columns = ['id', 'slug', 'username', 'url', 'title', 'author',
                    'publish_date', 'status', 'file_path', 'in_obsidian']
        return [dict(zip(columns, row)) for row in results]

    def update_post(self, slug, **kwargs):
        """Update post fields"""
        valid_fields = [
            'title', 'author', 'description', 'publish_date', 'updated_date',
            'categories', 'content_html', 'word_count', 'char_count',
            'file_path', 'status', 'downloaded_at', 'in_obsidian', 'moved_at'
        ]
        updates = []
        values = []
        for field, value in kwargs.items():
            if field in valid_fields:
                updates.append(f"{field} = ?")
                values.append(value)
        if updates:
            values.append(slug)
            self.conn.execute(
                f"UPDATE medium_posts SET {', '.join(updates)} WHERE slug = ?",
                values
            )

    def get_counts(self, username=None):
        """Get status counts"""
        if username:
            results = self.conn.execute("""
                SELECT status, COUNT(*) FROM medium_posts
                WHERE username = ? GROUP BY status
            """, [username]).fetchall()
            total = self.conn.execute(
                "SELECT COUNT(*) FROM medium_posts WHERE username = ?",
                [username]
            ).fetchone()[0]
        else:
            results = self.conn.execute("""
                SELECT status, COUNT(*) FROM medium_posts GROUP BY status
            """).fetchall()
            total = self.conn.execute(
                "SELECT COUNT(*) FROM medium_posts"
            ).fetchone()[0]
        counts = {row[0]: row[1] for row in results}
        counts['total'] = total
        return counts

    def get_unmoved_posts(self):
        """Get downloaded posts not yet in Obsidian"""
        results = self.conn.execute("""
            SELECT id, slug, username, url, title, author, publish_date, file_path
            FROM medium_posts
            WHERE status = 'downloaded' AND in_obsidian = FALSE AND file_path IS NOT NULL
            ORDER BY id
        """).fetchall()
        columns = ['id', 'slug', 'username', 'url', 'title', 'author',
                    'publish_date', 'file_path']
        return [dict(zip(columns, row)) for row in results]

    def mark_moved(self, slugs):
        """Mark posts as moved to Obsidian"""
        if not slugs:
            return
        placeholders = ','.join(['?'] * len(slugs))
        self.conn.execute(f"""
            UPDATE medium_posts
            SET in_obsidian = TRUE, moved_at = CURRENT_TIMESTAMP
            WHERE slug IN ({placeholders})
        """, list(slugs))

    def reset_failed(self):
        """Reset failed posts to pending"""
        self.conn.execute("""
            UPDATE medium_posts SET status = 'pending' WHERE status = 'failed'
        """)
        return self.conn.execute(
            "SELECT COUNT(*) FROM medium_posts WHERE status = 'pending'"
        ).fetchone()[0]

    def add_sync_log(self, username, total_feed, new_found, downloaded, failed, status):
        """Add a sync log entry"""
        self.conn.execute("""
            INSERT INTO medium_sync_logs (id, username, total_in_feed, new_posts_found,
                                          posts_downloaded, posts_failed, status)
            VALUES (nextval('medium_sync_logs_id_seq'), ?, ?, ?, ?, ?, ?)
        """, [username, total_feed, new_found, downloaded, failed, status])


# =============================================================================
# Medium Scraper
# =============================================================================

class MediumScraper:
    """Handles RSS feed parsing and HTML-to-markdown conversion for Medium posts.

    Medium provides an RSS feed at https://medium.com/feed/@username that
    returns the latest posts with full HTML content in <content:encoded>.
    For small accounts (~10-11 posts), this covers all posts.
    """

    FEED_URL_TEMPLATE = 'https://medium.com/feed/@{username}'

    GRAPHQL_URL = 'https://medium.com/_/graphql'

    GRAPHQL_QUERY = '''
    query PostPage($postId: ID!) {
        post(id: $postId) {
            id
            title
            creator { name }
            content {
                bodyModel {
                    paragraphs {
                        text
                        type
                        name
                        href
                        metadata { id originalWidth originalHeight }
                        markups { type start end href }
                    }
                }
            }
        }
    }
    '''

    def __init__(self, sid=None, uid=None):
        self.sid = sid
        self.uid = uid
        self.authenticated = bool(sid and uid)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': (
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            ),
        })
        if sid:
            self.session.cookies.set('sid', sid, domain='.medium.com')
        if uid:
            self.session.cookies.set('uid', uid, domain='.medium.com')

    # -------------------------------------------------------------------------
    # RSS feed discovery
    # -------------------------------------------------------------------------

    def fetch_feed_posts(self, username):
        """Fetch all posts from a Medium user's RSS feed.

        Returns list of dicts with: slug, url, title, author, description,
        publish_date, updated_date, categories, content_html
        """
        feed_url = self.FEED_URL_TEMPLATE.format(username=username)
        print(f"  Fetching RSS feed: {feed_url}")

        try:
            response = self.session.get(feed_url, timeout=30)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"  Error fetching feed: {e}")
            return []

        soup = BeautifulSoup(response.content, 'xml')
        items = soup.find_all('item')
        print(f"  Found {len(items)} items in feed")

        posts = []
        for item in items:
            post = self._parse_feed_item(item, username)
            if post:
                posts.append(post)

        return posts

    def _parse_feed_item(self, item, username):
        """Parse a single RSS <item> element into a post dict"""
        title = item.find('title')
        title = title.get_text(strip=True) if title else None

        link = item.find('link')
        url = link.get_text(strip=True) if link else None
        if not url:
            return None

        slug = self._url_to_slug(url)
        if not slug:
            return None

        # Author
        creator = item.find('dc:creator')
        author = creator.get_text(strip=True) if creator else None

        # Description
        description_elem = item.find('description')
        description = ''
        if description_elem:
            desc_text = description_elem.get_text(strip=True)
            # Description is often HTML snippet, extract plain text
            desc_soup = BeautifulSoup(desc_text, 'html.parser')
            description = desc_soup.get_text(strip=True)
            # Truncate long descriptions
            if len(description) > 300:
                description = description[:297] + '...'

        # Dates
        pub_date = item.find('pubDate')
        publish_date = self._parse_rss_date(pub_date.get_text(strip=True)) if pub_date else None

        updated = item.find('atom:updated') or item.find('updated')
        updated_date = self._parse_rss_date(updated.get_text(strip=True)) if updated else None

        # Categories/tags
        categories = []
        for cat in item.find_all('category'):
            cat_text = cat.get_text(strip=True)
            if cat_text:
                categories.append(cat_text)
        categories_str = ', '.join(categories) if categories else None

        # Full content HTML
        content_encoded = item.find('content:encoded')
        content_html = content_encoded.get_text() if content_encoded else None

        return {
            'slug': slug,
            'username': username,
            'url': url,
            'title': title,
            'author': author,
            'description': description,
            'publish_date': publish_date,
            'updated_date': updated_date,
            'categories': categories_str,
            'content_html': content_html,
        }

    def _url_to_slug(self, url):
        """Extract slug from Medium URL.

        Handles:
          https://medium.com/@user/some-post-title-abc123def456 -> some-post-title-abc123def456
          https://blog.dataengineerthings.org/some-post-slug-abc123 -> some-post-slug-abc123
          https://medium.com/some-publication/some-post-abc123 -> some-post-abc123
        """
        parsed = urlparse(url)
        path = parsed.path.strip('/')

        # Remove @username prefix if present
        parts = path.split('/')
        # Filter out empty parts and @username parts
        slug_parts = [p for p in parts if p and not p.startswith('@')]

        if slug_parts:
            return slug_parts[-1]
        return None

    def _parse_rss_date(self, date_str):
        """Parse RFC 2822 date string to YYYY-MM-DD format"""
        if not date_str:
            return None
        try:
            dt = parsedate_to_datetime(date_str)
            return dt.strftime('%Y-%m-%d')
        except (ValueError, TypeError):
            # Try ISO format fallback
            match = re.match(r'(\d{4}-\d{2}-\d{2})', date_str)
            if match:
                return match.group(1)
            return None

    # -------------------------------------------------------------------------
    # Page content fetching (fallback when RSS has no content:encoded)
    # -------------------------------------------------------------------------

    def fetch_post_content(self, url, retries=3):
        """Fetch full article HTML from a Medium post URL.

        Used as fallback when RSS feed doesn't include content:encoded
        (e.g. for publication posts). Extracts the <article> element.
        When sid cookie is configured, sends it to authenticate for
        member-only content (including custom domain publications).

        Returns HTML string or None on failure.
        """
        # For custom domains (not medium.com), the session cookie won't
        # auto-send, so pass sid explicitly via cookies parameter
        extra_cookies = {}
        if self.sid:
            domain = urlparse(url).hostname or ''
            if 'medium.com' not in domain:
                extra_cookies['sid'] = self.sid

        for attempt in range(retries):
            try:
                response = self.session.get(
                    url, timeout=30, cookies=extra_cookies or None
                )
                if response.status_code == 403:
                    # Cloudflare block — no retry will help
                    return None
                response.raise_for_status()

                soup = BeautifulSoup(response.text, 'html.parser')
                article = soup.find('article')
                if article:
                    return str(article)

                return None
            except requests.exceptions.RequestException:
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    return None
        return None

    # -------------------------------------------------------------------------
    # GraphQL API fetching (authenticated, for member-only posts)
    # -------------------------------------------------------------------------

    def fetch_post_graphql(self, post_id, retries=3):
        """Fetch full post content via Medium's GraphQL API.

        Requires valid sid + uid cookies. Returns markdown string or None.
        The post_id is the hex ID from the Medium URL suffix (e.g. 'abc123def456').
        """
        if not self.authenticated:
            return None

        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Cookie': f'sid={self.sid}; uid={self.uid}',
        }

        payload = {
            'operationName': 'PostPage',
            'variables': {'postId': post_id},
            'query': self.GRAPHQL_QUERY,
        }

        for attempt in range(retries):
            try:
                response = self.session.post(
                    self.GRAPHQL_URL, json=payload,
                    headers=headers, timeout=30
                )
                response.raise_for_status()
                data = response.json()

                post = data.get('data', {}).get('post')
                if not post or not post.get('content'):
                    return None

                paragraphs = (post['content']
                              .get('bodyModel', {})
                              .get('paragraphs', []))
                if len(paragraphs) < 3:
                    return None

                return self._paragraphs_to_markdown(paragraphs)

            except (requests.exceptions.RequestException, json.JSONDecodeError,
                    KeyError, TypeError):
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    return None
        return None

    def _extract_post_id(self, url):
        """Extract the hex post ID from a Medium URL.

        Medium URLs end with a hex ID after the last dash:
          https://blog.example.com/some-post-title-abc123def456
          -> abc123def456
        """
        path = urlparse(url).path.strip('/')
        slug = path.split('/')[-1] if '/' in path else path
        # Remove query params from slug
        slug = slug.split('?')[0]
        # The post ID is the last segment after the final dash
        # It's typically 12 hex chars
        match = re.search(r'-([0-9a-f]{8,})$', slug)
        if match:
            return match.group(1)
        return None

    def _paragraphs_to_markdown(self, paragraphs):
        """Convert Medium GraphQL paragraphs to markdown.

        Paragraph types: P, H3, H4, IMG, BQ, ULI, OLI, PRE
        Markup types: STRONG, EM, CODE, A (links)
        """
        lines = []
        # Skip the first H3 (title) and H4 (subtitle) — they're in frontmatter
        skip_initial_headers = True

        for para in paragraphs:
            ptype = para.get('type', '')
            text = para.get('text', '')
            markups = para.get('markups') or []
            metadata = para.get('metadata')

            if skip_initial_headers and ptype in ('H3', 'H4'):
                continue
            if ptype not in ('H3', 'H4'):
                skip_initial_headers = False

            # Apply inline markups (bold, italic, code, links)
            if text and markups:
                text = self._apply_markups(text, markups)

            if ptype == 'P':
                lines.append('')
                lines.append(text)

            elif ptype == 'H3':
                lines.append('')
                lines.append(f'## {text}')
                lines.append('')

            elif ptype == 'H4':
                lines.append('')
                lines.append(f'### {text}')
                lines.append('')

            elif ptype == 'IMG':
                if metadata and metadata.get('id'):
                    img_id = metadata['id']
                    img_url = f'https://cdn-images-1.medium.com/max/1024/{img_id}'
                    lines.append('')
                    lines.append(f'![{text}]({img_url})')
                    lines.append('')

            elif ptype == 'BQ':
                lines.append('')
                for line in text.split('\n'):
                    if line.strip():
                        lines.append(f'> {line.strip()}')
                lines.append('')

            elif ptype == 'ULI':
                lines.append(f'- {text}')

            elif ptype == 'OLI':
                lines.append(f'1. {text}')

            elif ptype == 'PRE':
                lines.append('')
                lines.append('```')
                lines.append(para.get('text', ''))  # Use raw text for code
                lines.append('```')
                lines.append('')

            elif ptype == 'MIXTAPE_EMBED':
                # Embedded link card — render as link
                href = para.get('href', '')
                if href:
                    lines.append('')
                    lines.append(f'[{text or href}]({href})')
                    lines.append('')

            else:
                if text:
                    lines.append('')
                    lines.append(text)

        result = '\n'.join(lines)
        result = re.sub(r'\n{3,}', '\n\n', result)
        return result.strip()

    def _apply_markups(self, text, markups):
        """Apply Medium markup annotations to text.

        Markups have start/end positions and types (STRONG, EM, CODE, A).
        Process from end to start to preserve positions.
        """
        # Sort markups by start position descending so insertions
        # don't shift positions of earlier markups
        sorted_markups = sorted(markups, key=lambda m: m.get('start', 0),
                                reverse=True)

        for markup in sorted_markups:
            mtype = markup.get('type', '')
            start = markup.get('start', 0)
            end = markup.get('end', 0)
            href = markup.get('href', '')
            segment = text[start:end]

            if not segment.strip():
                continue

            if mtype == 'STRONG':
                replacement = f'**{segment}**'
            elif mtype == 'EM':
                replacement = f'*{segment}*'
            elif mtype == 'CODE':
                replacement = f'`{segment}`'
            elif mtype == 'A' and href:
                replacement = f'[{segment}]({href})'
            else:
                continue

            text = text[:start] + replacement + text[end:]

        return text

    # -------------------------------------------------------------------------
    # HTML to Markdown conversion (from stored content_html)
    # -------------------------------------------------------------------------

    def parse_post_from_html(self, content_html):
        """Convert stored HTML content to markdown (no HTTP request needed).

        Returns markdown string or None if content is too short.
        """
        if not content_html or len(content_html) < 50:
            return None

        content = self._html_to_markdown(content_html)
        if not content or len(content) < 100:
            return None

        return content

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
# Medium Manager (Orchestrator)
# =============================================================================

class MediumManager:
    """Orchestrates Medium blog scraping operations"""

    def __init__(self, config=None):
        self.config = config or Config()
        self.config.load()
        self.db = MediumDatabase(self.config.database_path)
        self.scraper = MediumScraper(
            sid=self.config.sid or None,
            uid=self.config.uid or None,
        )
        self._shutdown = False

    def _setup_signal_handlers(self):
        """Setup graceful shutdown on Ctrl+C"""
        def handler(signum, frame):
            if self._shutdown:
                print("\nForce quit.")
                sys.exit(1)
            print("\nShutting down gracefully (press Ctrl+C again to force)...")
            self._shutdown = True
        signal.signal(signal.SIGINT, handler)

    def add_user(self, username):
        """Add a Medium username to track"""
        username = username.lstrip('@')
        users = list(self.config.users)
        if username in users:
            print(f"User @{username} is already tracked.")
            return
        users.append(username)
        self.config.set('users', users)
        print(f"Added @{username} to tracked users.")
        print(f"Tracked users: {', '.join('@' + u for u in users)}")

    def remove_user(self, username):
        """Remove a Medium username from tracking"""
        username = username.lstrip('@')
        users = list(self.config.users)
        if username not in users:
            print(f"User @{username} is not tracked.")
            return
        users.remove(username)
        self.config.set('users', users)
        print(f"Removed @{username} from tracked users.")
        if users:
            print(f"Remaining users: {', '.join('@' + u for u in users)}")
        else:
            print("No tracked users remaining.")

    def discover(self, username=None):
        """Fetch RSS feed and add new posts to database"""
        users = [username.lstrip('@')] if username else self.config.users
        if not users:
            print("No users to discover. Add a user first:")
            print("  scrape_medium.py add-user <username>")
            return

        total_new = 0
        for user in users:
            print(f"\nDiscovering posts for @{user}...")
            posts = self.scraper.fetch_feed_posts(user)

            if not posts:
                print(f"  No posts found for @{user}")
                continue

            new_count = 0
            with self.db:
                for post in posts:
                    added = self.db.add_post(
                        slug=post['slug'],
                        username=post['username'],
                        url=post['url'],
                        title=post['title'],
                        author=post['author'],
                        description=post['description'],
                        publish_date=post['publish_date'],
                        updated_date=post['updated_date'],
                        categories=post['categories'],
                        content_html=post['content_html'],
                    )
                    if added:
                        new_count += 1

                self.db.add_sync_log(user, len(posts), new_count, 0, 0, 'discovered')

            print(f"  New posts added: {new_count}")
            total_new += new_count

        with self.db:
            counts = self.db.get_counts()
        print(f"\nTotal in database: {counts.get('total', 0)}")
        print(f"  Pending: {counts.get('pending', 0)}")
        print(f"  Downloaded: {counts.get('downloaded', 0)}")
        print(f"  Failed: {counts.get('failed', 0)}")

    def scrape(self, slug=None, limit=None, parallel=False):
        """Convert pending posts from stored HTML to markdown files"""
        self._setup_signal_handlers()

        with self.db:
            if slug:
                post = self.db.get_post(slug)
                if not post:
                    print(f"Error: Post with slug '{slug}' not found")
                    print("Run 'discover' first to populate the database")
                    return
                posts = [post]
            else:
                posts = self.db.get_posts(status='pending', limit=limit)

        if not posts:
            print("No pending posts to scrape.")
            return

        if parallel and not slug:
            self._scrape_parallel(posts)
        else:
            self._scrape_sequential(posts)

    def _scrape_sequential(self, posts):
        """Convert posts one at a time"""
        print(f"Scraping {len(posts)} Medium posts (sequential)")
        print(f"Delay between requests: {self.config.delay}s")
        print()

        successful = 0
        failed = 0
        fetched_from_web = 0

        for i, post in enumerate(posts):
            if self._shutdown:
                print("\nStopping due to shutdown request.")
                break

            slug = post['slug']
            print(f"[{i+1}/{len(posts)}] {slug}...", end=" ", flush=True)

            # Get full post data (with content_html) if we don't have it
            with self.db:
                full_post = self.db.get_post(slug)

            content_html = full_post.get('content_html') if full_post else None
            content = self.scraper.parse_post_from_html(content_html)

            # Fallback: try GraphQL API (authenticated, full member content)
            if not content and full_post:
                post_id = self.scraper._extract_post_id(full_post['url'])
                if post_id:
                    content = self.scraper.fetch_post_graphql(
                        post_id, retries=self.config.max_retries
                    )
                    if content:
                        fetched_from_web += 1

            # Fallback: fetch page HTML (unauthenticated preview)
            if not content and full_post:
                fetched_html = self.scraper.fetch_post_content(
                    full_post['url'], retries=self.config.max_retries
                )
                if fetched_html:
                    content = self.scraper.parse_post_from_html(fetched_html)
                    if content:
                        fetched_from_web += 1

            if content:
                filepath = self._save_post(full_post, content)

                word_count = len(content.split())
                char_count = len(content)

                with self.db:
                    self.db.update_post(
                        slug,
                        word_count=word_count,
                        char_count=char_count,
                        file_path=filepath,
                        status='downloaded',
                        downloaded_at=datetime.now(),
                    )

                print(f"OK ({word_count} words)")
                successful += 1
            else:
                with self.db:
                    self.db.update_post(slug, status='failed')
                print("FAILED (no content)")
                failed += 1

            if i < len(posts) - 1 and not self._shutdown:
                time.sleep(self.config.delay)

        # Log sync
        with self.db:
            status = 'success' if failed == 0 else 'partial'
            self.db.add_sync_log(None, 0, 0, successful, failed, status)

        print(f"\nComplete: {successful} downloaded, {failed} failed")
        if fetched_from_web > 0:
            print(f"  ({fetched_from_web} fetched from web, rest from RSS cache)")

    def _scrape_parallel(self, posts):
        """Download posts in parallel using ThreadPoolExecutor.

        HTTP fetching and file saving run in parallel threads.
        DB writes are serialized on the main thread to avoid DuckDB
        concurrency issues.
        """
        max_workers = self.config.max_workers
        delay = self.config.delay

        print(f"Scraping {len(posts)} Medium posts (parallel, {max_workers} workers)")
        print(f"Delay between requests: {delay}s per worker")
        print()

        successful = 0
        failed = 0
        counter_lock = threading.Lock()
        progress_lock = threading.Lock()
        completed = 0

        # Pre-fetch full post data (including content_html) on main thread
        # DuckDB connections are not thread-safe, so all DB reads happen here
        full_posts = {}
        with self.db:
            for p in posts:
                full_posts[p['slug']] = self.db.get_post(p['slug'])

        def _fetch_post(post):
            """Fetch and convert a single post (no DB writes)."""
            nonlocal completed

            if self._shutdown:
                return None

            slug = post['slug']
            full_post = full_posts.get(slug)

            content_html = full_post.get('content_html') if full_post else None
            content = self.scraper.parse_post_from_html(content_html)
            fetched_html = None

            # Fallback: try GraphQL API (authenticated, full content)
            if not content and full_post:
                post_id = self.scraper._extract_post_id(full_post['url'])
                if post_id:
                    content = self.scraper.fetch_post_graphql(
                        post_id, retries=self.config.max_retries
                    )

            # Fallback: fetch page HTML (unauthenticated preview)
            if not content and full_post:
                fetched_html = self.scraper.fetch_post_content(
                    full_post['url'], retries=self.config.max_retries
                )
                if fetched_html:
                    content = self.scraper.parse_post_from_html(fetched_html)

            with counter_lock:
                completed += 1
                idx = completed

            if content:
                filepath = self._save_post(full_post, content)
                word_count = len(content.split())
                char_count = len(content)

                with progress_lock:
                    print(f"[{idx}/{len(posts)}] {slug}... OK ({word_count} words)")

                time.sleep(delay)
                return {
                    'slug': slug,
                    'success': True,
                    'filepath': filepath,
                    'word_count': word_count,
                    'char_count': char_count,
                    'fetched_html': fetched_html,
                }
            else:
                with progress_lock:
                    print(f"[{idx}/{len(posts)}] {slug}... FAILED (no content)")

                time.sleep(delay)
                return {
                    'slug': slug,
                    'success': False,
                }

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            for post in posts:
                if self._shutdown:
                    break
                future = executor.submit(_fetch_post, post)
                futures[future] = post

            for future in as_completed(futures):
                if self._shutdown:
                    executor.shutdown(wait=False, cancel_futures=True)
                    break

                outcome = future.result()
                if outcome is None:
                    continue

                if outcome['success']:
                    with self.db:
                        if outcome.get('fetched_html'):
                            self.db.update_post(
                                outcome['slug'],
                                content_html=outcome['fetched_html'],
                            )
                        self.db.update_post(
                            outcome['slug'],
                            word_count=outcome['word_count'],
                            char_count=outcome['char_count'],
                            file_path=outcome['filepath'],
                            status='downloaded',
                            downloaded_at=datetime.now(),
                        )
                    successful += 1
                else:
                    with self.db:
                        self.db.update_post(outcome['slug'], status='failed')
                    failed += 1

        # Log sync
        with self.db:
            status = 'success' if failed == 0 else 'partial'
            self.db.add_sync_log(None, 0, 0, successful, failed, status)

        print(f"\nComplete: {successful} downloaded, {failed} failed")

    def _save_post(self, post, content):
        """Save a Medium post as Obsidian markdown.

        Format matches existing Obsidian clippings with:
        - Title-based filename
        - Author as wikilink in YAML list
        - published/created date fields
        - description
        - tags: clippings, medium
        """
        output_dir = Path(self.config.staging_dir) / 'Medium'
        output_dir.mkdir(parents=True, exist_ok=True)

        # Filename based on title (sanitized)
        title = post.get('title') or post['slug']
        safe_title = re.sub(r'[<>:"/\\|?*]', '', title)
        safe_title = re.sub(r'\s+', ' ', safe_title).strip()
        filepath = output_dir / f"{safe_title}.md"

        # Escape quotes in title for YAML
        yaml_title = title.replace('"', '\\"')

        date = str(post.get('publish_date') or '')[:10]
        today = datetime.now().strftime('%Y-%m-%d')
        description = (post.get('description') or '').replace('"', '\\"')

        # Build frontmatter
        fm = []
        fm.append('---')
        fm.append(f'title: "{yaml_title}"')
        fm.append(f'source: "{post["url"]}"')

        # Author as wikilink
        author = post.get('author')
        if author:
            fm.append('author:')
            fm.append(f'  - "[[{author}]]"')

        if date:
            fm.append(f'published: {date}')
        fm.append(f'created: {today}')
        if description:
            fm.append(f'description: "{description}"')
        fm.append('tags:')
        fm.append('  - "clippings"')
        fm.append('  - "medium"')
        fm.append('---')

        # Build body
        body_parts = ['\n'.join(fm)]

        # Main content
        body_parts.append('')
        body_parts.append(content)
        body_parts.append('')

        with open(filepath, 'w', encoding='utf-8') as f:
            f.write('\n'.join(body_parts))

        return str(filepath)

    def list_posts(self, status=None, username=None, json_output=False):
        """List Medium posts"""
        with self.db:
            posts = self.db.get_posts(status=status, username=username)

        if json_output:
            print(json.dumps(posts, indent=2, default=str))
            return posts

        if not posts:
            filters = []
            if status:
                filters.append(f'status={status}')
            if username:
                filters.append(f'user=@{username}')
            filter_str = f" ({', '.join(filters)})" if filters else ''
            print(f"No posts found{filter_str}.")
            return []

        print(f"\n{'ID':<6} {'Status':<12} {'User':<16} {'Date':<12} {'Title'}")
        print('-' * 100)
        for p in posts:
            title = (p.get('title') or '')[:45]
            date = str(p.get('publish_date') or '')[:10]
            user = (p.get('username') or '')[:15]
            print(f"{p['id']:<6} {p['status']:<12} @{user:<15} {date:<12} {title}")
        print(f"\nTotal: {len(posts)}")
        return posts

    def show_status(self, username=None):
        """Show summary statistics"""
        with self.db:
            counts = self.db.get_counts(username=username)

        total = counts.get('total', 0)
        pending = counts.get('pending', 0)
        downloaded = counts.get('downloaded', 0)
        failed = counts.get('failed', 0)

        header = f"Medium Scraper Status"
        if username:
            header += f" (@{username})"
        print(f"\n{header}")
        print(f"{'='*40}")
        print(f"Total posts:    {total}")
        print(f"  Pending:      {pending}")
        print(f"  Downloaded:   {downloaded}")
        print(f"  Failed:       {failed}")

        if total > 0:
            pct = (downloaded / total) * 100
            print(f"\nProgress: {pct:.1f}%")

        # Show tracked users
        users = self.config.users
        if users:
            print(f"\nTracked users: {', '.join('@' + u for u in users)}")
        else:
            print(f"\nNo tracked users. Add one with: add-user <username>")

        print()

    def move_to_obsidian(self, all_posts=False):
        """Move downloaded posts to Obsidian vault"""
        obsidian_path = self.config.obsidian_vault
        if not obsidian_path:
            print("Error: Obsidian vault path not configured")
            print("Run: scrape_medium.py config set obsidian_vault /path/to/vault")
            return

        obsidian_path = Path(obsidian_path).expanduser()
        if not obsidian_path.exists():
            print(f"Error: Obsidian vault path does not exist: {obsidian_path}")
            return

        dst_dir = obsidian_path / 'Medium'
        dst_dir.mkdir(parents=True, exist_ok=True)

        with self.db:
            posts = self.db.get_unmoved_posts()

        if not posts:
            print("No posts to move (all already in Obsidian or none downloaded).")
            return

        moved_slugs = []
        for post in posts:
            src_file = Path(post['file_path'])
            if src_file.exists():
                dst_file = dst_dir / src_file.name
                shutil.copy2(src_file, dst_file)
                moved_slugs.append(post['slug'])

        with self.db:
            self.db.mark_moved(moved_slugs)

        print(f"Moved {len(moved_slugs)} posts to: {dst_dir}")

    def retry_failed(self):
        """Reset failed posts to pending"""
        with self.db:
            count = self.db.reset_failed()
        print(f"Reset failed posts. Pending count: {count}")

    # -------------------------------------------------------------------------
    # Series linking
    # -------------------------------------------------------------------------

    ROMAN = {'I': 1, 'II': 2, 'III': 3, 'IV': 4, 'V': 5,
             'VI': 6, 'VII': 7, 'VIII': 8, 'IX': 9, 'X': 10}

    _SERIES_PATTERNS = [
        # "… Part I of II", "… Part II of III"  (separator optional)
        re.compile(r'^(.*?)\s*[:\-\u2014\u2013]?\s*Part\s+(I{1,3}V?|VI{0,3}|IX|X)\s+of\s+\w+\s*$', re.IGNORECASE),
        # "… Part I", "… Part II"  (no total)
        re.compile(r'^(.*?)\s*[:\-\u2014\u2013]\s*Part\s+(I{1,3}V?|VI{0,3}|IX|X)\s*$', re.IGNORECASE),
        # "… Part 1 of 3", "… Part 2"  (arabic)
        re.compile(r'^(.*?)\s*[:\-\u2014\u2013]\s*Part\s+(\d+)(?:\s+of\s+\d+)?\s*$', re.IGNORECASE),
        # "(Part I)" or "(Part 1)" at end
        re.compile(r'^(.*?)\s*\(Part\s+(I{1,3}V?|VI{0,3}|IX|X|\d+)\)\s*$', re.IGNORECASE),
    ]

    def _detect_series(self, title):
        """Return (base_title, part_num) if title is part of a multi-part series, else None."""
        for pattern in self._SERIES_PATTERNS:
            m = pattern.match(title.strip())
            if m:
                base, part_str = m.group(1).strip(), m.group(2).upper()
                if part_str in self.ROMAN:
                    part_num = self.ROMAN[part_str]
                else:
                    try:
                        part_num = int(part_str)
                    except ValueError:
                        continue
                return base, part_num
        return None

    @staticmethod
    def _safe_filename(title):
        s = re.sub(r'[<>:"/\\|?*]', '', title)
        return re.sub(r'\s+', ' ', s).strip()

    def link_series(self, vault=False):
        """Detect multi-part series among downloaded posts and insert Obsidian wiki links.

        Operates on staging_dir by default; pass vault=True to also patch files
        already moved to the Obsidian vault.
        """
        dirs_to_check = [Path(self.config.staging_dir) / 'Medium']
        if vault and self.config.obsidian_vault:
            dirs_to_check.append(Path(self.config.obsidian_vault).expanduser() / 'Medium')

        with self.db:
            posts = self.db.get_posts(status='downloaded')

        if not posts:
            print("No downloaded posts found.")
            return

        # Group by detected series base title
        series: dict[str, list[tuple[int, dict, str]]] = {}
        unmatched = 0
        for post in posts:
            title = post.get('title') or ''
            result = self._detect_series(title)
            if result:
                base, part_num = result
                series.setdefault(base, []).append((part_num, post, title))
            else:
                unmatched += 1

        multi = {b: parts for b, parts in series.items() if len(parts) >= 2}
        if not multi:
            print(f"No multi-part series detected among {len(posts)} posts ({unmatched} unmatched).")
            return

        total_linked = 0
        for base_title, parts in multi.items():
            parts.sort(key=lambda x: x[0])
            print(f"\nSeries: {base_title!r}  ({len(parts)} parts)")

            for i, (part_num, post, title) in enumerate(parts):
                safe = self._safe_filename(title)

                # Build nav block
                nav_lines = ['', '---', '', '**Series Navigation**', '']
                if i > 0:
                    prev_safe = self._safe_filename(parts[i - 1][2])
                    nav_lines.append(f'- ← Previous: [[{prev_safe}]]')
                if i < len(parts) - 1:
                    next_safe = self._safe_filename(parts[i + 1][2])
                    nav_lines.append(f'- → Next: [[{next_safe}]]')
                nav_lines.append('')
                nav_block = '\n'.join(nav_lines)

                patched = 0
                for d in dirs_to_check:
                    filepath = d / f"{safe}.md"
                    if not filepath.exists():
                        continue
                    text = filepath.read_text(encoding='utf-8')
                    # Strip any previous nav block
                    marker = '\n---\n\n**Series Navigation**'
                    if marker in text:
                        text = text[:text.index(marker)]
                    filepath.write_text(text.rstrip() + nav_block, encoding='utf-8')
                    print(f"  Part {part_num}: [[{safe}]]  ✓")
                    patched += 1
                    total_linked += 1

                if patched == 0:
                    print(f"  Part {part_num}: file not found — {safe}.md")

        series_count = len(multi)
        print(f"\nDone. Linked {total_linked} files across {series_count} series.")

    def show_config(self):
        """Display current configuration"""
        print(f"Config file: {self.config.config_file}")
        print(f"Local config: {self.config.local_config_file}")
        print()
        print(f"staging_dir:   {self.config.staging_dir}")
        print(f"obsidian_vault: {self.config.obsidian_vault or '(not set)'}")
        print(f"database:      {self.config.database_path}")
        print(f"delay:         {self.config.delay}")
        print(f"max_retries:   {self.config.max_retries}")
        print(f"max_workers:   {self.config.max_workers}")
        sid = self.config.sid
        uid = self.config.uid
        if sid and uid:
            print(f"sid:           {sid[:8]}...{sid[-4:]} (set)")
            print(f"uid:           {uid} (set)")
            print(f"auth:          authenticated (full member-only content)")
        elif sid or uid:
            print(f"sid:           {'(set)' if sid else '(not set)'}")
            print(f"uid:           {'(set)' if uid else '(not set)'}")
            print(f"auth:          incomplete (need both sid and uid)")
        else:
            print(f"sid:           (not set)")
            print(f"uid:           (not set)")
            print(f"auth:          none (member-only posts will be previews only)")
        users = self.config.users
        if users:
            print(f"users:         {', '.join('@' + u for u in users)}")
        else:
            print(f"users:         (none)")

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
        print(f"Set medium.{key} = {value}")


# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Medium Blog Scraper - Download Medium posts as Obsidian markdown',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Add a Medium user to track
  %(prog)s add-user vutrinh274

  # Discover posts from RSS feed
  %(prog)s discover

  # Convert all pending posts to markdown
  %(prog)s scrape

  # Convert a single post by slug
  %(prog)s scrape --slug "some-post-slug-abc123"

  # List posts by status
  %(prog)s list --status downloaded

  # Show statistics
  %(prog)s status

  # Move to Obsidian
  %(prog)s move --all

  # Retry failed posts
  %(prog)s retry

  # Configuration
  %(prog)s config show
  %(prog)s config set obsidian_vault "/path/to/vault"
        """
    )

    subparsers = parser.add_subparsers(dest='command', help='Commands')

    # Add user
    add_user_parser = subparsers.add_parser('add-user', help='Add a Medium username to track')
    add_user_parser.add_argument('username', help='Medium username (with or without @)')

    # Remove user
    remove_user_parser = subparsers.add_parser('remove-user', help='Remove a tracked Medium username')
    remove_user_parser.add_argument('username', help='Medium username (with or without @)')

    # Discover
    discover_parser = subparsers.add_parser('discover', help='Fetch RSS feed and add new posts to database')
    discover_parser.add_argument('--user', help='Discover for a specific user only')

    # Scrape
    scrape_parser = subparsers.add_parser('scrape', help='Convert pending posts to markdown')
    scrape_parser.add_argument('--slug', help='Convert a specific post by slug')
    scrape_parser.add_argument('--limit', type=int, help='Limit number of posts to convert')
    scrape_parser.add_argument('--parallel', action='store_true',
                               help='Convert in parallel (default 4 workers)')
    scrape_parser.add_argument('--sequential', action='store_true',
                               help='Convert sequentially (default)')
    scrape_parser.add_argument('--workers', type=int,
                               help='Number of parallel workers (implies --parallel)')

    # List
    list_parser = subparsers.add_parser('list', help='List Medium posts')
    list_parser.add_argument('--status', choices=['pending', 'downloaded', 'failed'],
                             help='Filter by status')
    list_parser.add_argument('--user', help='Filter by username')
    list_parser.add_argument('--json', action='store_true', help='Output as JSON')

    # Status
    status_parser = subparsers.add_parser('status', help='Show summary statistics')
    status_parser.add_argument('--user', help='Show status for a specific user')

    # Move
    move_parser = subparsers.add_parser('move', help='Move downloaded posts to Obsidian vault')
    move_parser.add_argument('--all', action='store_true', required=True,
                             help='Move all downloaded posts')

    # Retry
    subparsers.add_parser('retry', help='Reset failed posts to pending')

    # Link series
    link_series_parser = subparsers.add_parser(
        'link-series',
        help='Detect multi-part series and add Obsidian [[wiki links]] between them')
    link_series_parser.add_argument(
        '--vault', action='store_true',
        help='Also patch files already moved to the Obsidian vault')

    # Config
    config_parser = subparsers.add_parser('config', help='View/set configuration')
    config_parser.add_argument('action', choices=['show', 'set'], help='Action')
    config_parser.add_argument('key', nargs='?', help='Config key')
    config_parser.add_argument('value', nargs='?', help='Value to set')

    args = parser.parse_args()

    config = Config()
    config.load()
    manager = MediumManager(config)

    if args.command == 'add-user':
        manager.add_user(args.username)

    elif args.command == 'remove-user':
        manager.remove_user(args.username)

    elif args.command == 'discover':
        manager.discover(username=args.user)

    elif args.command == 'scrape':
        parallel = args.parallel or bool(args.workers)
        if args.sequential:
            parallel = False
        if args.workers:
            config.set('max_workers', args.workers)
            config.load()
        manager.scrape(slug=args.slug, limit=args.limit, parallel=parallel)

    elif args.command == 'list':
        manager.list_posts(status=args.status, username=args.user,
                           json_output=args.json)

    elif args.command == 'status':
        manager.show_status(username=args.user)

    elif args.command == 'move':
        manager.move_to_obsidian(all_posts=args.all)

    elif args.command == 'retry':
        manager.retry_failed()

    elif args.command == 'link-series':
        manager.link_series(vault=args.vault)

    elif args.command == 'config':
        if args.action == 'show':
            manager.show_config()
        elif args.action == 'set':
            if not args.key or args.value is None:
                print("Usage: config set <key> <value>")
                print("Example: config set obsidian_vault /path/to/vault")
                return
            manager.set_config(args.key, args.value)

    else:
        parser.print_help()


if __name__ == '__main__':
    main()
