#!/usr/bin/env python3
"""
Documentation Site Scraper - Scrapes documentation sites to Obsidian markdown.

Scrapes full documentation from Snowflake and Soda (extensible to more sites),
saving each page as Obsidian markdown with DuckDB tracking and Obsidian vault
integration matching the existing scraper pipeline.

Commands:
    discover    Discover pages from documentation sites
    scrape      Download pending pages as markdown
    list        List pages by status
    status      Show summary statistics
    move        Move downloaded pages to Obsidian vault
    retry       Reset failed pages to pending
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
from abc import ABC, abstractmethod
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse, urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
from xml.etree import ElementTree

import duckdb
import requests
from bs4 import BeautifulSoup


# =============================================================================
# Configuration
# =============================================================================

class Config:
    """Configuration manager for docs scraper (reads [docs] section from TOML)"""

    DEFAULT_CONFIG = {
        'docs': {
            'staging_dir': 'docs_obsidian',
            'obsidian_vault': '',
            'database': 'docs.db',
            'delay': 1.0,
            'max_retries': 3,
            'max_workers': 4,
            'aws_paths': [],
            'databricks_paths': ['aws/en'],
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
        """Get a config value from [docs] section"""
        if self._config is None:
            self.load()
        return self._config.get('docs', {}).get(key, default)

    def set(self, key, value):
        """Set a config value in local config [docs] section"""
        local_config = {}
        if self.local_config_file.exists():
            with open(self.local_config_file, 'rb') as f:
                local_config = tomllib.load(f)

        if 'docs' not in local_config:
            local_config['docs'] = {}
        local_config['docs'][key] = value

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
        return self.get('staging_dir', 'docs_obsidian')

    @property
    def obsidian_vault(self):
        return self.get('obsidian_vault', '')

    @property
    def database_path(self):
        return self.get('database', 'docs.db')

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
    def aws_paths(self):
        return self.get('aws_paths', [])

    @property
    def databricks_paths(self):
        return self.get('databricks_paths', ['aws/en'])


# =============================================================================
# Database
# =============================================================================

class DocsDatabase:
    """DuckDB database manager for tracking documentation pages"""

    SCHEMA = """
    CREATE TABLE IF NOT EXISTS doc_pages (
        id INTEGER PRIMARY KEY,
        site VARCHAR NOT NULL,
        slug VARCHAR NOT NULL,
        url VARCHAR NOT NULL,
        title VARCHAR,
        parent_path VARCHAR,
        word_count INTEGER,
        char_count INTEGER,
        file_path VARCHAR,
        status VARCHAR DEFAULT 'pending',
        error_reason VARCHAR,
        error_detail VARCHAR,
        downloaded_at TIMESTAMP,
        in_obsidian BOOLEAN DEFAULT FALSE,
        moved_at TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(site, slug)
    );

    CREATE TABLE IF NOT EXISTS doc_sync_logs (
        id INTEGER PRIMARY KEY,
        synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        site VARCHAR,
        total_discovered INTEGER,
        new_pages_found INTEGER,
        pages_downloaded INTEGER,
        pages_failed INTEGER,
        status VARCHAR
    );

    CREATE SEQUENCE IF NOT EXISTS doc_pages_id_seq;
    CREATE SEQUENCE IF NOT EXISTS doc_sync_logs_id_seq;
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

    def add_page(self, site, slug, url, title=None, parent_path=None):
        """Add a new page (pending status). Returns True if added, False if duplicate."""
        try:
            self.conn.execute("""
                INSERT INTO doc_pages (id, site, slug, url, title, parent_path)
                VALUES (nextval('doc_pages_id_seq'), ?, ?, ?, ?, ?)
            """, [site, slug, url, title, parent_path])
            return True
        except duckdb.ConstraintException:
            return False

    def get_pages(self, site=None, status=None, limit=None, slug=None):
        """Get pages, optionally filtered by site and/or status"""
        query = "SELECT id, site, slug, url, title, parent_path, status, file_path, in_obsidian FROM doc_pages"
        conditions = []
        params = []
        if site:
            conditions.append("site = ?")
            params.append(site)
        if status:
            conditions.append("status = ?")
            params.append(status)
        if slug:
            conditions.append("slug = ?")
            params.append(slug)
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY id"
        if limit:
            query += " LIMIT ?"
            params.append(limit)
        results = self.conn.execute(query, params).fetchall()
        columns = ['id', 'site', 'slug', 'url', 'title', 'parent_path', 'status', 'file_path', 'in_obsidian']
        return [dict(zip(columns, row)) for row in results]

    def update_page(self, site, slug, **kwargs):
        """Update page fields"""
        valid_fields = [
            'title', 'word_count', 'char_count', 'file_path',
            'status', 'downloaded_at', 'in_obsidian', 'moved_at',
            'error_reason', 'error_detail'
        ]
        updates = []
        values = []
        for field, value in kwargs.items():
            if field in valid_fields:
                updates.append(f"{field} = ?")
                values.append(value)
        if updates:
            values.extend([site, slug])
            self.conn.execute(
                f"UPDATE doc_pages SET {', '.join(updates)} WHERE site = ? AND slug = ?",
                values
            )

    def get_counts(self, site=None):
        """Get status counts"""
        query = "SELECT status, COUNT(*) FROM doc_pages"
        params = []
        if site:
            query += " WHERE site = ?"
            params.append(site)
        query += " GROUP BY status"
        results = self.conn.execute(query, params).fetchall()
        counts = {row[0]: row[1] for row in results}

        total_query = "SELECT COUNT(*) FROM doc_pages"
        if site:
            total_query += " WHERE site = ?"
        total = self.conn.execute(total_query, params).fetchone()[0]
        counts['total'] = total
        return counts

    def get_unmoved_pages(self, site=None):
        """Get downloaded pages not yet in Obsidian"""
        query = """
            SELECT id, site, slug, url, title, file_path
            FROM doc_pages
            WHERE status = 'downloaded' AND in_obsidian = FALSE AND file_path IS NOT NULL
        """
        params = []
        if site:
            query += " AND site = ?"
            params.append(site)
        query += " ORDER BY id"
        results = self.conn.execute(query, params).fetchall()
        columns = ['id', 'site', 'slug', 'url', 'title', 'file_path']
        return [dict(zip(columns, row)) for row in results]

    def mark_moved(self, site, slugs):
        """Mark pages as moved to Obsidian"""
        if not slugs:
            return
        placeholders = ','.join(['?'] * len(slugs))
        self.conn.execute(f"""
            UPDATE doc_pages
            SET in_obsidian = TRUE, moved_at = CURRENT_TIMESTAMP
            WHERE site = ? AND slug IN ({placeholders})
        """, [site] + list(slugs))

    def reset_failed(self, site=None):
        """Reset failed pages to pending"""
        query = "UPDATE doc_pages SET status = 'pending', error_reason = NULL, error_detail = NULL WHERE status = 'failed'"
        params = []
        if site:
            query += " AND site = ?"
            params.append(site)
        self.conn.execute(query, params)
        count_query = "SELECT COUNT(*) FROM doc_pages WHERE status = 'pending'"
        if site:
            count_query += " AND site = ?"
        return self.conn.execute(count_query, params).fetchone()[0]

    def add_sync_log(self, site, total_discovered, new_found, downloaded, failed, status):
        """Add a sync log entry"""
        self.conn.execute("""
            INSERT INTO doc_sync_logs (id, site, total_discovered, new_pages_found,
                                       pages_downloaded, pages_failed, status)
            VALUES (nextval('doc_sync_logs_id_seq'), ?, ?, ?, ?, ?, ?)
        """, [site, total_discovered, new_found, downloaded, failed, status])


# =============================================================================
# Site Scraper Registry
# =============================================================================

SITE_SCRAPERS = {}


def register_site(key):
    """Decorator to register a site scraper class"""
    def decorator(cls):
        SITE_SCRAPERS[key] = cls
        return cls
    return decorator


# =============================================================================
# Base Site Scraper
# =============================================================================

class BaseSiteScraper(ABC):
    """Abstract base class for documentation site scrapers"""

    SITE_KEY = ''
    SITE_LABEL = ''
    BASE_URL = ''

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': (
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            ),
        })

    @abstractmethod
    def discover_pages(self):
        """Discover all pages. Returns list of dicts: {slug, url, title, parent_path}"""
        pass

    @abstractmethod
    def fetch_page_content(self, url, slug, retries=3):
        """Fetch and convert a single page. Returns {title, content} or {error, reason, detail}"""
        pass

    def extra_tags(self, page):
        """Return additional tags for a page beyond ['clippings', 'documentation', SITE_KEY].
        Override in subclasses to add site-specific tags."""
        return []

    # -------------------------------------------------------------------------
    # HTML to Markdown conversion
    # -------------------------------------------------------------------------

    def _html_to_markdown(self, html):
        """Convert body HTML string to markdown"""
        soup = BeautifulSoup(html, 'html.parser')

        # Strip nav/sidebar/TOC/footer (keep header — may contain page title inside content)
        for tag in soup.find_all(['nav', 'footer', 'aside', 'script', 'style', 'noscript', 'svg']):
            tag.decompose()

        # Strip elements whose primary role is navigation/sidebar/toc
        # Use exact class matching to avoid stripping content elements with utility classes
        for el in soup.find_all(True):
            classes = (el.attrs or {}).get('class', [])
            # Only strip if a class IS the navigation element (not just contains substring)
            exact_skip = {'sidebar', 'toc', 'table-of-contents', 'breadcrumb',
                          'breadcrumbs', 'pagination', 'site-navigation'}
            if any(c.lower() in exact_skip for c in classes):
                el.decompose()

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

        skip_tags = {'script', 'style', 'nav', 'footer', 'iframe', 'noscript', 'svg'}
        if tag in skip_tags:
            return

        skip_classes = {'share', 'social', 'sidebar', 'related', 'comment', 'newsletter',
                        'cta', 'breadcrumb', 'breadcrumbs', 'pagination', 'site-navigation'}
        elem_class_list = [c.lower() for c in (elem.attrs or {}).get('class', [])]
        if any(c in skip_classes for c in elem_class_list):
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
            # Check for admonitions (Snowflake/GitBook use various patterns)
            admonition_type = self._detect_admonition(elem)
            if admonition_type:
                text = elem.get_text(strip=True)
                if text:
                    # Remove the admonition label from text if present
                    for label in ['Note', 'Warning', 'Important', 'Tip', 'Caution', 'Info']:
                        if text.startswith(label):
                            text = text[len(label):].lstrip(':').strip()
                            break
                    lines.append('')
                    lines.append(f'> [!{admonition_type}]')
                    for line in text.split('\n'):
                        line = line.strip()
                        if line:
                            lines.append(f'> {line}')
                    lines.append('')
            else:
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
            self._process_list(elem, lines, depth=0, ordered=(tag == 'ol'))
            lines.append('')

        elif tag == 'pre':
            code = elem.find('code')
            if code:
                lang = ''
                for cls in (code.attrs or {}).get('class', []):
                    if cls.startswith('language-'):
                        lang = cls.replace('language-', '')
                        break
                    elif cls.startswith('lang-'):
                        lang = cls.replace('lang-', '')
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

        elif tag in ('div', 'section', 'article', 'main', 'header', 'span', 'a', 'details', 'summary'):
            for child in elem.children:
                if hasattr(child, 'name') and child.name:
                    self._process_element(child, lines, depth + 1)
                elif child.string and child.string.strip():
                    lines.append(child.string.strip())

        elif tag == 'dl':
            # Definition lists
            lines.append('')
            for child in elem.children:
                if not hasattr(child, 'name'):
                    continue
                if child.name == 'dt':
                    text = child.get_text(strip=True)
                    if text:
                        lines.append(f'**{text}**')
                elif child.name == 'dd':
                    text = self._inline_to_markdown(child)
                    if text.strip():
                        lines.append(f': {text.strip()}')
            lines.append('')

        else:
            for child in elem.children:
                if hasattr(child, 'name') and child.name:
                    self._process_element(child, lines, depth + 1)
                elif child.string and child.string.strip():
                    lines.append(child.string.strip())

    def _process_list(self, elem, lines, depth=0, ordered=False):
        """Process nested lists with proper indentation"""
        indent = '  ' * depth
        for i, li in enumerate(elem.find_all('li', recursive=False)):
            # Get direct text content (not nested lists)
            text_parts = []
            nested_list = None
            for child in li.children:
                if hasattr(child, 'name') and child.name in ('ul', 'ol'):
                    nested_list = child
                elif hasattr(child, 'name') and child.name:
                    inline = self._inline_to_markdown(child)
                    if inline.strip():
                        text_parts.append(inline.strip())
                elif child.string and child.string.strip():
                    text_parts.append(child.string.strip())

            text = ' '.join(text_parts)
            if not text:
                text = self._inline_to_markdown(li)
                # Remove any nested list text that got included
                if nested_list:
                    nested_text = nested_list.get_text()
                    text = text.replace(nested_text, '').strip()

            if text.strip():
                prefix = f"{i+1}." if ordered else '-'
                lines.append(f"{indent}{prefix} {text.strip()}")

            if nested_list:
                self._process_list(nested_list, lines, depth=depth+1,
                                   ordered=(nested_list.name == 'ol'))

    def _detect_admonition(self, elem):
        """Detect if a blockquote is an admonition and return its type"""
        classes = ' '.join((elem.attrs or {}).get('class', [])).lower()
        text = elem.get_text(strip=True)[:50].lower()

        admonition_map = {
            'note': 'NOTE', 'info': 'NOTE', 'information': 'NOTE',
            'warning': 'WARNING', 'warn': 'WARNING', 'caution': 'WARNING',
            'important': 'IMPORTANT', 'danger': 'WARNING',
            'tip': 'TIP', 'hint': 'TIP',
        }

        for key, value in admonition_map.items():
            if key in classes:
                return value

        # Check for text-based admonitions (e.g., "Note:", "Warning:")
        for key, value in admonition_map.items():
            if text.startswith(key + ':') or text.startswith(key + ' '):
                return value

        return None

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
            elif child.name in ('ul', 'ol'):
                # Skip nested lists in inline context
                continue
            else:
                text = child.get_text()
                if text:
                    parts.append(text)
        return ''.join(parts)

    def _table_to_markdown(self, table, lines):
        """Convert HTML table to markdown table"""
        rows = []
        for tr in table.find_all('tr'):
            cells = [td.get_text(strip=True).replace('|', '\\|').replace('\n', ' ')
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
# Snowflake Scraper
# =============================================================================

@register_site('snowflake')
class SnowflakeScraper(BaseSiteScraper):
    """Scraper for docs.snowflake.com using Next.js JSON endpoints"""

    SITE_KEY = 'snowflake'
    SITE_LABEL = 'Snowflake'
    BASE_URL = 'https://docs.snowflake.com'

    # Top-level documentation sections to crawl
    SECTION_ROOTS = [
        'user-guide-getting-started',
        'guides',
        'developer',
        'reference',
        'release-notes/overview',
        'tutorials',
    ]

    def __init__(self):
        super().__init__()
        self._build_id = None

    def _get_build_id(self, force_refresh=False):
        """Extract Next.js buildId from the homepage"""
        if self._build_id and not force_refresh:
            return self._build_id

        try:
            resp = self.session.get(f'{self.BASE_URL}/en/', timeout=30)
            resp.raise_for_status()

            # Look for __NEXT_DATA__ script tag
            soup = BeautifulSoup(resp.text, 'html.parser')
            script = soup.find('script', id='__NEXT_DATA__')
            if script and script.string:
                data = json.loads(script.string)
                self._build_id = data.get('buildId')
                if self._build_id:
                    return self._build_id

            # Fallback: regex search
            match = re.search(r'"buildId"\s*:\s*"([^"]+)"', resp.text)
            if match:
                self._build_id = match.group(1)
                return self._build_id

            raise RuntimeError("Could not extract buildId from Snowflake docs homepage")
        except requests.RequestException as e:
            raise RuntimeError(f"Failed to fetch Snowflake docs homepage: {e}")

    def _fetch_section_json(self, section_path, retries=3):
        """Fetch the Next.js JSON for a section path"""
        build_id = self._get_build_id()

        for attempt in range(retries):
            url = f'{self.BASE_URL}/_next/data/{build_id}/en/{section_path}.json'
            try:
                resp = self.session.get(url, timeout=30)
                if resp.status_code == 404 and attempt == 0:
                    # buildId may have rotated, refresh and retry
                    build_id = self._get_build_id(force_refresh=True)
                    continue
                resp.raise_for_status()
                return resp.json()
            except requests.RequestException as e:
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    print(f"  Warning: Failed to fetch section {section_path}: {e}")
                    return None
        return None

    def _walk_tree(self, node, pages, parent_path=''):
        """Recursively walk the page tree and collect page nodes"""
        if not isinstance(node, dict):
            return

        node_type = node.get('type', '')
        href = node.get('href', '')
        title = node.get('label', '') or node.get('title', '')

        if node_type == 'page' and href:
            # href is like "/en/user-guide/snowflake-horizon" — extract slug
            slug = href.lstrip('/')
            # Remove leading "en/" prefix for the slug
            if slug.startswith('en/'):
                slug = slug[3:]
            url = f'{self.BASE_URL}{href}'
            pages.append({
                'slug': slug,
                'url': url,
                'title': title,
                'parent_path': parent_path,
            })
            next_parent = slug
        else:
            next_parent = parent_path

        # Walk children
        children = node.get('children', [])
        if isinstance(children, list):
            for child in children:
                self._walk_tree(child, pages, next_parent)

    def discover_pages(self):
        """Discover all Snowflake documentation pages via Next.js JSON endpoints"""
        print(f"Discovering Snowflake documentation pages...")
        print(f"  Extracting buildId...")
        build_id = self._get_build_id()
        print(f"  buildId: {build_id}")

        all_pages = []

        for section in self.SECTION_ROOTS:
            print(f"  Fetching section: {section}...")
            data = self._fetch_section_json(section)
            if not data:
                continue

            page_props = data.get('pageProps', {})

            # Walk the tree structure
            tree = page_props.get('tree', {})
            if tree:
                section_pages = []
                self._walk_tree(tree, section_pages)
                print(f"    Found {len(section_pages)} pages in {section}")
                all_pages.extend(section_pages)

            # The section root page is typically already in the tree as the first child
            # No need to add it separately

        # Deduplicate by slug
        seen = set()
        unique_pages = []
        for page in all_pages:
            if page['slug'] not in seen:
                seen.add(page['slug'])
                unique_pages.append(page)

        print(f"  Total unique pages discovered: {len(unique_pages)}")
        return unique_pages

    def fetch_page_content(self, url, slug, retries=3):
        """Fetch a Snowflake doc page content via Next.js JSON endpoint"""
        try:
            data = self._fetch_section_json(slug, retries=retries)
            if not data:
                return {
                    'error': True,
                    'reason': 'fetch_failed',
                    'detail': f'Failed to fetch JSON for {slug}',
                }

            page_props = data.get('pageProps') or {}
            title = page_props.get('title', '')
            content_html = page_props.get('content', '')

            if not content_html:
                # Try fetching as regular HTML page
                return self._fetch_page_html(url, retries)

            # Convert HTML content to markdown
            content = self._html_to_markdown(content_html)

            if not content or len(content) < 20:
                return {
                    'error': True,
                    'reason': 'no_content',
                    'detail': f'Content too short ({len(content) if content else 0} chars) for {slug}',
                }

            return {
                'title': title,
                'content': content,
            }

        except Exception as e:
            return {
                'error': True,
                'reason': 'parse_error',
                'detail': f'{type(e).__name__}: {e}',
            }

    def _fetch_page_html(self, url, retries=3):
        """Fallback: fetch page as HTML and extract content"""
        for attempt in range(retries):
            try:
                resp = self.session.get(url, timeout=30)
                resp.raise_for_status()
                soup = BeautifulSoup(resp.text, 'html.parser')

                title = None
                og_title = soup.find('meta', property='og:title')
                if og_title:
                    title = og_title.get('content', '').strip()
                if not title:
                    title_tag = soup.find('title')
                    title = title_tag.get_text(strip=True) if title_tag else None

                article = (
                    soup.find('article') or
                    soup.find('main') or
                    soup.find('div', class_=re.compile(r'content', re.I)) or
                    soup.find('div', role='main')
                )

                if not article:
                    return {
                        'error': True,
                        'reason': 'no_content',
                        'detail': f'No content element found at {url}',
                    }

                content = self._html_to_markdown(str(article))
                if not content or len(content) < 20:
                    return {
                        'error': True,
                        'reason': 'no_content',
                        'detail': f'Content too short for {url}',
                    }

                return {'title': title, 'content': content}

            except requests.RequestException as e:
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    return {
                        'error': True,
                        'reason': f'http_error',
                        'detail': str(e),
                    }
        return {'error': True, 'reason': 'unknown', 'detail': 'All retries exhausted'}


# =============================================================================
# Soda Scraper
# =============================================================================

@register_site('soda')
class SodaScraper(BaseSiteScraper):
    """Scraper for docs.soda.io using sitemap + HTML scraping"""

    SITE_KEY = 'soda'
    SITE_LABEL = 'Soda'
    BASE_URL = 'https://docs.soda.io'

    def discover_pages(self):
        """Discover all Soda documentation pages via sitemap"""
        print(f"Discovering Soda documentation pages...")

        # Fetch sitemap index
        sitemap_url = f'{self.BASE_URL}/sitemap.xml'
        print(f"  Fetching sitemap index: {sitemap_url}")

        try:
            resp = self.session.get(sitemap_url, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"  Error fetching sitemap: {e}")
            return []

        # Parse sitemap index to find child sitemaps
        root = ElementTree.fromstring(resp.content)
        ns = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}

        child_sitemaps = []
        for sitemap in root.findall('sm:sitemap', ns):
            loc = sitemap.find('sm:loc', ns)
            if loc is not None and loc.text:
                child_sitemaps.append(loc.text)

        if not child_sitemaps:
            # Maybe it's a direct sitemap with <url> entries
            urls = self._parse_sitemap_urls(root, ns)
            if urls:
                print(f"  Found {len(urls)} pages directly in sitemap")
                return urls
            print("  No child sitemaps or URLs found")
            return []

        print(f"  Found {len(child_sitemaps)} child sitemaps")

        # Fetch each child sitemap
        all_pages = []
        for child_url in child_sitemaps:
            print(f"  Fetching: {child_url}")
            try:
                resp = self.session.get(child_url, timeout=30)
                resp.raise_for_status()
                child_root = ElementTree.fromstring(resp.content)
                pages = self._parse_sitemap_urls(child_root, ns)
                print(f"    Found {len(pages)} pages")
                all_pages.extend(pages)
            except Exception as e:
                print(f"    Error: {e}")

        # Deduplicate
        seen = set()
        unique_pages = []
        for page in all_pages:
            if page['slug'] not in seen:
                seen.add(page['slug'])
                unique_pages.append(page)

        print(f"  Total unique pages discovered: {len(unique_pages)}")
        return unique_pages

    def _parse_sitemap_urls(self, root, ns):
        """Parse <url> entries from a sitemap XML"""
        pages = []
        for url_elem in root.findall('sm:url', ns):
            loc = url_elem.find('sm:loc', ns)
            if loc is not None and loc.text:
                url = loc.text.strip()
                parsed = urlparse(url)
                slug = parsed.path.strip('/')
                if not slug:
                    slug = 'index'

                # Derive parent_path
                parts = slug.split('/')
                parent_path = '/'.join(parts[:-1]) if len(parts) > 1 else ''

                pages.append({
                    'slug': slug,
                    'url': url,
                    'title': None,  # Will be extracted during scraping
                    'parent_path': parent_path,
                })
        return pages

    def fetch_page_content(self, url, slug, retries=3):
        """Fetch a Soda doc page and extract content as markdown"""
        for attempt in range(retries):
            try:
                resp = self.session.get(url, timeout=30)
                resp.raise_for_status()

                soup = BeautifulSoup(resp.text, 'html.parser')

                # Extract title
                title = None
                og_title = soup.find('meta', property='og:title')
                if og_title:
                    title = og_title.get('content', '').strip()
                if not title:
                    h1 = soup.find('h1')
                    title = h1.get_text(strip=True) if h1 else None
                if not title:
                    title_tag = soup.find('title')
                    title = title_tag.get_text(strip=True) if title_tag else slug

                # Extract main content (GitBook structure)
                article = (
                    soup.find('article') or
                    soup.find('main') or
                    soup.find('div', class_=re.compile(r'page-body|content|markdown', re.I)) or
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
                        'detail': f'No content element found at {url}',
                    }

                content = self._html_to_markdown(str(article))

                if not content or len(content) < 20:
                    return {
                        'error': True,
                        'reason': 'no_content',
                        'detail': f'Content too short ({len(content) if content else 0} chars) for {slug}',
                    }

                return {
                    'title': title,
                    'content': content,
                }

            except requests.exceptions.HTTPError as e:
                status_code = e.response.status_code if e.response is not None else 0
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    return {
                        'error': True,
                        'reason': f'http_{status_code}',
                        'detail': str(e),
                    }

            except requests.exceptions.RequestException as e:
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    return {
                        'error': True,
                        'reason': 'request_error',
                        'detail': str(e),
                    }

            except Exception as e:
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    return {
                        'error': True,
                        'reason': 'parse_error',
                        'detail': f'{type(e).__name__}: {e}',
                    }

        return {'error': True, 'reason': 'unknown', 'detail': 'All retries exhausted'}


# =============================================================================
# AWS Scraper
# =============================================================================

@register_site('aws')
class AWSScraper(BaseSiteScraper):
    """Scraper for docs.aws.amazon.com using sitemap + HTML scraping"""

    SITE_KEY = 'aws'
    SITE_LABEL = 'AWS'
    BASE_URL = 'https://docs.aws.amazon.com'
    SITEMAP_INDEX = 'https://docs.aws.amazon.com/sitemap_index.xml'

    # Regex: locale prefix like "de", "fr", "de_de", "ja_jp", "zh_cn", etc.
    _LOCALE_RE = re.compile(r'^[a-z]{2}(_[a-z]{2})?$')

    def __init__(self):
        super().__init__()
        config = Config()
        config.load()
        self.paths = config.aws_paths  # List of path prefixes to filter

    def _is_localized_url(self, url):
        """Return True if the URL starts with a locale prefix (e.g. /de_de/, /ja_jp/)"""
        path = urlparse(url).path.strip('/')
        first = path.split('/')[0].lower() if path else ''
        return bool(self._LOCALE_RE.match(first))

    def _is_english_url(self, url):
        """Return False for non-English pages (language-prefixed paths)"""
        return not self._is_localized_url(url)

    def _parse_sitemap_urls(self, root, ns):
        """Parse <url> entries from a sitemap XML, English only"""
        pages = []
        for url_elem in root.findall('sm:url', ns):
            loc = url_elem.find('sm:loc', ns)
            if loc is None or not loc.text:
                continue
            url = loc.text.strip()
            if not self._is_english_url(url):
                continue
            slug = urlparse(url).path.strip('/')
            if slug.endswith('.html'):
                slug = slug[:-5]
            if not slug:
                slug = 'index'
            parts = slug.split('/')
            parent_path = '/'.join(parts[:-1]) if len(parts) > 1 else ''
            pages.append({
                'slug': slug,
                'url': url,
                'title': None,
                'parent_path': parent_path,
            })
        return pages

    def discover_pages(self):
        """Discover AWS docs pages via sitemap index, filtered by aws_paths config"""
        print(f"Discovering AWS documentation pages...")

        if not self.paths:
            print("  Warning: aws_paths is empty — this will discover ALL AWS docs (100k+ pages).")
            print("  Set aws_paths in config.local.toml to limit to specific services.")
            print("  Example: aws_paths = [\"AmazonS3/latest/userguide\", \"lambda/latest/dg\"]")

        print(f"  Fetching sitemap index...")
        try:
            resp = self.session.get(self.SITEMAP_INDEX, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"  Error fetching sitemap index: {e}")
            return []

        root = ElementTree.fromstring(resp.content)
        ns = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}

        all_sitemaps = [
            loc.text.strip()
            for sitemap in root.findall('sm:sitemap', ns)
            for loc in [sitemap.find('sm:loc', ns)]
            if loc is not None and loc.text
        ]
        print(f"  Found {len(all_sitemaps)} service sitemaps")

        # Always skip localized sitemaps (non-English)
        all_sitemaps = [sm for sm in all_sitemaps if not self._is_localized_url(sm)]

        # Filter by configured paths if provided
        if self.paths:
            filtered = [
                sm for sm in all_sitemaps
                if any(p.strip('/').lower() in sm.lower() for p in self.paths)
            ]
            print(f"  Filtered to {len(filtered)} sitemaps matching aws_paths")
            all_sitemaps = filtered

        if not all_sitemaps:
            print("  No matching sitemaps found. Check your aws_paths config.")
            return []

        # Fetch each service sitemap
        all_pages = []
        for sm_url in all_sitemaps:
            print(f"  Fetching: {sm_url}")
            try:
                resp = self.session.get(sm_url, timeout=30)
                resp.raise_for_status()
                child_root = ElementTree.fromstring(resp.content)
                pages = self._parse_sitemap_urls(child_root, ns)
                print(f"    Found {len(pages)} English pages")
                all_pages.extend(pages)
            except Exception as e:
                print(f"    Error: {e}")

        # Deduplicate by slug
        seen = set()
        unique_pages = []
        for page in all_pages:
            if page['slug'] not in seen:
                seen.add(page['slug'])
                unique_pages.append(page)

        print(f"  Total unique pages discovered: {len(unique_pages)}")
        return unique_pages

    def fetch_page_content(self, url, slug, retries=3):
        """Fetch an AWS docs page and extract content as markdown"""
        for attempt in range(retries):
            try:
                resp = self.session.get(url, timeout=30)
                resp.raise_for_status()

                soup = BeautifulSoup(resp.text, 'html.parser')

                # Extract title: prefer h1.topictitle, fall back to meta/title tag
                title = None
                h1 = soup.find('h1', class_='topictitle')
                if h1:
                    title = h1.get_text(strip=True)
                if not title:
                    og = soup.find('meta', attrs={'name': 'description'})
                    meta_title = soup.find('meta', property='og:title')
                    if meta_title:
                        title = meta_title.get('content', '').strip()
                if not title:
                    title_tag = soup.find('title')
                    title = title_tag.get_text(strip=True) if title_tag else slug.split('/')[-1]

                # Find main content area
                article = (
                    soup.find(id='main-col-body') or
                    soup.find(id='main-content') or
                    soup.find(id='main-column') or
                    soup.find('main') or
                    soup.find('article')
                )

                if not article:
                    article = soup.find('body')
                    if article:
                        for tag in article.find_all(['nav', 'header', 'aside', 'script', 'style']):
                            tag.decompose()

                if not article:
                    return {
                        'error': True,
                        'reason': 'no_content',
                        'detail': f'No content element found at {url}',
                    }

                # Strip AWS-specific navigation/chrome elements
                for el_id in ['guide-toc', 'breadcrumbs', 'page-toc-src',
                               'left-column', 'right-column', 'doc-history-btn',
                               'feedback-section', 'quick-feedback']:
                    el = article.find(id=el_id)
                    if el:
                        el.decompose()

                for cls in ['page-toc-container', 'prev-next', 'doc-history',
                            'awsdocs-filter-selector', 'awsdocs-language-banner']:
                    for el in article.find_all(class_=cls):
                        el.decompose()

                content = self._html_to_markdown(str(article))

                if not content or len(content) < 20:
                    return {
                        'error': True,
                        'reason': 'no_content',
                        'detail': f'Content too short ({len(content) if content else 0} chars) for {slug}',
                    }

                return {'title': title, 'content': content}

            except requests.exceptions.HTTPError as e:
                status_code = e.response.status_code if e.response is not None else 0
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    return {'error': True, 'reason': f'http_{status_code}', 'detail': str(e)}

            except requests.exceptions.RequestException as e:
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    return {'error': True, 'reason': 'request_error', 'detail': str(e)}

            except Exception as e:
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    return {'error': True, 'reason': 'parse_error', 'detail': f'{type(e).__name__}: {e}'}

        return {'error': True, 'reason': 'unknown', 'detail': 'All retries exhausted'}


# =============================================================================
# Databricks Docs Scraper
# =============================================================================

@register_site('databricks')
class DatabricksDocsScraper(BaseSiteScraper):
    """Scraper for docs.databricks.com using sitemap + Docusaurus HTML"""

    SITE_KEY = 'databricks'
    SITE_LABEL = 'Databricks Docs'
    BASE_URL = 'https://docs.databricks.com'

    _CLOUD_MAP = {'aws': 'aws', 'gcp': 'gcp', 'azure': 'azure'}

    def __init__(self):
        super().__init__()
        config = Config()
        config.load()
        # e.g. ["aws/en", "gcp/en"] — defaults to aws/en only
        self.paths = config.databricks_paths or ['aws/en']

    def extra_tags(self, page):
        """Add cloud-platform tag (aws/gcp/azure) derived from the slug prefix."""
        slug = page.get('slug', '')
        cloud = slug.split('/')[0].lower() if slug else ''
        return [self._CLOUD_MAP[cloud]] if cloud in self._CLOUD_MAP else []

    def _parse_sitemap_urls(self, root, ns):
        """Parse <url> entries from a sitemap XML"""
        pages = []
        for url_elem in root.findall('sm:url', ns):
            loc = url_elem.find('sm:loc', ns)
            if loc is None or not loc.text:
                continue
            url = loc.text.strip()
            slug = urlparse(url).path.strip('/')
            if not slug:
                slug = 'index'
            parts = slug.split('/')
            parent_path = '/'.join(parts[:-1]) if len(parts) > 1 else ''
            pages.append({
                'slug': slug,
                'url': url,
                'title': None,
                'parent_path': parent_path,
            })
        return pages

    def discover_pages(self):
        """Discover Databricks docs pages via per-path sitemaps"""
        print(f"Discovering Databricks documentation pages (paths: {self.paths})...")

        ns = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
        all_pages = []

        for path in self.paths:
            path = path.strip('/')
            sitemap_url = f'{self.BASE_URL}/{path}/sitemap.xml'
            print(f"  Fetching: {sitemap_url}")
            try:
                resp = self.session.get(sitemap_url, timeout=30)
                resp.raise_for_status()
                root = ElementTree.fromstring(resp.content)

                # Check if it's a sitemap index (has <sitemap> children)
                child_sitemaps = root.findall('sm:sitemap', ns)
                if child_sitemaps:
                    for sitemap in child_sitemaps:
                        loc = sitemap.find('sm:loc', ns)
                        if loc is None or not loc.text:
                            continue
                        child_url = loc.text.strip()
                        print(f"    Fetching child: {child_url}")
                        try:
                            child_resp = self.session.get(child_url, timeout=30)
                            child_resp.raise_for_status()
                            child_root = ElementTree.fromstring(child_resp.content)
                            pages = self._parse_sitemap_urls(child_root, ns)
                            print(f"      Found {len(pages)} pages")
                            all_pages.extend(pages)
                        except Exception as e:
                            print(f"      Error: {e}")
                else:
                    pages = self._parse_sitemap_urls(root, ns)
                    print(f"    Found {len(pages)} pages")
                    all_pages.extend(pages)

            except Exception as e:
                print(f"    Error fetching {sitemap_url}: {e}")

        # Deduplicate by slug
        seen = set()
        unique_pages = []
        for page in all_pages:
            if page['slug'] not in seen:
                seen.add(page['slug'])
                unique_pages.append(page)

        print(f"  Total unique pages discovered: {len(unique_pages)}")
        return unique_pages

    def fetch_page_content(self, url, slug, retries=3):
        """Fetch a Databricks docs page and extract content as markdown"""
        for attempt in range(retries):
            try:
                resp = self.session.get(url, timeout=30)
                resp.raise_for_status()

                soup = BeautifulSoup(resp.text, 'html.parser')

                # Extract title from first h1, fall back to og:title / <title>
                title = None
                h1 = soup.find('h1')
                if h1:
                    title = h1.get_text(strip=True)
                if not title:
                    og = soup.find('meta', property='og:title')
                    if og:
                        title = og.get('content', '').strip()
                if not title:
                    title_tag = soup.find('title')
                    if title_tag:
                        # Docusaurus titles are like "Page Title | Databricks"
                        title = title_tag.get_text(strip=True).split('|')[0].strip()
                if not title:
                    title = slug.split('/')[-1]

                # Find main article content (Docusaurus uses <article> inside <main>)
                article = (
                    soup.find('article') or
                    soup.find('main') or
                    soup.find(class_=re.compile(r'docMainContainer|markdown', re.I))
                )

                if not article:
                    article = soup.find('body')
                    if article:
                        for tag in article.find_all(['nav', 'header', 'footer',
                                                     'aside', 'script', 'style']):
                            tag.decompose()

                if not article:
                    return {
                        'error': True,
                        'reason': 'no_content',
                        'detail': f'No content element found at {url}',
                    }

                # Strip Docusaurus navigation chrome
                for tag in article.find_all(['nav', 'footer', 'aside',
                                             'script', 'style', 'noscript']):
                    tag.decompose()

                for cls_pattern in [
                    'theme-doc-toc', 'pagination-nav', 'breadcrumbs',
                    'theme-last-updated', 'docusaurus-mt-lg', 'col--3',
                ]:
                    for el in article.find_all(class_=re.compile(cls_pattern, re.I)):
                        el.decompose()

                content = self._html_to_markdown(str(article))

                if not content or len(content) < 20:
                    return {
                        'error': True,
                        'reason': 'no_content',
                        'detail': f'Content too short ({len(content) if content else 0} chars) for {slug}',
                    }

                return {'title': title, 'content': content}

            except requests.exceptions.HTTPError as e:
                status_code = e.response.status_code if e.response is not None else 0
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    return {'error': True, 'reason': f'http_{status_code}', 'detail': str(e)}

            except requests.exceptions.RequestException as e:
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    return {'error': True, 'reason': 'request_error', 'detail': str(e)}

            except Exception as e:
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    return {'error': True, 'reason': 'parse_error',
                            'detail': f'{type(e).__name__}: {e}'}

        return {'error': True, 'reason': 'unknown', 'detail': 'All retries exhausted'}


# =============================================================================
# Claude Code Docs Scraper
# =============================================================================

@register_site('claude-code')
class ClaudeCodeScraper(BaseSiteScraper):
    """Scraper for code.claude.com/docs/en using sitemap + raw .md endpoints"""

    SITE_KEY = 'claude-code'
    SITE_LABEL = 'Claude Code Docs'
    BASE_URL = 'https://code.claude.com/docs'
    SITEMAP_URL = 'https://code.claude.com/docs/sitemap.xml'
    LANG = 'en'

    def discover_pages(self):
        """Discover all English pages from sitemap + llms.txt (for completeness)"""
        print(f"Discovering Claude Code documentation pages ({self.LANG})...")

        seen = set()
        pages = []
        lang_prefix = f'{self.BASE_URL}/{self.LANG}/'

        # --- Primary: sitemap ---
        print(f"  Fetching sitemap: {self.SITEMAP_URL}")
        try:
            resp = self.session.get(self.SITEMAP_URL, timeout=30)
            resp.raise_for_status()
            root = ElementTree.fromstring(resp.content)
            ns = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
            for url_elem in root.findall('sm:url', ns):
                loc = url_elem.find('sm:loc', ns)
                if loc is None or not loc.text:
                    continue
                page_url = loc.text.strip()
                if not page_url.startswith(lang_prefix):
                    continue
                if page_url in seen:
                    continue
                seen.add(page_url)
                slug = page_url.replace(self.BASE_URL + '/', '').rstrip('/')
                pages.append({
                    'slug': slug,
                    'url': page_url + '.md',
                    'title': None,
                    'parent_path': self.LANG,
                })
            print(f"  Sitemap: {len(pages)} {self.LANG} pages")
        except requests.RequestException as e:
            print(f"  Sitemap error: {e}")

        # --- Supplemental: llms.txt (catches pages absent from sitemap, e.g. changelog) ---
        llms_url = f'{self.BASE_URL}/llms.txt'
        print(f"  Fetching llms.txt: {llms_url}")
        try:
            resp = self.session.get(llms_url, timeout=30)
            resp.raise_for_status()
            link_re = re.compile(r'\[([^\]]+)\]\((https://[^\)]+\.md)\)')
            extra = 0
            for line in resp.text.splitlines():
                for title, md_url in link_re.findall(line):
                    if not md_url.startswith(lang_prefix):
                        continue
                    # Normalise: strip .md to get canonical URL for dedup
                    canonical = md_url[:-3] if md_url.endswith('.md') else md_url
                    if canonical in seen:
                        continue
                    seen.add(canonical)
                    slug = canonical.replace(self.BASE_URL + '/', '').rstrip('/')
                    pages.append({
                        'slug': slug,
                        'url': md_url,
                        'title': title,
                        'parent_path': self.LANG,
                    })
                    extra += 1
            if extra:
                print(f"  llms.txt: +{extra} extra pages not in sitemap")
        except requests.RequestException as e:
            print(f"  llms.txt error (non-fatal): {e}")

        print(f"  Total: {len(pages)} pages")
        return pages

    def fetch_page_content(self, url, slug, retries=3):
        """Fetch raw markdown from .md URL and clean up MDX components"""
        for attempt in range(retries):
            try:
                resp = self.session.get(url, timeout=30)
                resp.raise_for_status()

                raw = resp.text

                # Strip the llms.txt preamble block that appears at the top of each page
                # Format: "> ## Documentation Index\n> Fetch ...\n"
                raw = re.sub(r'^>\s*##\s*Documentation Index.*?(?=\n#|\Z)', '', raw,
                             flags=re.DOTALL)

                # Convert callout components to Obsidian callout syntax
                for tag, callout_type in [('Info', 'NOTE'), ('Tip', 'TIP'),
                                          ('Warning', 'WARNING'), ('Caution', 'WARNING')]:
                    raw = re.sub(
                        rf'<{tag}>\s*(.*?)\s*</{tag}>',
                        lambda m, ct=callout_type: '> [!' + ct + ']\n' + '\n'.join(
                            '> ' + l for l in m.group(1).strip().splitlines()
                        ),
                        raw, flags=re.DOTALL
                    )

                # Convert <Accordion title="X"> content </Accordion> to ### X\ncontent
                raw = re.sub(
                    r'<Accordion\s+[^>]*title="([^"]+)"[^>]*>\s*(.*?)\s*</Accordion>',
                    lambda m: f'### {m.group(1)}\n\n{m.group(2).strip()}',
                    raw, flags=re.DOTALL
                )

                # Convert <Tab title="X"> content </Tab> to #### X\ncontent
                raw = re.sub(
                    r'<Tab\s+[^>]*title="([^"]+)"[^>]*>\s*(.*?)\s*</Tab>',
                    lambda m: f'#### {m.group(1)}\n\n{m.group(2).strip()}',
                    raw, flags=re.DOTALL
                )

                # Strip remaining MDX container tags (keep inner content)
                for tag in ['Tabs', 'AccordionGroup', 'CardGroup', 'Card', 'Steps',
                            'Step', 'Note', 'Frame']:
                    raw = re.sub(rf'</?{tag}(?:\s[^>]*)?\s*/?>', '', raw)

                # Strip any remaining unknown JSX-style tags (single-line self-closing or open/close)
                raw = re.sub(r'<[A-Z][A-Za-z]*(?:\s[^>]*)?\s*/>', '', raw)
                raw = re.sub(r'</[A-Z][A-Za-z]*>', '', raw)
                raw = re.sub(r'<[A-Z][A-Za-z]*(?:\s[^>]*)?>', '', raw)

                # Collapse excessive blank lines
                raw = re.sub(r'\n{3,}', '\n\n', raw)
                content = raw.strip()

                if not content or len(content) < 20:
                    return {
                        'error': True,
                        'reason': 'no_content',
                        'detail': f'Content too short ({len(content)} chars) for {slug}',
                    }

                # Extract title from first # heading
                title = None
                for line in content.splitlines():
                    line = line.strip()
                    if line.startswith('# '):
                        title = line[2:].strip()
                        break
                if not title:
                    title = slug.split('/')[-1].replace('-', ' ').title()

                return {'title': title, 'content': content}

            except requests.exceptions.HTTPError as e:
                status_code = e.response.status_code if e.response is not None else 0
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    return {
                        'error': True,
                        'reason': f'http_{status_code}',
                        'detail': str(e),
                    }

            except requests.exceptions.RequestException as e:
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    return {
                        'error': True,
                        'reason': 'request_error',
                        'detail': str(e),
                    }

        return {'error': True, 'reason': 'unknown', 'detail': 'All retries exhausted'}


# =============================================================================
# Docs Manager (Orchestrator)
# =============================================================================

class DocsManager:
    """Orchestrates documentation scraping operations"""

    def __init__(self, config=None):
        self.config = config or Config()
        self.config.load()
        self.db = DocsDatabase(self.config.database_path)
        self._shutdown = False

    def _get_scrapers(self, site=None):
        """Get scraper instances, optionally filtered by site key"""
        if site:
            if site not in SITE_SCRAPERS:
                print(f"Error: Unknown site '{site}'")
                print(f"Available sites: {', '.join(SITE_SCRAPERS.keys())}")
                return {}
            return {site: SITE_SCRAPERS[site]()}
        return {key: cls() for key, cls in SITE_SCRAPERS.items()}

    def _setup_signal_handlers(self):
        """Setup graceful shutdown on Ctrl+C"""
        def handler(signum, frame):
            if self._shutdown:
                print("\nForce quit.")
                sys.exit(1)
            print("\nShutting down gracefully (press Ctrl+C again to force)...")
            self._shutdown = True
        signal.signal(signal.SIGINT, handler)

    def discover(self, site=None):
        """Discover documentation pages from configured sites"""
        scrapers = self._get_scrapers(site)
        if not scrapers:
            return

        with self.db:
            for site_key, scraper in scrapers.items():
                pages = scraper.discover_pages()
                if not pages:
                    print(f"No pages discovered for {scraper.SITE_LABEL}")
                    continue

                new_count = 0
                for page in pages:
                    added = self.db.add_page(
                        site=site_key,
                        slug=page['slug'],
                        url=page['url'],
                        title=page.get('title'),
                        parent_path=page.get('parent_path', ''),
                    )
                    if added:
                        new_count += 1

                counts = self.db.get_counts(site=site_key)
                self.db.add_sync_log(site_key, len(pages), new_count, 0, 0, 'discovered')

                print(f"\n{scraper.SITE_LABEL}: {new_count} new pages added (of {len(pages)} discovered)")
                print(f"  Total in database: {counts.get('total', 0)}")
                print(f"  Pending: {counts.get('pending', 0)}")
                print(f"  Downloaded: {counts.get('downloaded', 0)}")
                print(f"  Failed: {counts.get('failed', 0)}")

    def scrape(self, site=None, limit=None, slug=None, parallel=False):
        """Scrape documentation pages"""
        self._setup_signal_handlers()

        scrapers = self._get_scrapers(site)
        if not scrapers:
            return

        with self.db:
            for site_key, scraper in scrapers.items():
                if self._shutdown:
                    break

                pages = self.db.get_pages(site=site_key, status='pending', limit=limit, slug=slug)

                if not pages:
                    print(f"No pending pages for {scraper.SITE_LABEL}.")
                    continue

                if parallel and not slug:
                    self._scrape_parallel(pages, scraper, site_key)
                else:
                    self._scrape_sequential(pages, scraper, site_key)

    def _scrape_sequential(self, pages, scraper, site_key):
        """Scrape pages one at a time"""
        print(f"\nScraping {len(pages)} {scraper.SITE_LABEL} pages (sequential)")
        print(f"Delay between requests: {self.config.delay}s")
        print()

        successful = 0
        failed = 0

        for i, page in enumerate(pages):
            if self._shutdown:
                print("\nStopping due to shutdown request.")
                break

            title_display = (page.get('title') or page['slug'])[:60]
            print(f"[{i+1}/{len(pages)}] {title_display}...", end=" ", flush=True)

            result = scraper.fetch_page_content(
                page['url'], page['slug'], retries=self.config.max_retries
            )

            if result and not result.get('error') and result.get('content'):
                filepath = self._save_page(page, result, scraper)

                word_count = len(result['content'].split())
                char_count = len(result['content'])

                self.db.update_page(
                    site_key, page['slug'],
                    title=result.get('title') or page.get('title'),
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
                self.db.update_page(
                    site_key, page['slug'],
                    status='failed',
                    error_reason=error_reason,
                    error_detail=error_detail,
                )
                print(f"FAILED ({error_reason})")
                failed += 1

            if i < len(pages) - 1 and not self._shutdown:
                time.sleep(self.config.delay)

        self.db.add_sync_log(site_key, 0, 0, successful, failed,
                             'success' if failed == 0 else 'partial')
        print(f"\n{scraper.SITE_LABEL}: {successful} downloaded, {failed} failed")

    def _scrape_parallel(self, pages, scraper, site_key):
        """Scrape pages in parallel using ThreadPoolExecutor"""
        max_workers = self.config.max_workers
        delay = self.config.delay

        print(f"\nScraping {len(pages)} {scraper.SITE_LABEL} pages (parallel, {max_workers} workers)")
        print(f"Delay between requests: {delay}s per worker")
        print()

        successful = 0
        failed = 0
        counter_lock = threading.Lock()
        progress_lock = threading.Lock()
        completed = 0

        def _fetch_page(page):
            nonlocal completed

            if self._shutdown:
                return None

            result = scraper.fetch_page_content(
                page['url'], page['slug'], retries=self.config.max_retries
            )

            with counter_lock:
                completed += 1
                idx = completed

            title_display = (page.get('title') or page['slug'])[:60]

            if result and not result.get('error') and result.get('content'):
                filepath = self._save_page(page, result, scraper)
                word_count = len(result['content'].split())
                char_count = len(result['content'])

                with progress_lock:
                    print(f"[{idx}/{len(pages)}] {title_display}... OK ({word_count} words)")

                time.sleep(delay)
                return {
                    'site': site_key,
                    'slug': page['slug'],
                    'success': True,
                    'result': result,
                    'title': page.get('title'),
                    'filepath': filepath,
                    'word_count': word_count,
                    'char_count': char_count,
                }
            else:
                error_reason = result.get('reason', 'unknown') if result else 'unknown'
                with progress_lock:
                    print(f"[{idx}/{len(pages)}] {title_display}... FAILED ({error_reason})")

                time.sleep(delay)
                return {
                    'site': site_key,
                    'slug': page['slug'],
                    'success': False,
                    'error_reason': error_reason,
                    'error_detail': result.get('detail', '') if result else '',
                }

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            for page in pages:
                if self._shutdown:
                    break
                future = executor.submit(_fetch_page, page)
                futures[future] = page

            for future in as_completed(futures):
                if self._shutdown:
                    executor.shutdown(wait=False, cancel_futures=True)
                    break

                outcome = future.result()
                if outcome is None:
                    continue

                if outcome['success']:
                    r = outcome['result']
                    self.db.update_page(
                        outcome['site'], outcome['slug'],
                        title=r.get('title') or outcome.get('title'),
                        word_count=outcome['word_count'],
                        char_count=outcome['char_count'],
                        file_path=outcome['filepath'],
                        status='downloaded',
                        downloaded_at=datetime.now(),
                    )
                    successful += 1
                else:
                    self.db.update_page(
                        outcome['site'], outcome['slug'],
                        status='failed',
                        error_reason=outcome.get('error_reason', 'unknown'),
                        error_detail=outcome.get('error_detail', ''),
                    )
                    failed += 1

        self.db.add_sync_log(site_key, 0, 0, successful, failed,
                             'success' if failed == 0 else 'partial')
        print(f"\n{scraper.SITE_LABEL}: {successful} downloaded, {failed} failed")

    def _save_page(self, page, result, scraper):
        """Save a documentation page as Obsidian markdown"""
        slug = page['slug']

        # Build file path mirroring URL hierarchy
        # e.g. docs_obsidian/Snowflake/en/guides/data-loading.md
        site_dir = Path(self.config.staging_dir) / scraper.SITE_LABEL

        # Use slug as directory structure
        slug_parts = slug.split('/')
        if len(slug_parts) > 1:
            subdir = '/'.join(slug_parts[:-1])
            filename = slug_parts[-1]
        else:
            subdir = ''
            filename = slug

        # Clean filename
        safe_filename = re.sub(r'[<>:"|?*]', '', filename)
        safe_filename = re.sub(r'\s+', ' ', safe_filename).strip()
        if not safe_filename:
            safe_filename = 'index'

        if subdir:
            output_dir = site_dir / subdir
        else:
            output_dir = site_dir

        output_dir.mkdir(parents=True, exist_ok=True)
        filepath = output_dir / f"{safe_filename}.md"

        # Build frontmatter
        title = result.get('title') or page.get('title') or slug
        yaml_title = title.replace('"', '\\"')
        url = page.get('url', '')
        today = datetime.now().strftime('%Y-%m-%d')

        tags = ['clippings', 'documentation', scraper.SITE_KEY] + scraper.extra_tags(page)

        fm = []
        fm.append('---')
        fm.append(f'title: "{yaml_title}"')
        fm.append(f'source: "{url}"')
        fm.append(f'created: {today}')
        fm.append('tags:')
        for tag in tags:
            fm.append(f'  - "{tag}"')
        fm.append('---')

        body_parts = ['\n'.join(fm)]
        body_parts.append('')
        body_parts.append(result['content'])
        body_parts.append('')

        with open(filepath, 'w', encoding='utf-8') as f:
            f.write('\n'.join(body_parts))

        return str(filepath)

    def list_pages(self, site=None, status=None, json_output=False):
        """List documentation pages"""
        with self.db:
            pages = self.db.get_pages(site=site, status=status)

        if json_output:
            print(json.dumps(pages, indent=2, default=str))
            return pages

        if not pages:
            filters = []
            if site:
                filters.append(f'site={site}')
            if status:
                filters.append(f'status={status}')
            filter_str = f" ({', '.join(filters)})" if filters else ''
            print(f"No pages found{filter_str}.")
            return []

        print(f"\n{'ID':<6} {'Site':<12} {'Status':<12} {'Slug'}")
        print('-' * 90)
        for p in pages:
            slug_display = (p.get('slug') or '')[:55]
            print(f"{p['id']:<6} {p['site']:<12} {p['status']:<12} {slug_display}")
        print(f"\nTotal: {len(pages)}")
        return pages

    def show_status(self, site=None):
        """Show summary statistics"""
        scrapers = self._get_scrapers(site)
        sites_to_show = list(scrapers.keys()) if scrapers else list(SITE_SCRAPERS.keys())

        with self.db:
            print(f"\nDocumentation Scraper Status")
            print(f"{'='*50}")

            for site_key in sites_to_show:
                counts = self.db.get_counts(site=site_key)
                label = SITE_SCRAPERS[site_key].SITE_LABEL if site_key in SITE_SCRAPERS else site_key
                total = counts.get('total', 0)
                pending = counts.get('pending', 0)
                downloaded = counts.get('downloaded', 0)
                failed = counts.get('failed', 0)

                print(f"\n{label}:")
                print(f"  Total pages:   {total}")
                print(f"  Pending:       {pending}")
                print(f"  Downloaded:    {downloaded}")
                print(f"  Failed:        {failed}")

                if total > 0:
                    pct = (downloaded / total) * 100
                    print(f"  Progress:      {pct:.1f}%")

            # Overall totals
            all_counts = self.db.get_counts()
            total = all_counts.get('total', 0)
            if total > 0 and len(sites_to_show) > 1:
                print(f"\nOverall: {all_counts.get('downloaded', 0)}/{total} downloaded "
                      f"({all_counts.get('downloaded', 0)/total*100:.1f}%)")
            print()

    def move_to_obsidian(self, all_pages=False, site=None):
        """Move downloaded pages to Obsidian vault"""
        obsidian_path = self.config.obsidian_vault
        if not obsidian_path:
            print("Error: Obsidian vault path not configured")
            print("Run: scrape_docs.py config set obsidian_vault /path/to/vault")
            return

        obsidian_path = Path(obsidian_path).expanduser()
        if not obsidian_path.exists():
            print(f"Error: Obsidian vault path does not exist: {obsidian_path}")
            return

        with self.db:
            pages = self.db.get_unmoved_pages(site=site)

            if not pages:
                print("No pages to move (all already in Obsidian or none downloaded).")
                return

            moved_count = 0
            for page in pages:
                src_file = Path(page['file_path'])
                if not src_file.exists():
                    continue

                # Preserve directory structure relative to staging_dir
                try:
                    rel_path = src_file.relative_to(self.config.staging_dir)
                except ValueError:
                    rel_path = Path(page['site'].capitalize()) / src_file.name

                dst_file = obsidian_path / rel_path
                dst_file.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src_file, dst_file)
                moved_count += 1

            # Mark all as moved
            for site_key in set(p['site'] for p in pages):
                site_slugs = [p['slug'] for p in pages if p['site'] == site_key]
                self.db.mark_moved(site_key, site_slugs)

        print(f"Moved {moved_count} pages to: {obsidian_path}")

    def retry_failed(self, site=None):
        """Reset failed pages to pending"""
        with self.db:
            count = self.db.reset_failed(site=site)
        print(f"Reset failed pages. Pending count: {count}")

    def show_config(self):
        """Display current configuration"""
        print(f"Config file: {self.config.config_file}")
        print(f"Local config: {self.config.local_config_file}")
        print()
        print(f"staging_dir:     {self.config.staging_dir}")
        print(f"obsidian_vault:  {self.config.obsidian_vault or '(not set)'}")
        print(f"database:        {self.config.database_path}")
        print(f"delay:           {self.config.delay}")
        print(f"max_retries:     {self.config.max_retries}")
        print(f"max_workers:     {self.config.max_workers}")
        print()
        print(f"Available sites: {', '.join(SITE_SCRAPERS.keys())}")

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
        print(f"Set docs.{key} = {value}")


# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Documentation Site Scraper - Download doc sites as Obsidian markdown',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Discover all documentation pages
  %(prog)s discover
  %(prog)s discover --site snowflake
  %(prog)s discover --site soda

  # Download pending pages
  %(prog)s scrape --site soda --limit 5
  %(prog)s scrape --site snowflake --limit 5
  %(prog)s scrape --parallel

  # Download a specific page by slug
  %(prog)s scrape --site snowflake --slug guides/data-loading

  # List pages
  %(prog)s list --site snowflake --status downloaded
  %(prog)s list --json

  # Show statistics
  %(prog)s status
  %(prog)s status --site soda

  # Move to Obsidian
  %(prog)s move --all

  # Retry failed pages
  %(prog)s retry --site snowflake

  # Configuration
  %(prog)s config show
  %(prog)s config set obsidian_vault "/path/to/vault"
        """
    )

    subparsers = parser.add_subparsers(dest='command', help='Commands')

    # Discover
    discover_parser = subparsers.add_parser('discover', help='Discover pages from documentation sites')
    discover_parser.add_argument('--site', choices=list(SITE_SCRAPERS.keys()),
                                 help='Only discover from this site')

    # Scrape
    scrape_parser = subparsers.add_parser('scrape', help='Download pending pages')
    scrape_parser.add_argument('--site', choices=list(SITE_SCRAPERS.keys()),
                                help='Only scrape this site')
    scrape_parser.add_argument('--limit', type=int, help='Limit number of pages to download')
    scrape_parser.add_argument('--slug', type=str, help='Download a specific page by slug')
    scrape_parser.add_argument('--parallel', action='store_true',
                                help='Download in parallel (default 4 workers)')
    scrape_parser.add_argument('--workers', type=int,
                                help='Number of parallel workers (implies --parallel)')

    # List
    list_parser = subparsers.add_parser('list', help='List pages')
    list_parser.add_argument('--site', choices=list(SITE_SCRAPERS.keys()),
                              help='Filter by site')
    list_parser.add_argument('--status', choices=['pending', 'downloaded', 'failed'],
                              help='Filter by status')
    list_parser.add_argument('--json', action='store_true', help='Output as JSON')

    # Status
    status_parser = subparsers.add_parser('status', help='Show summary statistics')
    status_parser.add_argument('--site', choices=list(SITE_SCRAPERS.keys()),
                                help='Show status for this site only')

    # Move
    move_parser = subparsers.add_parser('move', help='Move downloaded pages to Obsidian vault')
    move_parser.add_argument('--all', action='store_true', required=True,
                              help='Move all downloaded pages')
    move_parser.add_argument('--site', choices=list(SITE_SCRAPERS.keys()),
                              help='Only move pages from this site')

    # Retry
    retry_parser = subparsers.add_parser('retry', help='Reset failed pages to pending')
    retry_parser.add_argument('--site', choices=list(SITE_SCRAPERS.keys()),
                               help='Only retry this site')

    # Config
    config_parser = subparsers.add_parser('config', help='View/set configuration')
    config_parser.add_argument('action', choices=['show', 'set'], help='Action')
    config_parser.add_argument('key', nargs='?', help='Config key')
    config_parser.add_argument('value', nargs='?', help='Value to set')

    args = parser.parse_args()

    config = Config()
    config.load()
    manager = DocsManager(config)

    if args.command == 'discover':
        manager.discover(site=args.site)

    elif args.command == 'scrape':
        parallel = args.parallel or bool(args.workers)
        if args.workers:
            config.set('max_workers', args.workers)
            config.load()
        manager.scrape(site=args.site, limit=args.limit, slug=args.slug, parallel=parallel)

    elif args.command == 'list':
        manager.list_pages(site=args.site, status=args.status, json_output=args.json)

    elif args.command == 'status':
        manager.show_status(site=args.site)

    elif args.command == 'move':
        manager.move_to_obsidian(all_pages=args.all, site=args.site)

    elif args.command == 'retry':
        manager.retry_failed(site=args.site)

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
