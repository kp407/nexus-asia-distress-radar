"""
crawlers/firecrawl_client.py
════════════════════════════════════════════════════════════════════════
Firecrawl API integration for Nexus Asia Distress Radar.

Wraps Firecrawl's scrape + crawl endpoints and provides a drop-in
replacement for requests.Session.get() on URLs that are blocked by
Cloudflare / govt firewalls when run from GitHub Actions.

BLOCKED SOURCES THIS SOLVES:
  Property Portals:
    - 99acres.com               (Cloudflare)
    - magicbricks.com           (Cloudflare)
    - squareyards.com           (Cloudflare)
    - anarock.com               (Cloudflare)
    - jll.co.in                 (Cloudflare)
    - cbre.co.in                (Cloudflare)
    - colliers.com              (Cloudflare)
    - knightfrank.co.in         (Cloudflare)

  Bank / Legal Portals:
    - ibapi.in                  (intermittent)
    - bankauctions.co.in        (blocks scrapers)
    - sarfaesi.com              (blocks scrapers)
    - drt.gov.in                (govt firewall)
    - nclt.gov.in               (govt firewall)
    - ibbi.gov.in               (blocks automated requests)
    - mca.gov.in                (blocks bots)

  ARC Portfolios (dynamic JS):
    - narcl.co.in               (site down / slow)
    - edelweissarc.com          (dynamic JS rendering)
    - arcil.com                 (dynamic JS rendering)
    - phoenixarc.co.in          (dynamic JS rendering)
    - jmfarc.com                (dynamic JS rendering)
    - kotakarc.com              (dynamic JS rendering)

  Research / Media:
    - reuters.com               (blocks scrapers)
    - vccircle.com              (paywall + blocks)
    - economictimes.indiatimes.com (rate limits)
    - sebi.gov.in               (pagination blocks)

USAGE:
  from crawlers.firecrawl_client import FirecrawlSession

  # In any crawler, replace:
  #   session = requests.Session()
  # with:
  #   session = FirecrawlSession()
  # Everything else stays the same — safe_get() works identically.

FIRECRAWL FREE TIER: 500 pages/month
FIRECRAWL STARTER:   100,000 pages/month @ $16/month
════════════════════════════════════════════════════════════════════════
"""

import os
import re
import time
import logging
import requests
from typing import Optional

logger = logging.getLogger("nexus.firecrawl")

FIRECRAWL_API_KEY = os.environ.get("FIRECRAWL_API_KEY", "")
FIRECRAWL_BASE    = "https://api.firecrawl.dev/v1"

# ── Domains that MUST go through Firecrawl ─────────────────────────────
# Direct requests to these will always fail from GitHub Actions
FIRECRAWL_DOMAINS = {
    # Property portals
    "99acres.com",
    "magicbricks.com",
    "squareyards.com",
    "anarock.com",
    "jll.co.in",
    "cbre.co.in",
    "colliers.com",
    "knightfrank.co.in",
    "commonfloor.com",
    "housing.com",
    # Bank / legal / govt
    "ibapi.in",
    "bankauctions.co.in",
    "sarfaesi.com",
    "npaauctions.com",
    "drt.gov.in",
    "nclt.gov.in",
    "ibbi.gov.in",
    "mca.gov.in",
    # ARC portfolios
    "narcl.co.in",
    "edelweissarc.com",
    "arcil.com",
    "phoenixarc.co.in",
    "jmfarc.com",
    "kotakarc.com",
    "cfmarc.in",
    "uvarcl.com",
    # Research / media
    "reuters.com",
    "vccircle.com",
    "dealstreetasia.com",
    "sebi.gov.in",
    # Individual bank portals
    "bankofbaroda.in",
    "pnbindia.in",
    "unionbankofindia.co.in",
    "canarabank.com",
    "centralbankofindia.co.in",
    "iob.in",
    "bankofmaharashtra.in",
    "sbi.co.in",
}

# ── Domains where direct requests work fine ────────────────────────────
# These stay on regular requests (fast, free, no API quota used)
DIRECT_DOMAINS = {
    "news.google.com",
    "livelaw.in",
    "barandbench.com",
    "economictimes.indiatimes.com",
    "livemint.com",
    "business-standard.com",
    "moneycontrol.com",
    "financialexpress.com",
    "thehindubusinessline.com",
    "zeebiz.com",
    "rbi.org.in",
    "scconline.com",
    "indiacorplaw.in",
    "insolvencytracker.in",
    "npablog.in",
    "verdictum.in",
    "inc42.com",
}


def _get_domain(url: str) -> str:
    """Extract base domain from URL."""
    m = re.search(r"https?://(?:www\.)?([^/]+)", url)
    return m.group(1).lower() if m else ""


def _needs_firecrawl(url: str) -> bool:
    """Check if URL needs Firecrawl based on domain."""
    domain = _get_domain(url)
    # Check exact match and subdomain match
    return any(
        domain == fc_domain or domain.endswith("." + fc_domain)
        for fc_domain in FIRECRAWL_DOMAINS
    )


class FakeResponse:
    """
    Mimics requests.Response so existing safe_get() code works
    without modification. Only implements what safe_get checks:
    - status_code
    - text
    - json()
    """
    def __init__(self, content: str, status_code: int = 200, url: str = ""):
        self.status_code = status_code
        self.text = content
        self.url = url
        self._json = None

    def json(self):
        import json
        if self._json is None:
            try:
                self._json = json.loads(self.text)
            except Exception:
                self._json = {}
        return self._json


def firecrawl_scrape(url: str, wait_ms: int = 2000) -> Optional[FakeResponse]:
    """
    Call Firecrawl /scrape endpoint for a single URL.
    Returns FakeResponse with markdown + HTML content,
    or None on failure.

    wait_ms: milliseconds to wait for dynamic JS to render
             2000ms handles most React/Vue/Angular SPA portals
             Increase to 4000 for especially slow govt sites
    """
    if not FIRECRAWL_API_KEY:
        logger.warning("FIRECRAWL_API_KEY not set — skipping Firecrawl")
        return None

    payload = {
        "url": url,
        "formats": ["html", "markdown"],
        "waitFor": wait_ms,
        "actions": [],
        "onlyMainContent": False,   # get full page not just article
        "timeout": 30000,
    }

    try:
        r = requests.post(
            f"{FIRECRAWL_BASE}/scrape",
            headers={
                "Authorization": f"Bearer {FIRECRAWL_API_KEY}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=45,
        )

        if r.status_code == 200:
            data = r.json()
            if data.get("success"):
                # Prefer HTML for BeautifulSoup parsing
                html = data.get("data", {}).get("html", "")
                md   = data.get("data", {}).get("markdown", "")
                content = html if html else md
                logger.info(f"  Firecrawl ✓ [{url[:60]}] — {len(content)} chars")
                return FakeResponse(content, 200, url)
            else:
                logger.warning(f"  Firecrawl returned success=false for {url}: {data}")
                return None

        elif r.status_code == 402:
            logger.error("  Firecrawl: payment required — check your plan/credits")
            return None
        elif r.status_code == 429:
            logger.warning("  Firecrawl: rate limited — waiting 10s")
            time.sleep(10)
            return firecrawl_scrape(url, wait_ms)  # one retry
        else:
            logger.warning(f"  Firecrawl HTTP {r.status_code} for {url}: {r.text[:200]}")
            return None

    except Exception as e:
        logger.error(f"  Firecrawl request failed for {url}: {e}")
        return None


def firecrawl_crawl(
    base_url: str,
    max_pages: int = 10,
    include_paths: list = None,
    exclude_paths: list = None,
) -> list[FakeResponse]:
    """
    Call Firecrawl /crawl endpoint to crawl multiple pages from a base URL.
    Useful for paginated portals (ibapi.in auction listings, bank portals).

    Returns list of FakeResponse objects, one per crawled page.
    """
    if not FIRECRAWL_API_KEY:
        return []

    payload = {
        "url": base_url,
        "maxDepth": 2,
        "limit": max_pages,
        "scrapeOptions": {
            "formats": ["html", "markdown"],
            "waitFor": 2000,
            "onlyMainContent": False,
        },
    }
    if include_paths:
        payload["includePaths"] = include_paths
    if exclude_paths:
        payload["excludePaths"] = exclude_paths

    try:
        # Start crawl job
        r = requests.post(
            f"{FIRECRAWL_BASE}/crawl",
            headers={
                "Authorization": f"Bearer {FIRECRAWL_API_KEY}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=30,
        )

        if r.status_code != 200:
            logger.warning(f"  Firecrawl crawl start failed {r.status_code}: {r.text[:200]}")
            return []

        job_id = r.json().get("id")
        if not job_id:
            return []

        logger.info(f"  Firecrawl crawl job {job_id} started for {base_url}")

        # Poll for completion
        for attempt in range(30):  # max 5 minutes
            time.sleep(10)
            status_r = requests.get(
                f"{FIRECRAWL_BASE}/crawl/{job_id}",
                headers={"Authorization": f"Bearer {FIRECRAWL_API_KEY}"},
                timeout=15,
            )
            if status_r.status_code != 200:
                continue

            status_data = status_r.json()
            status = status_data.get("status", "")

            if status == "completed":
                pages = status_data.get("data", [])
                logger.info(f"  Firecrawl crawl complete — {len(pages)} pages")
                results = []
                for page in pages:
                    html = page.get("html", "") or page.get("markdown", "")
                    page_url = page.get("metadata", {}).get("sourceURL", base_url)
                    if html:
                        results.append(FakeResponse(html, 200, page_url))
                return results

            elif status == "failed":
                logger.warning(f"  Firecrawl crawl job {job_id} failed")
                return []

            logger.debug(f"  Firecrawl crawl {attempt+1}/30: {status}")

        logger.warning("  Firecrawl crawl timed out after 5 minutes")
        return []

    except Exception as e:
        logger.error(f"  Firecrawl crawl failed for {base_url}: {e}")
        return []


class FirecrawlSession:
    """
    Drop-in replacement for requests.Session.
    Automatically routes blocked URLs through Firecrawl,
    direct URLs through regular requests.

    Usage (in any crawler):
        # OLD:
        session = requests.Session()
        # NEW:
        from crawlers.firecrawl_client import FirecrawlSession
        session = FirecrawlSession()

        # Then use exactly as before:
        resp = self.safe_get(session, url)
        if resp:
            soup = BeautifulSoup(resp.text, "html.parser")
    """

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-IN,en;q=0.9",
    }

    def __init__(self, firecrawl_wait_ms: int = 2000):
        self._session = requests.Session()
        self._session.headers.update(self.HEADERS)
        self.firecrawl_wait_ms = firecrawl_wait_ms
        self._firecrawl_available = bool(FIRECRAWL_API_KEY)

        if not self._firecrawl_available:
            logger.warning(
                "FIRECRAWL_API_KEY not set. Blocked portals will fail. "
                "Add secret in GitHub → Settings → Secrets → FIRECRAWL_API_KEY"
            )

    def headers(self):
        """Allows session.headers.update() calls from existing crawlers."""
        return self._session.headers

    def get(self, url: str, **kwargs) -> Optional[requests.Response]:
        """
        Route request: Firecrawl for blocked domains, direct for the rest.
        kwargs are passed to requests.Session.get() for direct requests.
        Ignores kwargs for Firecrawl (timeout etc. handled internally).
        """
        if self._firecrawl_available and _needs_firecrawl(url):
            logger.info(f"  → Firecrawl: {url[:70]}")
            # Use longer wait for JS-heavy ARC and govt portals
            domain = _get_domain(url)
            wait = 4000 if any(d in domain for d in [
                "drt.gov.in", "nclt.gov.in", "ibbi.gov.in", "mca.gov.in",
                "narcl.co.in", "edelweissarc.com", "arcil.com",
                "kotakarc.com", "jmfarc.com", "phoenixarc.co.in",
            ]) else self.firecrawl_wait_ms

            resp = firecrawl_scrape(url, wait_ms=wait)
            return resp
        else:
            # Direct request for non-blocked domains
            timeout = kwargs.pop("timeout", 20)
            try:
                r = self._session.get(url, timeout=timeout, **kwargs)
                return r if r.status_code == 200 else None
            except Exception as e:
                logger.error(f"  Direct GET failed [{url[:60]}]: {e}")
                return None

    def post(self, url: str, **kwargs):
        """Pass-through for any POST calls (API endpoints)."""
        try:
            return self._session.post(url, **kwargs)
        except Exception as e:
            logger.error(f"  POST failed [{url[:60]}]: {e}")
            return None
