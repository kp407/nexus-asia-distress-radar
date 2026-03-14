"""
crawlers/drt_sarfaesi.py
═══════════════════════════════════════════════════════════════════════════
Crawls DRT (Debt Recovery Tribunal) portals and legal NPA notice sources.

Why this matters (from deal intel):
  - Lawyers handling NPA and SARFAESI cases are the BEST source for
    unlisted distressed properties — before they hit public auction
  - DRT orders precede physical possession by 2–6 months
  - These leads = first-mover advantage before asset hits bankauction.in
  - Establish lawyer connects → get inventory nobody else sees

Sources:
  1. DRT National Portal (drt.gov.in) — cause lists, orders, OAs filed
  2. SARFAESI Notice aggregators — Section 13(2) and 13(4) notices
  3. Google News — DRT + Mumbai + commercial property signals
  4. High Court Mumbai — Writ petitions against bank possessions
  5. Legal500 / Cyril Amarchand / AZB NPA practice updates
     (proxy: their websites / press releases)
═══════════════════════════════════════════════════════════════════════════
"""

import re
import time
import logging
import requests
from .firecrawl_client import FirecrawlSession
from bs4 import BeautifulSoup
from urllib.parse import quote
from .base import BaseCrawler, DistressEvent

logger = logging.getLogger(__name__)

# DRT bench locations in India — we weight Mumbai + Pune + Delhi heavily
DRT_BENCHES = {
    'drt_mumbai':   'https://drt.gov.in/DRT_Mumbai',
    'drt_pune':     'https://drt.gov.in/DRT_Pune',
    'drt_delhi':    'https://drt.gov.in/DRT_Delhi',
    'drt_ahmedabad':'https://drt.gov.in/DRT_Ahmedabad',
    'drt_bangalore':'https://drt.gov.in/DRT_Bangalore',
    'drt_chennai':  'https://drt.gov.in/DRT_Chennai',
    'drt_national': 'https://drt.gov.in',
}

# NPA law firm / legal news blogs that surface unlisted deals
LEGAL_NPA_SOURCES = [
    ('NPA Legal Blog', 'https://npablog.in/feed'),
    ('Insolvency Tracker', 'https://insolvencytracker.in/feed'),
    ('CAM Blog / NPA', 'https://www.camlegal.in/blog/feed'),
    ('AZB NPA Updates', 'https://azbpartners.com/blog/feed/'),
    ('Cyril Amarchand Blog', 'https://camcoblog.com/feed/'),
    ('Nishith Desai Finance', 'https://www.nishithdesai.com/rss.xml'),
    ('JSA NPA', 'https://www.jsalaw.com/news-alerts/rss/'),
    ('Khaitan NPA', 'https://www.khaitanco.com/thought-leadership/rss'),
    ('Trilegal Finance', 'https://trilegal.com/insights/rss/'),
]

# SARFAESI / NPA keyword signals specifically for DRT context
DRT_KEYWORDS = [
    'original application', 'oa filed', 'debt recovery',
    'recovery certificate', 'drt order', 'drt-i', 'drt-ii',
    'section 13(2)', 'section 13(4)', '60 day notice',
    'symbolic possession', 'physical possession taken',
    'secured asset auctioned', 'attachment of property',
    'recovery officer', 'certificate of recovery',
    'npa loan recovery', 'nbfc recovery', 'bank recovery case',
    'lok adalat settlement', 'sarfaesi possession',
    'caveat petition', 'stay against possession',
    'writ against bank auction', 'high court stay auction',
]

# Property value ranges of interest (INR crore)
MIN_DEAL_SIZE_CR = 5.0
MAX_DEAL_SIZE_CR = 500.0


class DRTPortalCrawler(BaseCrawler):
    """
    Crawls Debt Recovery Tribunal national portal.
    DRT orders are precursors to physical possession and auction —
    catching a deal at DRT stage = 2–6 months head start.
    """
    SOURCE_NAME = 'DRT Portal'
    SOURCE_URL = 'https://drt.gov.in'
    CATEGORY = 'legal'

    CRAWL_URLS = [
        # DRT portal restructured — try current paths
        'https://drt.gov.in/causelist',
        'https://drt.gov.in/orders',
        'https://drt.gov.in/judgments',
        # Older paths as fallback
        'https://drt.gov.in/DRT_Mumbai/causelistschedule.aspx',
        'https://drt.gov.in/content/drt-mumbai',
    ]

    def crawl(self) -> list[DistressEvent]:
        events = []
        session = FirecrawlSession()

        for url in self.CRAWL_URLS:
            resp = self.safe_get(session, url)
            if not resp:
                continue

            soup = BeautifulSoup(resp.text, 'html.parser')

            # DRT sites use tables for case lists
            for tag in soup.find_all(['tr', 'li', 'p', 'td']):
                text = tag.get_text(separator=' ', strip=True)
                if len(text) < 20:
                    continue

                # Check for DRT-specific signals
                t_lower = text.lower()
                kws = self.detect_keywords(text)
                drt_hit = any(k in t_lower for k in DRT_KEYWORDS)

                if not kws and not drt_hit:
                    continue

                companies = self.extract_company_names(text)
                company_name = companies[0] if companies else 'DRT Case'

                link_tag = tag.find('a', href=True)
                article_url = url
                if link_tag:
                    href = link_tag['href']
                    article_url = (
                        href if href.startswith('http')
                        else self.SOURCE_URL + href
                    )

                # Extract OA / Case number
                case_match = re.search(
                    r'(?:OA|RC|IA|MA)[\/\-\s]\d+[\/\-\s]\d{4}',
                    text, re.IGNORECASE
                )
                case_number = case_match.group(0) if case_match else ''

                # Identify the DRT bench from URL
                bench = 'DRT'
                for bench_key in DRT_BENCHES:
                    if bench_key.replace('drt_', '').upper() in url.upper():
                        bench = f'DRT {bench_key.replace("drt_", "").title()}'
                        break

                cat = kws[0][1] if kws else 'creditor_action'
                kw  = kws[0][0] if kws else 'debt recovery tribunal'

                events.append(self.make_event(
                    company_name=company_name,
                    keyword=kw,
                    category=cat,
                    url=article_url,
                    headline=text[:250],
                    snippet=text[:600],
                    metadata={
                        'drt_bench': bench,
                        'case_number': case_number,
                        'source_type': 'legal_tribunal',
                        'pre_auction_signal': True,
                        'source_page': url,
                    },
                ))

                if len(events) >= self.MAX_ARTICLES:
                    break

        self.logger.info(f'DRT Portal: {len(events)} events')
        return events


class SARFAESINoticeCrawler(BaseCrawler):
    """
    Crawls SARFAESI Section 13(2) and 13(4) notice aggregators.
    These notices = bank has declared NPA and initiated recovery.
    Physical possession typically follows within 60–90 days.
    This is the EARLIEST public signal for distressed asset deals.
    """
    SOURCE_NAME = 'SARFAESI Notices'
    SOURCE_URL = 'https://ibapi.in'
    CATEGORY = 'auction'

    # IBAPI is the RBI-backed notice aggregator
    CRAWL_URLS = [
        'https://ibapi.in/Auctions/GetAuctions?type=active',
        'https://ibapi.in/Auctions/GetAuctions?type=upcoming',
        'https://ibapi.in/Notices',
        'https://ibapi.in/Possession',
    ]

    # Google News queries for SARFAESI notices (works from GitHub Actions)
    GNEWS_QUERIES = [
        'SARFAESI section 13(2) notice Mumbai commercial property',
        'SARFAESI section 13(4) possession Mumbai office',
        'bank possession notice Mumbai Grade A office',
        'SARFAESI notice Thane Navi Mumbai commercial',
        'bank auction commercial office space Mumbai 2024',
        'NPA commercial property auction Mumbai crore',
    ]

    GNEWS_BASE = 'https://news.google.com/rss/search?q={q}&hl=en-IN&gl=IN&ceid=IN:en'

    def crawl(self) -> list[DistressEvent]:
        events = []
        session = FirecrawlSession()

        # 1. IBAPI direct crawl
        for url in self.CRAWL_URLS:
            resp = self.safe_get(session, url)
            if not resp:
                continue

            # Try JSON first (IBAPI has API endpoints)
            if 'GetAuctions' in url:
                try:
                    data = resp.json()
                    items = data if isinstance(data, list) else data.get('data', data.get('auctions', []))
                    for item in items[:self.MAX_ARTICLES]:
                        text = (
                            f"{item.get('title', '')} {item.get('description', '')} "
                            f"{item.get('assetDescription', '')} {item.get('borrowerName', '')}"
                        )
                        if not text.strip():
                            continue

                        price_text = str(item.get('reservePrice', item.get('reserve_price', '')))
                        price = None
                        try:
                            price = float(price_text.replace(',', '')) / 1e7  # paise → crore
                        except (ValueError, TypeError):
                            price = self._extract_price(text)

                        location = self._extract_location(item, text)
                        asset_class = self._classify_asset(item, text)

                        auction_date = (
                            item.get('auctionDate') or
                            item.get('auction_date') or
                            item.get('saleDate')
                        )

                        bank = (
                            item.get('bankName') or
                            item.get('bank_name') or
                            item.get('creditorName', 'Bank')
                        )

                        company = (
                            item.get('borrowerName') or
                            item.get('borrower_name') or
                            item.get('companyName') or
                            'SARFAESI Auction Property'
                        )

                        events.append(self.make_event(
                            company_name=company[:100],
                            keyword='sarfaesi',
                            category='sarfaesi',
                            url=item.get('url', item.get('link', self.SOURCE_URL)),
                            headline=text[:250],
                            snippet=text[:600],
                            metadata={
                                'bank': bank,
                                'reserve_price_crore': price,
                                'location': location,
                                'asset_class': asset_class,
                                'auction_date': str(auction_date) if auction_date else None,
                                'is_mmr': self._is_mmr(location),
                                'source': 'ibapi',
                                'raw_item': {
                                    k: str(v)[:200]
                                    for k, v in item.items()
                                    if k in ('id', 'status', 'auctionType', 'propertyType')
                                },
                            },
                        ))
                    self.logger.info(f'IBAPI JSON {url[-30:]}: {len(items)} items')
                    continue
                except (ValueError, KeyError):
                    pass

            # HTML fallback
            soup = BeautifulSoup(resp.text, 'html.parser')
            for row in soup.find_all(['tr', 'div', 'article'])[:self.MAX_ARTICLES]:
                text = row.get_text(separator=' ', strip=True)
                if len(text) < 30:
                    continue
                t_lower = text.lower()
                if not any(w in t_lower for w in ['sarfaesi', 'auction', 'possession', 'npa', 'reserve price']):
                    continue

                link = row.find('a', href=True)
                link_url = self.SOURCE_URL
                if link:
                    href = link['href']
                    link_url = href if href.startswith('http') else self.SOURCE_URL + href

                price = self._extract_price(text)
                location = text  # pass full text for location detection

                events.append(self.make_event(
                    company_name='SARFAESI Auction Property',
                    keyword='sarfaesi',
                    category='sarfaesi',
                    url=link_url,
                    headline=text[:250],
                    snippet=text[:600],
                    metadata={
                        'reserve_price_crore': price,
                        'source': 'ibapi_html',
                    },
                ))

        # 2. Google News for SARFAESI signals (always accessible)
        for query in self.GNEWS_QUERIES:
            url = self.GNEWS_BASE.format(q=quote(query))
            resp = self.safe_get(session, url)
            if not resp:
                continue

            try:
                import xml.etree.ElementTree as ET
                root = ET.fromstring(resp.text)
                for item in root.iter('item'):
                    title = (item.findtext('title') or '').strip()
                    link  = (item.findtext('link') or '').strip()
                    desc  = (item.findtext('description') or '').strip()
                    if not title:
                        continue

                    full = f'{title} {desc}'
                    kws = self.detect_keywords(full)
                    if not kws and not any(k in full.lower() for k in DRT_KEYWORDS[:8]):
                        continue

                    companies = self.extract_company_names(title)
                    company = companies[0] if companies else 'SARFAESI Signal'
                    kw  = kws[0][0] if kws else 'sarfaesi'
                    cat = kws[0][1] if kws else 'sarfaesi'

                    events.append(self.make_event(
                        company_name=company,
                        keyword=kw,
                        category=cat,
                        url=link,
                        headline=title,
                        snippet=desc[:400],
                        metadata={
                            'gnews_query': query,
                            'source': 'gnews_sarfaesi',
                        },
                    ))
            except Exception as e:
                self.logger.debug(f'GNews parse error [{query[:40]}]: {e}')

            time.sleep(0.8)

        self.logger.info(f'SARFAESI Notices: {len(events)} events')
        return events

    # ─── helpers ───────────────────────────────────────────────

    def _extract_price(self, text: str) -> float | None:
        from .multi_bank_auctions import extract_price_inr
        return extract_price_inr(text)

    def _is_mmr(self, location) -> bool:
        if not location:
            return False
        from .multi_bank_auctions import MMR_LOCATIONS
        return any(loc in location.lower() for loc in MMR_LOCATIONS)

    def _extract_location(self, item: dict, fallback_text: str) -> str | None:
        loc = (
            item.get('state') or item.get('city') or
            item.get('location') or item.get('district') or ''
        )
        if loc:
            return loc
        from .multi_bank_auctions import detect_mmr_location
        return detect_mmr_location(fallback_text)

    def _classify_asset(self, item: dict, text: str) -> str:
        prop_type = (
            item.get('propertyType') or item.get('assetType') or ''
        ).lower()
        if 'commercial' in prop_type or 'office' in prop_type:
            return 'commercial'
        if 'residential' in prop_type or 'flat' in prop_type:
            return 'residential'
        if 'land' in prop_type or 'plot' in prop_type:
            return 'land'
        from .multi_bank_auctions import detect_asset_class
        return detect_asset_class(text)


class NPALawyerNetworkCrawler(BaseCrawler):
    """
    Surfaces unlisted NPA/SARFAESI deals through legal intelligence feeds.
    Lawyers handling NPA cases = earliest deal sourcing channel.
    These leads arrive 3–9 months before a property hits bankauction.in.

    Strategy: crawl law firm NPA practice blogs + legal news sites that
    report on significant SARFAESI matters before public auction.
    """
    SOURCE_NAME = 'NPA Legal Intelligence'
    SOURCE_URL = 'https://barandbench.com'
    CATEGORY = 'legal'

    # Legal RSS feeds with NPA / finance practice content
    LEGAL_FEEDS = [
        ('Bar & Bench Finance', 'https://www.barandbench.com/feed'),
        ('LiveLaw Finance',     'https://www.livelaw.in/rss/'),
        ('SCC Online Blog',     'https://www.scconline.com/blog/feed/'),
        ('IndiaCorpLaw',        'https://indiacorplaw.in/feed'),
        ('Mondaq India',        'https://www.mondaq.com/rss.aspx?tagid=252'),
        ('Trilegal Insights',   'https://trilegal.com/insights/rss/'),
        ('Khaitan Finance',     'https://www.khaitanco.com/thought-leadership/rss'),
    ]

    # Additional targeted Google News queries for lawyer/NPA channel
    GNEWS_NPA_QUERIES = [
        'NPA recovery lawyer Mumbai commercial property 2024',
        'SARFAESI lawyer India commercial real estate Mumbai',
        'DRT Mumbai commercial property order 2024',
        'NPA resolution commercial office Mumbai crore',
        'distressed commercial property Mumbai sale 2024',
    ]

    GNEWS_BASE = 'https://news.google.com/rss/search?q={q}&hl=en-IN&gl=IN&ceid=IN:en'

    def crawl(self) -> list[DistressEvent]:
        events = []
        session = FirecrawlSession()

        # 1. Legal RSS feeds
        for source_name, feed_url in self.LEGAL_FEEDS:
            try:
                resp = self.safe_get(session, feed_url)
                if not resp:
                    continue

                import xml.etree.ElementTree as ET
                root = ET.fromstring(resp.text)

                for item in root.iter('item'):
                    title = (item.findtext('title') or '').strip()
                    link  = (item.findtext('link') or '').strip()
                    desc  = (item.findtext('description') or '').strip()
                    pub   = (item.findtext('pubDate') or '').strip()

                    if not title:
                        continue

                    full_text = f'{title} {desc}'
                    kws = self.detect_keywords(full_text)

                    # Also check for DRT / SARFAESI specific keywords
                    drt_hit = any(k in full_text.lower() for k in DRT_KEYWORDS)

                    if not kws and not drt_hit:
                        continue

                    companies = self.extract_company_names(title)
                    company = companies[0] if companies else 'NPA Legal Matter'
                    kw  = kws[0][0] if kws else 'debt recovery tribunal'
                    cat = kws[0][1] if kws else 'creditor_action'

                    # Extract case references
                    case_match = re.search(
                        r'(?:OA|CP|RC|IA|WP|DRT)[\/\-\s#][\d]+',
                        full_text, re.IGNORECASE
                    )

                    events.append(self.make_event(
                        company_name=company,
                        keyword=kw,
                        category=cat,
                        url=link,
                        headline=title,
                        snippet=desc[:400],
                        published_at=pub[:50] if pub else None,
                        metadata={
                            'source': source_name,
                            'case_reference': case_match.group(0) if case_match else '',
                            'channel': 'legal_intelligence',
                            'pre_auction_signal': True,
                        },
                    ))

                self.logger.info(f'  {source_name}: {len(events)} signals so far')
                time.sleep(0.8)

            except Exception as e:
                self.logger.warning(f'  {source_name} feed error: {e}')

        # 2. Google News NPA intelligence
        for query in self.GNEWS_NPA_QUERIES:
            url = self.GNEWS_BASE.format(q=quote(query))
            resp = self.safe_get(session, url)
            if not resp:
                continue
            try:
                import xml.etree.ElementTree as ET
                root = ET.fromstring(resp.text)
                for item in root.iter('item'):
                    title = (item.findtext('title') or '').strip()
                    link  = (item.findtext('link') or '').strip()
                    desc  = (item.findtext('description') or '').strip()
                    if not title:
                        continue

                    full = f'{title} {desc}'
                    kws = self.detect_keywords(full)
                    if not kws and not any(k in full.lower() for k in DRT_KEYWORDS[:6]):
                        continue

                    company = self.extract_company_names(title)
                    events.append(self.make_event(
                        company_name=company[0] if company else 'NPA Property Signal',
                        keyword=kws[0][0] if kws else 'npa',
                        category=kws[0][1] if kws else 'default',
                        url=link,
                        headline=title,
                        snippet=desc[:400],
                        metadata={
                            'gnews_query': query,
                            'channel': 'gnews_legal',
                        },
                    ))
            except Exception as e:
                self.logger.debug(f'GNews NPA error [{query[:40]}]: {e}')
            time.sleep(0.8)

        self.logger.info(f'NPA Legal Intelligence: {len(events)} events')
        return events
