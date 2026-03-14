"""
crawlers/cap_rate_market.py
═══════════════════════════════════════════════════════════════════════════
Live Cap Rate Market Intelligence Crawler

Pulls real rent and yield data from multiple sources weekly and writes
dated snapshots into cap_rate_snapshots table. Once multiple snapshots
exist per micro-market, the dashboard shows compression/expansion trends.

Sources (in order of reliability):
  1. Google News RSS — JLL / Anarock / Colliers / Knight Frank reports
     that quote actual rent psf and yield numbers per micro-market
  2. 99acres/MagicBricks pre-leased listings — extract ask price + quoted
     rent → compute observed cap rate from real transactions
  3. Economic Times / Business Standard CRE articles — rent movement
     ("BKC rents rise 8%", "Andheri asking rents soften")
  4. Hardcoded quarterly baseline update — fallback when scrapers yield
     nothing; updates the seed values with a fresh snapshot_date

What gets written to DB (cap_rate_snapshots):
  - micro_market, asset_class, cap_rate_pct
  - avg_rent_psf, avg_price_psf
  - sample_size (number of listings averaged)
  - snapshot_date (today — so each weekly run creates a new dated row)
  - source ("JLL Q1 2026", "99acres listings", "Market Intel" etc.)
  - notes (any context extracted from the article)

The dashboard reads all snapshots ordered by snapshot_date and
displays the trend line per micro-market.
═══════════════════════════════════════════════════════════════════════════
"""

import re
import os
import json
import time
import logging
import requests
from .firecrawl_client import FirecrawlSession
from datetime import date, datetime, timezone
from urllib.parse import quote
from collections import defaultdict
from bs4 import BeautifulSoup
from .base import BaseCrawler, DistressEvent

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# MARKET KNOWLEDGE BASE
# Updated quarterly. These are fallback baselines — real data
# from scrapers override these when available.
# ─────────────────────────────────────────────────────────────

# Current quarter baseline (Q1 2026)
# Sources: JLL India Office Market Q4 2025, Anarock CRE Report Q4 2025
MARKET_BASELINE_Q1_2026 = [
    # (micro_market, asset_class, cap_rate_pct, avg_rent_psf, avg_price_psf_inr, note)
    # avg_price_psf_inr = ₹ per sqft capital value (e.g. BKC = ₹75,000/sqft to buy)
    ('BKC',            'grade_a_office', 5.28,  360, 75000, 'Core CBD — JLL Q4 2025'),
    ('Lower Parel',    'grade_a_office', 5.50,  290, 58000, 'Office corridor — Anarock Q4 2025'),
    ('Worli',          'grade_a_office', 5.72,  265, 51000, 'Premium SBD — Knight Frank Q4 2025'),
    ('Andheri',        'grade_a_office', 6.09,  155, 28000, 'IT zone East — Colliers Q4 2025'),
    ('Powai',          'grade_a_office', 6.46,  135, 23000, 'Tech campus — JLL Q4 2025'),
    ('Goregaon',       'grade_a_office', 6.71,  122, 20000, 'SBD north — Market Intel'),
    ('Malad',          'grade_a_office', 6.90,  118, 18800, 'Suburban — active deal pipeline'),
    ('Kurla',          'grade_b_office', 7.16,  112, 17200, 'Commercial hub'),
    ('Vikhroli',       'grade_b_office', 7.33,  102, 15300, 'Eastern suburbs'),
    ('Thane',          'grade_a_office', 7.48,   83, 12200, 'Suburban node'),
    ('Navi Mumbai',    'grade_a_office', 7.80,   78, 11000, 'IT parks — near threshold'),
    ('Airoli',         'grade_b_office', 8.25,   72,  9600, 'Trans-harbour — meets threshold'),
    ('Belapur',        'grade_b_office', 8.42,   67,  8750, 'Node CBD — meets threshold'),
    ('Vashi',          'grade_a_office', 8.05,   82, 11200, 'Node CBD — meets threshold'),
    ('Wadala',         'grade_a_office', 6.60,  132, 22000, 'Central — BKC spillover'),
    ('Pune CBD',       'grade_a_office', 6.88,  110, 17600, 'Pune central'),
    ('Pune Hinjewadi', 'grade_a_office', 7.60,   85, 12300, 'IT park zone'),
    ('Hyderabad HiTec','grade_a_office', 7.33,   90, 13500, 'HITEC City corridor'),
    ('Bengaluru CBD',  'grade_a_office', 6.41,  120, 20600, 'Bengaluru core'),
    ('Bengaluru ORR',  'grade_a_office', 7.16,   95, 14600, 'Outer Ring Road'),
]

# Micro-market name normalisation — maps scraped variations to canonical names
MARKET_ALIASES = {
    'bandra kurla':   'BKC',
    'bandra kurla complex': 'BKC',
    'bkc':            'BKC',
    'lower parel':    'Lower Parel',
    'parel':          'Lower Parel',
    'worli':          'Worli',
    'andheri east':   'Andheri',
    'andheri':        'Andheri',
    'andheri west':   'Andheri',
    'powai':          'Powai',
    'goregaon east':  'Goregaon',
    'goregaon':       'Goregaon',
    'malad':          'Malad',
    'malad west':     'Malad',
    'malad east':     'Malad',
    'kurla':          'Kurla',
    'kurla west':     'Kurla',
    'vikhroli':       'Vikhroli',
    'thane':          'Thane',
    'thane west':     'Thane',
    'navi mumbai':    'Navi Mumbai',
    'airoli':         'Airoli',
    'belapur':        'Belapur',
    'cbd belapur':    'Belapur',
    'vashi':          'Vashi',
    'wadala':         'Wadala',
    'pune':           'Pune CBD',
    'hinjewadi':      'Pune Hinjewadi',
    'hitec city':     'Hyderabad HiTec',
    'hitech city':    'Hyderabad HiTec',
    'bengaluru':      'Bengaluru CBD',
    'bangalore':      'Bengaluru CBD',
    'outer ring road':'Bengaluru ORR',
    'orr':            'Bengaluru ORR',
}

# ─────────────────────────────────────────────────────────────
# EXTRACTORS
# ─────────────────────────────────────────────────────────────

# Rent: "₹150/sqft", "Rs 120 per sqft", "120 psf", "rental of 95 per sq ft"
RENT_RE = re.compile(
    r'(?:rent|rental|lease|asking|quoted|quoting|quoted\s+at|at)\s*'
    r'(?:rs\.?|₹|inr)?\s*([\d,]+(?:\.\d+)?)\s*'
    r'(?:per\s+sq\.?\s*ft\.?|per\s+sqft|psf|\/sqft|\/sq\.?ft\.?)',
    re.IGNORECASE
)
# Also catch "₹350 psf" or "Rs 280/sqft" without the rent keyword
RENT_RAW_RE = re.compile(
    r'(?:rs\.?|₹|inr)\s*([\d,]+(?:\.\d+)?)\s*'
    r'(?:per\s+sq\.?\s*ft\.?|per\s+sqft|psf|\/sqft)',
    re.IGNORECASE
)

# Price: "₹45 crore", "Rs 120 cr", "25 lakh per sqft"
PRICE_CR_RE = re.compile(
    r'(?:rs\.?|₹|inr)?\s*([\d,]+(?:\.\d+)?)\s*(?:crore|cr\.?\b)',
    re.IGNORECASE
)
PRICE_PSF_RE = re.compile(
    r'(?:rs\.?|₹|inr)?\s*([\d,]+(?:\.\d+)?)\s*(?:lakh|lac)\s*per\s*sq\.?\s*ft',
    re.IGNORECASE
)

# Cap rate / yield: "yield of 7.2%", "cap rate 6.8%", "8.5% yield", "grossing 9%"
YIELD_RE = re.compile(
    r'(?:yield|cap\s*rate|grossing|returns?|rental\s+yield|investment\s+yield)'
    r'\s*(?:of|at|:)?\s*([\d]+(?:\.\d+)?)\s*%'
    r'|'
    r'([\d]+(?:\.\d+)?)\s*%\s*(?:yield|cap\s*rate|return|rental\s+yield)',
    re.IGNORECASE
)

# Area: "40,000 sqft", "1.2 lakh sqft"
AREA_RE = re.compile(
    r'([\d,]+(?:\.\d+)?)\s*(?:lakh\s+)?(?:sq\.?\s*ft\.?|sqft|sft)',
    re.IGNORECASE
)
AREA_LAKH_RE = re.compile(
    r'([\d,]+(?:\.\d+)?)\s*lakh\s+sq\.?\s*ft',
    re.IGNORECASE
)

# Micro-market mention in text
MARKET_RE = re.compile(
    r'\b(bkc|bandra\s+kurla|lower\s+parel|worli|andheri|powai|malad|goregaon|'
    r'kurla|vikhroli|thane|navi\s+mumbai|airoli|belapur|vashi|wadala|'
    r'hinjewadi|hitec\s+city|hitech|outer\s+ring\s+road|bangalore|bengaluru)\b',
    re.IGNORECASE
)

# Rent movement signals: "rents rose 8%", "asking rents up 5%", "rents softened"
RENT_MOVE_RE = re.compile(
    r'(?:rent|rental)s?\s+(?:rose|risen|increased|up|jumped|grew|'
    r'softened|declined|fell|down|dropped|compressed)\s*(?:by\s*)?([\d]+(?:\.\d+)?)?\s*%?',
    re.IGNORECASE
)


def extract_rent(text):
    for pat in (RENT_RE, RENT_RAW_RE):
        m = pat.search(text)
        if m:
            try:
                val = float(m.group(1).replace(',', ''))
                if 30 <= val <= 800:   # sanity: ₹30–₹800 psf/month is realistic
                    return val
            except (ValueError, AttributeError):
                pass
    return None


def extract_price_cr(text):
    m = PRICE_CR_RE.search(text)
    if m:
        try:
            val = float(m.group(1).replace(',', ''))
            if 1 <= val <= 5000:
                return val
        except (ValueError, AttributeError):
            pass
    return None


def extract_price_psf(text):
    """Extract price in ₹/sqft (from lakh/sqft quoted prices)."""
    m = PRICE_PSF_RE.search(text)
    if m:
        try:
            val = float(m.group(1).replace(',', '')) * 100000  # lakh → INR
            if 500 <= val <= 50000:
                return val
        except (ValueError, AttributeError):
            pass
    return None


def extract_yield(text):
    m = YIELD_RE.search(text)
    if m:
        try:
            val = float((m.group(1) or m.group(2)).replace(',', ''))
            if 3.0 <= val <= 20.0:   # sanity: 3–20% is realistic yield range
                return val
        except (ValueError, AttributeError):
            pass
    return None


def extract_area(text):
    m_lakh = AREA_LAKH_RE.search(text)
    if m_lakh:
        try:
            return float(m_lakh.group(1).replace(',', '')) * 100000
        except (ValueError, AttributeError):
            pass
    m = AREA_RE.search(text)
    if m:
        try:
            val = float(m.group(1).replace(',', ''))
            return val if val > 100 else val * 100000  # assume lakh sqft if small
        except (ValueError, AttributeError):
            pass
    return None


def normalise_market(raw):
    """Map scraped market name → canonical micro-market label."""
    if not raw:
        return None
    r = raw.lower().strip()
    for alias, canonical in MARKET_ALIASES.items():
        if alias in r:
            return canonical
    return raw.title()


def compute_cap_rate_from_listing(rent_psf, area_sqft, price_cr):
    """
    Compute cap rate from listing data.
    NOI = rent_psf × area × 11 effective months (1 month vacancy).
    """
    if not (rent_psf and area_sqft and price_cr):
        return None
    noi = rent_psf * area_sqft * 11
    price_inr = price_cr * 1e7
    if price_inr <= 0:
        return None
    cap = round(noi / price_inr * 100, 2)
    return cap if 2.0 <= cap <= 25.0 else None  # sanity bounds


def compute_cap_from_psf(rent_psf, price_psf_inr):
    """
    Compute cap rate when both rent and price are per-sqft.
    rent_psf: ₹ per sqft per month
    price_psf_inr: ₹ per sqft capital value (e.g. BKC = 75000)
    cap_rate = (rent_psf × 11 months) / price_psf_inr
    """
    if not (rent_psf and price_psf_inr and price_psf_inr > 0):
        return None
    cap = round(rent_psf * 11 / price_psf_inr * 100, 2)
    return cap if 2.0 <= cap <= 20.0 else None


# ─────────────────────────────────────────────────────────────
# SUPABASE WRITE
# ─────────────────────────────────────────────────────────────

def _supabase_insert_snapshot(snapshot):
    """Insert a single cap_rate_snapshot row via Supabase REST API."""
    url  = os.environ.get('SUPABASE_URL', '').rstrip('/')
    key  = os.environ.get('SUPABASE_SERVICE_ROLE_KEY', '') or os.environ.get('SUPABASE_ANON_KEY', '')
    if not url or not key:
        logger.warning('SUPABASE creds not set — snapshot not written')
        return False
    try:
        r = requests.post(
            f'{url}/rest/v1/cap_rate_snapshots',
            headers={
                'apikey': key,
                'Authorization': f'Bearer {key}',
                'Content-Type': 'application/json',
                'Prefer': 'return=minimal',
            },
            json=snapshot,
            timeout=15,
        )
        if r.status_code in (200, 201):
            return True
        logger.warning(f'Snapshot insert {r.status_code}: {r.text[:120]}')
        return False
    except Exception as e:
        logger.error(f'Snapshot insert error: {e}')
        return False


# ─────────────────────────────────────────────────────────────
# CRAWLER
# ─────────────────────────────────────────────────────────────

class CapRateMarketCrawler(BaseCrawler):
    """
    Weekly cap rate intelligence crawler.

    Collects real rent / yield data from news articles and property portals,
    aggregates by micro-market, and writes dated snapshots to cap_rate_snapshots.

    Each weekly run inserts a new snapshot row per market — the dashboard
    reads all rows and plots the trend, showing compression or expansion.
    """
    SOURCE_NAME = 'Cap Rate Market Intelligence'
    SOURCE_URL  = 'https://www.google.com'
    CATEGORY    = 'other'

    TODAY = date.today().isoformat()

    # ── Google News queries that surface real rent/yield data ──
    GNEWS_QUERIES = [
        # Rent movement
        ('rent_data',   'Mumbai office rent per sqft 2025 2026'),
        ('rent_data',   'BKC Lower Parel office rental per sqft asking'),
        ('rent_data',   'Andheri Powai Malad office rent psf 2025'),
        ('rent_data',   'Thane Navi Mumbai office rent per sqft yield'),
        # Yield / cap rate
        ('yield_data',  'Mumbai Grade A office yield cap rate 2025 2026'),
        ('yield_data',  'Mumbai commercial real estate yield investment 2025'),
        ('yield_data',  'India office market rental yield JLL Anarock 2025'),
        # Broker reports
        ('report',      'JLL India office market quarterly report 2025 2026'),
        ('report',      'Anarock commercial real estate Mumbai quarterly 2025'),
        ('report',      'Knight Frank India office market outlook 2025'),
        ('report',      'Colliers India office rent yield report 2025'),
        ('report',      'CBRE India office market 2025 2026 outlook'),
        # Rent movements
        ('movement',    'Mumbai BKC office rents rise 2025'),
        ('movement',    'Mumbai office rental market softening 2025'),
        ('movement',    'Grade A office vacancy Mumbai 2025'),
        # Pre-leased deals
        ('transaction', 'pre-leased office building sale Mumbai crore yield'),
        ('transaction', 'office building sale investment yield Mumbai 2025'),
        ('transaction', 'commercial property investment grade a yield Mumbai'),
    ]

    GNEWS_BASE = 'https://news.google.com/rss/search?q={q}&hl=en-IN&gl=IN&ceid=IN:en'

    # ── Property portal listings for observed cap rates ──
    PORTAL_URLS = [
        ('99acres',      'https://www.99acres.com/pre-leased-commercial-property-for-sale-in-mumbai-ffid-2/'),
        ('99acres',      'https://www.99acres.com/commercial-property-for-sale-in-mumbai-proptypes-1-30-ffid-2/'),
        ('MagicBricks',  'https://www.magicbricks.com/pre-leased-commercial-property-for-sale-in-mumbai'),
        ('MagicBricks',  'https://www.magicbricks.com/commercial-office-space-for-sale-in-mumbai-pppfs'),
        ('SquareYards',  'https://www.squareyards.com/commercial-office-space-for-sale-in-mumbai'),
    ]

    # ── Financial media RSS for CRE articles ──
    MEDIA_RSS = [
        ('Economic Times CRE', 'https://economictimes.indiatimes.com/industry/services/property-/-cstruction/rssfeeds/13358259.cms'),
        ('Economic Times',     'https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms'),
        ('Business Standard',  'https://www.business-standard.com/rss/finance-16.rss'),
        ('Mint',               'https://www.livemint.com/rss/companies'),
    ]

    def crawl(self):
        """
        Main entry point. Returns DistressEvents for the distress feed
        AND writes cap_rate_snapshot rows to DB directly.
        """
        session = FirecrawlSession()
        session.headers.update(self.HEADERS)

        # Accumulator: market → list of observed (rent_psf, cap_rate, source)
        observations = defaultdict(list)
        events = []

        # ── 1. Google News RSS ──────────────────────────────────
        self.logger.info('  ── Cap Rate: Google News scan')
        for query_type, query in self.GNEWS_QUERIES:
            url = self.GNEWS_BASE.format(q=quote(query))
            resp = self.safe_get(session, url, timeout=15)
            if not resp:
                continue

            try:
                import xml.etree.ElementTree as ET
                root = ET.fromstring(resp.text)
                items = list(root.iter('item'))
                hits = 0

                for item in items[:30]:
                    title = (item.findtext('title') or '').strip()
                    desc  = (item.findtext('description') or '').strip()
                    link  = (item.findtext('link') or '').strip()
                    pub   = (item.findtext('pubDate') or '').strip()
                    if not title:
                        continue

                    full = f'{title} {desc}'

                    # Find all micro-market mentions in article
                    market_hits = MARKET_RE.findall(full)
                    if not market_hits:
                        continue

                    rent  = extract_rent(full)
                    yield_pct = extract_yield(full)

                    if not rent and not yield_pct:
                        continue

                    # Check for rent movement modifiers
                    move_match = RENT_MOVE_RE.search(full)
                    move_note = move_match.group(0)[:60] if move_match else ''

                    for raw_market in set(market_hits):
                        market = normalise_market(raw_market)
                        if not market:
                            continue

                        obs = {
                            'market':     market,
                            'rent_psf':   rent,
                            'yield_pct':  yield_pct,
                            'source':     f'Google News / {query_type}',
                            'title':      title[:120],
                            'link':       link,
                            'pub':        pub[:50],
                            'note':       move_note,
                        }
                        observations[market].append(obs)
                        hits += 1

                        # Create a distress event for the feed (rent movement = market signal)
                        if move_note or yield_pct:
                            events.append(self.make_event(
                                company_name=f'CRE Market — {market}',
                                keyword='distressed_asset',
                                category='distressed_asset',
                                url=link,
                                headline=title,
                                snippet=desc[:400],
                                metadata={
                                    'market': market,
                                    'rent_psf': rent,
                                    'yield_pct': yield_pct,
                                    'movement': move_note,
                                    'query_type': query_type,
                                    'data_type': 'cap_rate_signal',
                                },
                            ))

                self.logger.info(f'    [{query[:45]}]: {len(items)} items → {hits} market signals')
            except Exception as e:
                self.logger.debug(f'GNews parse error [{query[:40]}]: {e}')
            time.sleep(0.7)

        # ── 2. Property portal listings ──────────────────────────
        self.logger.info('  ── Cap Rate: Property portal scan')
        portal_obs = defaultdict(list)

        for portal_name, url in self.PORTAL_URLS:
            resp = self.safe_get(session, url, timeout=20)
            if not resp:
                continue

            soup = BeautifulSoup(resp.text, 'html.parser')
            cards = (
                soup.select('.propertyCard, .srp__list--item, .mb-srp__card') or
                soup.select('[class*="property"], [class*="listing"]') or
                soup.find_all('article') or
                soup.find_all('li', class_=re.compile(r'property|listing|result', re.I))
            )

            card_count = 0
            for card in cards[:40]:
                text = card.get_text(separator=' ', strip=True)
                if len(text) < 60:
                    continue

                # Must be commercial / pre-leased
                if not any(w in text.lower() for w in [
                    'office', 'commercial', 'pre-leased', 'pre leased',
                    'investment', 'it park', 'business park', 'grade a',
                ]):
                    continue

                link_el = card.find('a', href=True)
                card_url = link_el['href'] if link_el else url
                if card_url and not card_url.startswith('http'):
                    card_url = 'https://' + url.split('/')[2] + card_url

                rent      = extract_rent(text)
                price_cr  = extract_price_cr(text)
                price_psf = extract_price_psf(text)
                area      = extract_area(text)

                # Detect market
                market_hits = MARKET_RE.findall(text)
                market = normalise_market(market_hits[0]) if market_hits else None
                if not market:
                    continue

                # Compute observed cap rate
                cap_rate = None
                if rent and area and price_cr:
                    cap_rate = compute_cap_rate_from_listing(rent, area, price_cr)
                elif rent and price_psf:
                    cap_rate = compute_cap_from_psf(rent, price_psf)

                if not (rent or cap_rate):
                    continue

                portal_obs[market].append({
                    'rent_psf':  rent,
                    'cap_rate':  cap_rate,
                    'price_cr':  price_cr,
                    'area':      area,
                    'source':    portal_name,
                    'url':       card_url,
                })
                card_count += 1

            self.logger.info(f'    {portal_name}: {len(cards)} cards → {card_count} priced listings')
            time.sleep(1.2)

        # ── 3. Financial media RSS for CRE rent articles ──────────
        self.logger.info('  ── Cap Rate: Financial media RSS')
        for media_name, feed_url in self.MEDIA_RSS:
            resp = self.safe_get(session, feed_url, timeout=15)
            if not resp:
                continue
            try:
                import xml.etree.ElementTree as ET
                root = ET.fromstring(resp.text)
                for item in list(root.iter('item'))[:50]:
                    title = (item.findtext('title') or '').strip()
                    desc  = (item.findtext('description') or '').strip()
                    link  = (item.findtext('link') or '').strip()
                    if not title:
                        continue

                    full = f'{title} {desc}'
                    full_lower = full.lower()

                    # Only CRE-relevant articles
                    if not any(w in full_lower for w in [
                        'office', 'commercial', 'rent', 'rental', 'yield',
                        'cap rate', 'grade a', 'pre-leased', 'realty',
                        'real estate commercial',
                    ]):
                        continue

                    rent = extract_rent(full)
                    yield_pct = extract_yield(full)
                    market_hits = MARKET_RE.findall(full)
                    if not market_hits or (not rent and not yield_pct):
                        continue

                    for raw_market in set(market_hits):
                        market = normalise_market(raw_market)
                        if market:
                            observations[market].append({
                                'market':    market,
                                'rent_psf':  rent,
                                'yield_pct': yield_pct,
                                'source':    media_name,
                                'title':     title[:120],
                                'link':      link,
                            })
            except Exception as e:
                self.logger.debug(f'Media RSS error [{media_name}]: {e}')
            time.sleep(0.6)

        # ── 4. Aggregate observations → snapshots ─────────────────
        self.logger.info('  ── Cap Rate: Aggregating and writing snapshots')
        snapshots_written = 0
        today = date.today().isoformat()

        # From Google News + media RSS
        for market, obs_list in observations.items():
            rents  = [o['rent_psf'] for o in obs_list if o.get('rent_psf')]
            yields = [o['yield_pct'] for o in obs_list if o.get('yield_pct')]

            if not rents and not yields:
                continue

            # Get baseline for this market to fill missing data
            baseline = next(
                (b for b in MARKET_BASELINE_Q1_2026 if b[0] == market),
                None
            )

            avg_rent     = round(sum(rents) / len(rents), 1) if rents else (baseline[3] if baseline else None)
            cap_rate_obs = round(sum(yields) / len(yields), 2) if yields else None

            # If we have observed rent but no yield, compute from baseline price
            if avg_rent and not cap_rate_obs and baseline:
                baseline_price_psf = baseline[4]
                cap_rate_obs = compute_cap_from_psf(avg_rent, baseline_price_psf)

            if not cap_rate_obs:
                continue

            source_names = list(set(o['source'] for o in obs_list))[:3]
            snapshot = {
                'micro_market':   market,
                'asset_class':    'grade_a_office',
                'cap_rate_pct':   cap_rate_obs,
                'avg_rent_psf':   avg_rent,
                'avg_price_psf':  baseline[4] if baseline else None,
                'sample_size':    len(obs_list),
                'snapshot_date':  today,
                'source':         ', '.join(source_names),
                'notes':          f'{len(obs_list)} data points from news/media',
            }
            if _supabase_insert_snapshot(snapshot):
                snapshots_written += 1
                self.logger.info(
                    f'    Snapshot: {market} cap={cap_rate_obs}% rent=₹{avg_rent} '
                    f'({len(obs_list)} observations)'
                )

        # From property portal listings (most reliable — real transaction data)
        for market, listings in portal_obs.items():
            rents  = [l['rent_psf'] for l in listings if l.get('rent_psf')]
            caps   = [l['cap_rate'] for l in listings if l.get('cap_rate')]
            prices = [l['price_cr'] for l in listings if l.get('price_cr')]

            if not rents and not caps:
                continue

            avg_rent = round(sum(rents) / len(rents), 1) if rents else None
            avg_cap  = round(sum(caps) / len(caps), 2) if caps else None
            portals  = list(set(l['source'] for l in listings))

            if not avg_cap and avg_rent:
                baseline = next(
                    (b for b in MARKET_BASELINE_Q1_2026 if b[0] == market), None
                )
                if baseline:
                    avg_cap = compute_cap_from_psf(avg_rent, baseline[4])

            if not avg_cap:
                continue

            snapshot = {
                'micro_market':   market,
                'asset_class':    'grade_a_office',
                'cap_rate_pct':   avg_cap,
                'avg_rent_psf':   avg_rent,
                'avg_price_psf':  None,
                'sample_size':    len(listings),
                'snapshot_date':  today,
                'source':         f'Listings: {", ".join(portals)}',
                'notes':          f'{len(listings)} pre-leased listings observed',
            }
            if _supabase_insert_snapshot(snapshot):
                snapshots_written += 1
                self.logger.info(
                    f'    Portal snapshot: {market} cap={avg_cap}% '
                    f'({len(listings)} listings)'
                )

        # ── 5. Quarterly baseline refresh ──────────────────────────
        # Always insert fresh baseline rows so the DB always has today's date
        # even if scrapers found nothing new. These use our known Q1 2026 values.
        # The dashboard shows these as the "floor" data series.
        baseline_written = 0
        for (mkt, asset_cls, cap, rent, price_psf, note) in MARKET_BASELINE_Q1_2026:
            # Only write baseline if we didn't already write a scraped snapshot
            if mkt in observations or mkt in portal_obs:
                continue  # scraped data already written — don't overwrite

            snapshot = {
                'micro_market':   mkt,
                'asset_class':    asset_cls,
                'cap_rate_pct':   cap,
                'avg_rent_psf':   rent,
                'avg_price_psf':  price_psf,
                'sample_size':    1,
                'snapshot_date':  today,
                'source':         'Market Intel Q1 2026',
                'notes':          note,
            }
            if _supabase_insert_snapshot(snapshot):
                baseline_written += 1

        self.logger.info(
            f'Cap Rate Market Intelligence: {len(events)} signals | '
            f'{snapshots_written} scraped snapshots + {baseline_written} baseline snapshots written'
        )
        return events
