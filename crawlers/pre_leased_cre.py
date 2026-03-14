"""
crawlers/pre_leased_cre.py
═══════════════════════════════════════════════════════════════════════════
Pre-Leased Commercial Real Estate Tracker — Mumbai / MMR Market

Why this matters (from deal intel):
  - International investors (London-based) want Grade A office portfolios
    at 8.5–9% cap rate expectations
  - Mumbai Grade A currently offers only 6–7% cap rate
  - Strategy: convince via 3 escalations over 10 years @ 15% avg
  - Malad (pre-leased, reputed tenant) = CLEAN deal, easy execution
  - Lotus Building = strata complications, needs right buyer/manager
  - Build DATABASE of pre-leased assets to pitch before desperation sets in

This crawler tracks:
  1. 99acres pre-leased commercial listings
  2. MagicBricks commercial investment properties
  3. Anarock / Colliers / JLL India market reports
  4. PropEquity Grade A office vacancy data (via press/news)
  5. Cap rate calculator enrichment on all found assets

Cap Rate Formula:
  NOI / Current Market Value = Cap Rate
  For Mumbai Grade A: typical NOI = rent × 11 months (1 month vacancy)
  Escalation: 15% every 3 years per standard IPC lease terms
═══════════════════════════════════════════════════════════════════════════
"""

import re
import time
import math
import logging
import requests
from .firecrawl_client import FirecrawlSession
from bs4 import BeautifulSoup
from urllib.parse import quote
from .base import BaseCrawler, DistressEvent

logger = logging.getLogger(__name__)

# ─── CAP RATE ENGINE ─────────────────────────────────────────────────────

# Mumbai micro-market rent benchmarks (₹ per sqft per month, as of 2024-25)
MUMBAI_RENT_BENCHMARKS = {
    'bkc':           {'grade_a': 350, 'grade_b': 200},
    'lower parel':   {'grade_a': 280, 'grade_b': 160},
    'worli':         {'grade_a': 260, 'grade_b': 150},
    'andheri':       {'grade_a': 150, 'grade_b': 100},
    'powai':         {'grade_a': 130, 'grade_b': 85},
    'malad':         {'grade_a': 115, 'grade_b': 75},
    'goregaon':      {'grade_a': 120, 'grade_b': 78},
    'kurla':         {'grade_a': 110, 'grade_b': 70},
    'vikhroli':      {'grade_a': 100, 'grade_b': 65},
    'thane':         {'grade_a': 80,  'grade_b': 55},
    'navi mumbai':   {'grade_a': 75,  'grade_b': 50},
    'airoli':        {'grade_a': 70,  'grade_b': 48},
    'vashi':         {'grade_a': 80,  'grade_b': 52},
    'belapur':       {'grade_a': 65,  'grade_b': 45},
    'wadala':        {'grade_a': 130, 'grade_b': 85},
    'default':       {'grade_a': 120, 'grade_b': 75},
}

# Typical cap rate ranges Mumbai market
CAP_RATE_BENCHMARKS = {
    'grade_a_core': (0.055, 0.070),   # 5.5–7.0% Grade A core
    'grade_b':      (0.070, 0.085),   # 7.0–8.5% Grade B
    'suburban':     (0.080, 0.095),   # 8.0–9.5% suburban
    'distressed':   (0.095, 0.130),   # 9.5%+ distressed / strata
}

# Lease escalation assumptions
STANDARD_ESCALATION_RATE   = 0.15   # 15% per 3 years
STANDARD_ESCALATION_YEARS  = 3      # every 3 years
PROJECTION_YEARS           = 10     # 10 year hold period


def compute_cap_rate(
    rent_per_sqft_monthly: float,
    area_sqft: float,
    price_crore: float,
    vacancy_months: float = 1.0,
) -> dict:
    """
    Compute cap rate and 10-year return projection.
    Returns a dict with cap_rate, noi_annual, yield_on_cost, irr_estimate.
    """
    if not all([rent_per_sqft_monthly, area_sqft, price_crore]):
        return {}

    price_inr = price_crore * 1e7

    # NOI = annual rent × (12 - vacancy months)
    gross_annual = rent_per_sqft_monthly * area_sqft * 12
    effective_months = max(0, 12 - vacancy_months)
    noi = rent_per_sqft_monthly * area_sqft * effective_months

    cap_rate = noi / price_inr if price_inr > 0 else 0

    # 10-year cashflow projection with 15% escalation every 3 years
    total_cashflow = 0
    current_rent = rent_per_sqft_monthly
    for year in range(1, PROJECTION_YEARS + 1):
        if year > 1 and (year - 1) % STANDARD_ESCALATION_YEARS == 0:
            current_rent *= (1 + STANDARD_ESCALATION_RATE)
        year_noi = current_rent * area_sqft * effective_months
        total_cashflow += year_noi

    # Simple yield on cost (undiscounted)
    yield_on_cost_10yr = total_cashflow / price_inr if price_inr > 0 else 0

    # Rough IRR estimate using simple NPV approximation
    # Assumes terminal value = Year 10 NOI / exit cap rate (6%)
    final_noi = current_rent * area_sqft * effective_months
    terminal_value = final_noi / 0.065
    total_return = total_cashflow + terminal_value
    irr_estimate = (total_return / price_inr) ** (1 / PROJECTION_YEARS) - 1

    return {
        'cap_rate_pct': round(cap_rate * 100, 2),
        'noi_annual_cr': round(noi / 1e7, 3),
        'gross_rent_annual_cr': round(gross_annual / 1e7, 3),
        'yield_on_cost_10yr_pct': round(yield_on_cost_10yr * 100, 1),
        'irr_estimate_pct': round(irr_estimate * 100, 1),
        'meets_investor_threshold': cap_rate >= 0.085,  # 8.5%+ target
        'upside_to_threshold_pct': round((0.085 - cap_rate) * 100, 2),
    }


def get_location_rent(location: str, grade: str = 'grade_a') -> float:
    """Get benchmark rent for a location."""
    if not location:
        return MUMBAI_RENT_BENCHMARKS['default'][grade]
    loc_lower = location.lower()
    for key, rents in MUMBAI_RENT_BENCHMARKS.items():
        if key in loc_lower or loc_lower in key:
            return rents.get(grade, rents['grade_b'])
    return MUMBAI_RENT_BENCHMARKS['default'][grade]


# ─── Area extractor ─────────────────────────────────────────────────────
AREA_RE = re.compile(
    r'([\d,]+(?:\.\d+)?)\s*(?:sq\.?\s*ft\.?|sqft|square\s*feet|sft)',
    re.IGNORECASE
)

PRICE_RE = re.compile(
    r'(?:rs\.?|₹|inr)?\s*([\d,]+(?:\.\d+)?)\s*(crore|cr\.?|lakh|lac|lakhs)',
    re.IGNORECASE
)

RENT_RE = re.compile(
    r'(?:rent|rental|lease).*?([\d,]+(?:\.\d+)?)\s*(?:per\s+sqft|psf|\/sqft)',
    re.IGNORECASE
)


def extract_area(text: str) -> float | None:
    m = AREA_RE.search(text)
    if m:
        try:
            return float(m.group(1).replace(',', ''))
        except ValueError:
            pass
    return None


def extract_price(text: str) -> float | None:
    m = PRICE_RE.search(text)
    if m:
        try:
            val = float(m.group(1).replace(',', ''))
            unit = m.group(2).lower()
            return val if 'cr' in unit else val / 100
        except ValueError:
            pass
    return None


def extract_rent_psf(text: str) -> float | None:
    m = RENT_RE.search(text)
    if m:
        try:
            return float(m.group(1).replace(',', ''))
        except ValueError:
            pass
    return None


# ─── TENANT QUALITY SCORER ───────────────────────────────────────────────
# Meeting note: Malad is clean because it's leased to a "reputed institution"
# Tenant quality directly affects investor appetite and pricing

BLUE_CHIP_TENANTS = [
    # IT / Tech
    'tcs', 'infosys', 'wipro', 'hcl', 'cognizant', 'accenture', 'ibm',
    'capgemini', 'tech mahindra', 'oracle', 'microsoft', 'google',
    'amazon', 'flipkart', 'paytm',
    # BFSI
    'hdfc bank', 'icici bank', 'sbi', 'axis bank', 'kotak', 'citi',
    'morgan stanley', 'jp morgan', 'goldman sachs', 'deutsche bank',
    'standard chartered', 'hsbc', 'bnp paribas', 'barclays',
    # Insurance / NBFC
    'lic', 'sbi life', 'hdfc life', 'bajaj finance', 'aditya birla',
    # Consulting
    'mckinsey', 'bcg', 'deloitte', 'kpmg', 'pwc', 'ey',
    # Govt / PSU
    'sebi', 'rbi', 'nse', 'bse', 'irdai', 'government of india',
]

GOOD_TENANTS = [
    'mnc', 'fortune 500', 'listed company', 'public sector', 'psu',
    'bank', 'insurance', 'nbfc', 'it company', 'software company',
    'pharma', 'healthcare',
]


def score_tenant(text: str) -> tuple[int, str]:
    """Returns (score 0-100, tenant_category)."""
    t = text.lower()
    for tenant in BLUE_CHIP_TENANTS:
        if tenant in t:
            return 95, 'blue_chip'
    for kw in GOOD_TENANTS:
        if kw in t:
            return 70, 'institutional'
    if any(w in t for w in ['pre-leased', 'pre leased', 'leased to', 'occupied by']):
        return 55, 'pre_leased_unknown_tenant'
    return 30, 'unknown'


# ─── CRAWLERS ─────────────────────────────────────────────────────────────

class PreLeasedCommercialCrawler(BaseCrawler):
    """
    Crawls 99acres and MagicBricks for pre-leased commercial properties.
    Enriches every listing with cap rate calculation and tenant scoring.
    Flags assets that meet the 8.5%+ cap rate target for international investors.
    """
    SOURCE_NAME = 'Pre-Leased CRE'
    SOURCE_URL = 'https://www.99acres.com'
    CATEGORY = 'other'

    CRAWL_URLS = [
        # 99acres pre-leased commercial
        'https://www.99acres.com/commercial-property-for-sale-in-mumbai-proptypes-1-30-ffid-2/',
        'https://www.99acres.com/pre-leased-commercial-property-for-sale-in-mumbai-ffid-2/',
        'https://www.99acres.com/investment-commercial-property-for-sale-in-thane-ffid-2/',
        # MagicBricks
        'https://www.magicbricks.com/commercial-office-space-for-sale-in-mumbai-pppfs',
        'https://www.magicbricks.com/pre-leased-commercial-property-for-sale-in-mumbai',
        # SquareYards
        'https://www.squareyards.com/commercial-office-space-for-sale-in-mumbai',
    ]

    # Google News feeds for pre-leased deal announcements
    GNEWS_QUERIES = [
        'pre-leased office space sale Mumbai crore 2024',
        'Grade A office building sale Mumbai pre-leased',
        'pre-leased commercial property Mumbai investment 2024',
        'office building sale Malad Andheri BKC Mumbai crore',
        'strata office sale Mumbai Grade A crore 2024',
        'commercial building sale Kurla Thane Navi Mumbai',
        'pre-leased retail shop Mumbai investment yield',
    ]

    GNEWS_BASE = 'https://news.google.com/rss/search?q={q}&hl=en-IN&gl=IN&ceid=IN:en'

    def crawl(self) -> list[DistressEvent]:
        events = []
        session = FirecrawlSession()

        # 1. Direct property portal crawl
        for url in self.CRAWL_URLS:
            resp = self.safe_get(session, url)
            if not resp:
                continue

            soup = BeautifulSoup(resp.text, 'html.parser')

            # Extract property cards
            cards = (
                soup.select('.propertyCard') or
                soup.select('.srp__list--item') or
                soup.select('.mb-srp__card') or
                soup.select('[class*="property"]') or
                soup.select('[class*="listing"]') or
                soup.find_all('article')
            )

            for card in cards[:self.MAX_ARTICLES]:
                text = card.get_text(separator=' ', strip=True)
                if len(text) < 50:
                    continue

                # Only commercial / investment properties
                if not any(w in text.lower() for w in [
                    'office', 'commercial', 'pre-leased', 'pre leased',
                    'investment', 'shop', 'showroom', 'it park',
                ]):
                    continue

                link = card.find('a', href=True)
                prop_url = self.SOURCE_URL
                if link:
                    href = link['href']
                    prop_url = href if href.startswith('http') else self.SOURCE_URL + href

                # Extract financial signals
                price       = extract_price(text)
                area        = extract_area(text)
                rent_psf    = extract_rent_psf(text)
                from .multi_bank_auctions import detect_mmr_location
                location    = detect_mmr_location(text)

                # Infer rent from benchmark if not stated
                if not rent_psf and location:
                    grade = 'grade_a' if any(
                        w in text.lower() for w in ['grade a', 'grade-a', 'a grade', 'premium', 'it park']
                    ) else 'grade_b'
                    rent_psf = get_location_rent(location, grade)

                # Cap rate computation
                cap_rate_data = {}
                if price and area and rent_psf:
                    cap_rate_data = compute_cap_rate(rent_psf, area, price)

                # Tenant scoring
                tenant_score, tenant_cat = score_tenant(text)

                # Overall deal score
                deal_score = 0
                if cap_rate_data.get('cap_rate_pct', 0) >= 8.5:
                    deal_score += 50
                elif cap_rate_data.get('cap_rate_pct', 0) >= 7.0:
                    deal_score += 30
                deal_score += tenant_score // 4  # max +25
                if location:
                    deal_score += 15
                if price and 10 <= price <= 300:
                    deal_score += 10  # sweet spot deal size

                # Only store if there's genuine signal
                if deal_score < 20 and not cap_rate_data:
                    continue

                events.append(self.make_event(
                    company_name=f'Pre-Leased CRE — {location or "Mumbai"}',
                    keyword='pre_leased_asset',
                    category='other',
                    url=prop_url,
                    headline=text[:200],
                    snippet=text[:600],
                    metadata={
                        'asset_type': 'pre_leased_commercial',
                        'location': location,
                        'area_sqft': area,
                        'price_crore': price,
                        'rent_psf': rent_psf,
                        'cap_rate_data': cap_rate_data,
                        'tenant_score': tenant_score,
                        'tenant_category': tenant_cat,
                        'deal_score': deal_score,
                        'meets_investor_threshold': cap_rate_data.get('meets_investor_threshold', False),
                        'source_portal': url.split('/')[2],
                    },
                ))

            self.logger.info(f'  {url.split("/")[2]}: {len(cards)} cards parsed')
            time.sleep(1.2)

        # 2. Google News for deal announcements
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
                    price  = extract_price(full)
                    area   = extract_area(full)
                    from .multi_bank_auctions import detect_mmr_location
                    loc    = detect_mmr_location(full)

                    cap_rate_data = {}
                    if price and area:
                        rent_psf = get_location_rent(loc or 'default')
                        cap_rate_data = compute_cap_rate(rent_psf, area, price)

                    tenant_score, tenant_cat = score_tenant(full)

                    events.append(self.make_event(
                        company_name=f'CRE Signal — {loc or "Mumbai"}',
                        keyword='pre_leased_asset',
                        category='other',
                        url=link,
                        headline=title,
                        snippet=desc[:400],
                        metadata={
                            'asset_type': 'pre_leased_cre_news',
                            'location': loc,
                            'price_crore': price,
                            'area_sqft': area,
                            'cap_rate_data': cap_rate_data,
                            'tenant_score': tenant_score,
                            'tenant_category': tenant_cat,
                            'gnews_query': query,
                        },
                    ))
            except Exception as e:
                self.logger.debug(f'GNews CRE error [{query[:40]}]: {e}')
            time.sleep(0.8)

        self.logger.info(f'Pre-Leased CRE: {len(events)} events')
        return events


class GradeAOfficeVacancyCrawler(BaseCrawler):
    """
    Tracks Grade A office vacancy signals — buildings that have been
    vacant for extended periods become motivated sellers (like Lotus building).
    
    Sources: IPC research reports via Google News, PropEquity signals,
    JLL / Anarock / Colliers quarterly reports.
    
    Meeting insight: Lotus building vacant for long time → strata complications
    → find right person to manage → leasing easier than sale once strata resolved.
    """
    SOURCE_NAME = 'Grade A Office Vacancy'
    SOURCE_URL = 'https://www.google.com'
    CATEGORY = 'other'

    GNEWS_QUERIES = [
        'Grade A office vacant Mumbai sale 2024',
        'vacant office building Mumbai sale motivated seller',
        'strata office building Mumbai sale complex',
        'JLL office vacancy Mumbai Grade A 2024',
        'Anarock commercial office vacancy Mumbai quarterly',
        'Colliers office market Mumbai vacancy report',
        'office building Mumbai unsold strata floors',
        'commercial real estate distress sale Mumbai 2024',
        'PE fund commercial office exit Mumbai 2024',
        'family office commercial real estate Mumbai sale',
    ]

    GNEWS_BASE = 'https://news.google.com/rss/search?q={q}&hl=en-IN&gl=IN&ceid=IN:en'

    # IPC / research firm report URLs
    RESEARCH_SOURCES = [
        ('JLL India', 'https://www.jll.co.in/en/trends-and-insights/research/rss'),
        ('Knight Frank India', 'https://www.knightfrank.co.in/blog/rss'),
        ('Colliers India', 'https://www.colliers.com/en-in/research/rss'),
        ('Anarock', 'https://www.anarock.com/blog/feed/'),
        ('CBRE India', 'https://www.cbre.co.in/insights/rss'),
    ]

    def crawl(self) -> list[DistressEvent]:
        events = []
        session = FirecrawlSession()

        # 1. IPC research RSS
        for source_name, feed_url in self.RESEARCH_SOURCES:
            resp = self.safe_get(session, feed_url)
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

                    full = f'{title} {desc}'.lower()
                    # Look for vacancy / distress signals
                    if not any(w in full for w in [
                        'vacant', 'vacancy', 'distress', 'motivated seller',
                        'sale', 'exit', 'portfolio', 'strata', 'npa office',
                        'unsold', 'for sale', 'investment opportunity',
                    ]):
                        continue

                    from .multi_bank_auctions import detect_mmr_location
                    loc = detect_mmr_location(full)
                    price = extract_price(f'{title} {desc}')
                    area = extract_area(f'{title} {desc}')

                    events.append(self.make_event(
                        company_name=f'Office Vacancy Signal — {source_name}',
                        keyword='distressed_asset',
                        category='distressed_asset',
                        url=link,
                        headline=title,
                        snippet=desc[:400],
                        metadata={
                            'asset_type': 'grade_a_office',
                            'source': source_name,
                            'location': loc,
                            'price_crore': price,
                            'area_sqft': area,
                            'signal_type': 'vacancy_report',
                        },
                    ))
            except Exception as e:
                self.logger.debug(f'{source_name} feed error: {e}')
            time.sleep(0.8)

        # 2. Google News vacancy signals
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
                    from .multi_bank_auctions import detect_mmr_location
                    loc = detect_mmr_location(full)
                    price = extract_price(full)
                    tenant_score, tenant_cat = score_tenant(full)

                    events.append(self.make_event(
                        company_name=f'Grade A Vacancy — {loc or "Mumbai"}',
                        keyword='distressed_asset',
                        category='distressed_asset',
                        url=link,
                        headline=title,
                        snippet=desc[:400],
                        metadata={
                            'asset_type': 'grade_a_office_vacancy',
                            'location': loc,
                            'price_crore': price,
                            'tenant_score': tenant_score,
                            'gnews_query': query,
                            'signal_type': 'vacancy_news',
                        },
                    ))
            except Exception as e:
                self.logger.debug(f'GNews vacancy error: {e}')
            time.sleep(0.8)

        self.logger.info(f'Grade A Office Vacancy: {len(events)} events')
        return events
