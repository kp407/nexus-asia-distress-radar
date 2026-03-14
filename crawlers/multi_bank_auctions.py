"""
crawlers/multi_bank_auctions.py
═══════════════════════════════════════════════════════════════════════════
Crawls Indian bank auction portals for SARFAESI & NPA property auctions.

SOURCE HIERARCHY (best → fallback):
  Tier 1 — Aggregators (structured data, high yield):
    • IBAPI JSON API      — RBI-mandated portal, all PSU banks, REST JSON
    • BankAuctions.co.in  — third-party aggregator, scrapeable HTML cards
    • Sarfaesi.com        — SARFAESI notice aggregator, borrower names

  Tier 2 — Direct bank portals (HTML, hit-or-miss per bank):
    • Bank of Baroda, PNB, Canara, Union, BoM, Central, IOB

  Tier 3 — PDF link harvesters (title + date metadata only):
    • SBI — publishes PDFs; we capture link title + auction date

Why this matters:
  • Banks publish SARFAESI auction notices with reserve price, property
    address, auction date — fastest path to desperate-seller inventory
  • Sweet spot: commercial + MMR + 5–150 crore = Grade A office / retail
    strata units at 20–40% below market
═══════════════════════════════════════════════════════════════════════════
"""

import re
import json
import logging
import requests
from bs4 import BeautifulSoup
from .base import BaseCrawler, DistressEvent

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# SHARED CONSTANTS
# ─────────────────────────────────────────────────────────────

PRICE_RE = re.compile(
    r'(?:rs\.?|₹|inr|reserve\s+price[:\s]*)?\s*([\d,]+(?:\.\d+)?)\s*(crore|cr\.?|lakh|lac|lakhs|l\b)',
    re.IGNORECASE
)

ALL_LOCATIONS = {
    # MMR micro-markets
    'mumbai': 'Mumbai', 'thane': 'Thane', 'navi mumbai': 'Navi Mumbai',
    'kalyan': 'Kalyan', 'dombivli': 'Dombivli', 'panvel': 'Panvel',
    'vasai': 'Vasai', 'virar': 'Virar', 'mira road': 'Mira Road',
    'bhiwandi': 'Bhiwandi', 'kurla': 'Kurla', 'bandra': 'Bandra',
    'andheri': 'Andheri', 'malad': 'Malad', 'goregaon': 'Goregaon',
    'borivali': 'Borivali', 'kandivali': 'Kandivali', 'dahisar': 'Dahisar',
    'mulund': 'Mulund', 'ghatkopar': 'Ghatkopar', 'chembur': 'Chembur',
    'wadala': 'Wadala', 'worli': 'Worli', 'lower parel': 'Lower Parel',
    'bkc': 'BKC', 'dharavi': 'Dharavi', 'sion': 'Sion',
    'matunga': 'Matunga', 'dadar': 'Dadar', 'prabhadevi': 'Prabhadevi',
    'powai': 'Powai', 'vikhroli': 'Vikhroli', 'kanjurmarg': 'Kanjurmarg',
    'bhandup': 'Bhandup', 'airoli': 'Airoli', 'belapur': 'Belapur',
    'kharghar': 'Kharghar', 'ulwe': 'Ulwe', 'nahur': 'Nahur',
    # Other metros
    'pune': 'Pune', 'pimpri': 'Pimpri', 'chinchwad': 'Chinchwad',
    'delhi': 'Delhi', 'noida': 'Noida', 'gurgaon': 'Gurgaon',
    'gurugram': 'Gurugram', 'faridabad': 'Faridabad', 'ghaziabad': 'Ghaziabad',
    'bengaluru': 'Bengaluru', 'bangalore': 'Bangalore', 'whitefield': 'Whitefield',
    'hyderabad': 'Hyderabad', 'secunderabad': 'Secunderabad',
    'chennai': 'Chennai', 'ahmedabad': 'Ahmedabad', 'surat': 'Surat',
    'kolkata': 'Kolkata', 'jaipur': 'Jaipur', 'lucknow': 'Lucknow',
    'chandigarh': 'Chandigarh', 'kochi': 'Kochi', 'indore': 'Indore',
}

MMR_CITIES = {
    'mumbai', 'thane', 'navi mumbai', 'kalyan', 'dombivli', 'panvel',
    'vasai', 'virar', 'mira road', 'bhiwandi', 'kurla', 'bandra',
    'andheri', 'malad', 'goregaon', 'borivali', 'kandivali', 'dahisar',
    'mulund', 'ghatkopar', 'chembur', 'wadala', 'worli', 'lower parel',
    'bkc', 'powai', 'vikhroli', 'kanjurmarg', 'bhandup', 'airoli',
    'belapur', 'kharghar', 'ulwe', 'dadar', 'prabhadevi', 'sion',
}

COMMERCIAL_KW = [
    'office', 'commercial', 'shop', 'showroom', 'godown', 'warehouse',
    'factory', 'industrial', 'it park', 'it/ites', 'bpo', 'business park',
    'mall', 'retail', 'plaza', 'complex', 'premises', 'unit', 'floor',
    'wing', 'building', 'tower', 'shed',
]
RESIDENTIAL_KW = [
    'flat', 'apartment', 'bungalow', 'villa', 'residential', 'row house',
    'tenement', 'chawl', '1bhk', '2bhk', '3bhk', '4bhk', 'duplex',
]
LAND_KW = [
    'open land', 'na plot', 'agricultural land', 'vacant land', 'n.a. plot',
]

BANK_NAME_RE = re.compile(
    r'\b(State Bank of India|SBI|HDFC Bank|ICICI Bank|Axis Bank|'
    r'Bank of Baroda|Punjab National Bank|PNB|Canara Bank|'
    r'Union Bank|Bank of Maharashtra|Central Bank of India|'
    r'Indian Bank|Indian Overseas Bank|IOB|UCO Bank|'
    r'Bank of India|IDBI|Federal Bank|Kotak|YES Bank)\b',
    re.IGNORECASE
)

DATE_RE = re.compile(
    r'(\d{1,2}[\/\-\.]\d{1,2}[\/\-\.]\d{2,4})'
    r'|(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{4})',
    re.IGNORECASE
)

AUCTION_SIGNALS = {
    'auction', 'sarfaesi', 'reserve price', 'e-auction', 'e auction',
    'sale notice', 'possession notice', 'npa property', 'bid document',
}


# ─────────────────────────────────────────────────────────────
# SHARED UTILITIES
# ─────────────────────────────────────────────────────────────

def extract_price_inr(text):
    match = PRICE_RE.search(text)
    if not match:
        return None
    try:
        amount = float(match.group(1).replace(',', ''))
        unit = match.group(2).lower()
        if unit in ('crore', 'cr', 'cr.'):
            return round(amount, 2)
        elif unit in ('lakh', 'lac', 'lakhs', 'l'):
            return round(amount / 100, 4)
    except (ValueError, AttributeError):
        pass
    return None


def detect_location(text):
    """Returns (location_label, is_mmr). Longest match wins."""
    t = text.lower()
    for key in sorted(ALL_LOCATIONS.keys(), key=len, reverse=True):
        if key in t:
            return ALL_LOCATIONS[key], (key in MMR_CITIES)
    return None, False


def detect_asset_class(text):
    t = text.lower()
    for kw in COMMERCIAL_KW:
        if kw in t:
            return 'commercial'
    for kw in LAND_KW:
        if kw in t:
            return 'land'
    for kw in RESIDENTIAL_KW:
        if kw in t:
            return 'residential'
    return 'other'


def extract_bank_name(text):
    m = BANK_NAME_RE.search(text)
    return m.group(0).strip() if m else 'Bank'


def extract_auction_date(text):
    m = DATE_RE.search(text)
    if m:
        return (m.group(1) or m.group(2)).strip()
    return None


def deal_score(price, location, is_mmr, asset_class):
    s = 0
    if asset_class == 'commercial':
        s += 40
    elif asset_class == 'land':
        s += 20
    if is_mmr:
        s += 30
    elif location:
        s += 15
    if price:
        s += 30 if 5 <= price <= 150 else (5 if price < 5 else 15)
    return s


def parse_html_auction_page(soup, source_url, bank_name, min_score=0):
    results = []
    CARD_SELS = [
        '.property-card', '.auction-card', '.listing-card',
        '.property-item', '.auction-item', '.listing-item',
    ]
    candidates = []
    for sel in CARD_SELS:
        candidates = soup.select(sel)
        if candidates:
            break
    if not candidates:
        candidates = (soup.select('article') or
                      soup.find_all('tr') or
                      soup.find_all('li'))

    for tag in candidates:
        text = tag.get_text(separator=' ', strip=True)
        if len(text) < 40:
            continue
        t_lower = text.lower()
        if not any(sig in t_lower for sig in AUCTION_SIGNALS):
            continue

        link = tag.find('a', href=True)
        url = source_url
        if link:
            href = link['href']
            url = href if href.startswith('http') else source_url.rstrip('/') + '/' + href.lstrip('/')

        price = extract_price_inr(text)
        location, is_mmr = detect_location(text)
        asset_class = detect_asset_class(text)
        auction_date = extract_auction_date(text)
        score = deal_score(price, location, is_mmr, asset_class)

        if score < min_score:
            continue

        results.append({
            'text': text[:600], 'url': url,
            'price_crore': price, 'location': location, 'is_mmr': is_mmr,
            'asset_class': asset_class, 'auction_date': auction_date,
            'bank': bank_name, 'deal_score': score,
        })
    return results


def row_to_event(crawler, row):
    loc_label   = f' — {row["location"]}' if row['location'] else ''
    asset_label = row['asset_class'].title()
    company     = f'{row["bank"]} Auction{loc_label} [{asset_label}]'
    headline    = row['text'][:200]
    if row['price_crore']:
        headline = f'₹{row["price_crore"]} Cr | {headline}'
    return crawler.make_event(
        company_name=company,
        keyword='asset auction',
        category='auction',
        url=row['url'],
        headline=headline,
        snippet=row['text'],
        metadata={
            'bank': row['bank'],
            'reserve_price_crore': row['price_crore'],
            'location': row['location'],
            'is_mmr': row['is_mmr'],
            'asset_class': row['asset_class'],
            'auction_date': row['auction_date'],
            'deal_score': row['deal_score'],
        },
    )


# ─────────────────────────────────────────────────────────────
# TIER 1 — AGGREGATORS
# ─────────────────────────────────────────────────────────────

class IBAPIAuctionCrawler(BaseCrawler):
    """
    IBAPI — RBI-mandated Indian Banks Auction Portal.
    Covers all PSU + major private banks in one place.

    Strategy:
      1. Try JSON REST endpoint (richest data, structured fields)
      2. Try alternate JSON path
      3. Fall back to HTML scraping of homepage
    """
    SOURCE_NAME = 'IBAPI Auctions'
    SOURCE_URL  = 'https://ibapi.in'
    CATEGORY    = 'auction'

    JSON_ENDPOINTS = [
        'https://ibapi.in/Auctions/GetAuctions?type=active&pageSize=200',
        'https://ibapi.in/api/v1/auctions?status=active&limit=200',
        'https://ibapi.in/Auctions/GetAuctions?type=active',
    ]

    # IBAPI field name variants across API versions
    FIELD_MAPS = [
        ('reservePrice',  'propertyDescription', 'bankName',  'auctionDate', 'city',     'auctionUrl'),
        ('reserve_price', 'property_desc',        'bank_name', 'auction_date','location', 'url'),
        ('ReservePrice',  'PropertyDescription',  'BankName',  'AuctionDate', 'City',     'Url'),
        ('amount',        'description',           'lender',    'date',        'city',     'link'),
    ]

    def crawl(self):
        events  = []
        session = requests.Session()
        session.headers.update({
            **self.HEADERS,
            'Accept': 'application/json, text/html, */*',
            'X-Requested-With': 'XMLHttpRequest',
        })

        for endpoint in self.JSON_ENDPOINTS:
            resp = self.safe_get(session, endpoint, timeout=15)
            if not resp:
                continue
            try:
                data = resp.json()
                if isinstance(data, dict):
                    auctions = (data.get('data') or data.get('auctions') or
                                data.get('result') or data.get('items') or [])
                elif isinstance(data, list):
                    auctions = data
                else:
                    continue
                if auctions:
                    self.logger.info(f'IBAPI JSON: {len(auctions)} auctions from {endpoint}')
                    events = self._parse_json(auctions)
                    break
            except (json.JSONDecodeError, ValueError):
                continue

        if not events:
            self.logger.info('IBAPI JSON unavailable — trying HTML')
            resp = self.safe_get(session, self.SOURCE_URL, timeout=15)
            if resp:
                soup = BeautifulSoup(resp.text, 'html.parser')
                rows = parse_html_auction_page(soup, self.SOURCE_URL, 'IBAPI', min_score=0)
                events = [row_to_event(self, r) for r in rows[:self.MAX_ARTICLES]]

        self.logger.info(f'IBAPI: {len(events)} events')
        return events

    def _parse_json(self, auctions):
        events = []
        for item in auctions[:self.MAX_ARTICLES * 2]:
            if not isinstance(item, dict):
                continue

            price_raw = prop_desc = bank = auction_date = city = item_url = None
            for pf, df, bf, dtf, cf, uf in self.FIELD_MAPS:
                if pf in item or df in item:
                    price_raw    = item.get(pf)
                    prop_desc    = item.get(df, '')
                    bank         = item.get(bf, 'IBAPI Bank')
                    auction_date = item.get(dtf)
                    city         = item.get(cf)
                    item_url     = item.get(uf, self.SOURCE_URL)
                    break

            if not prop_desc:
                prop_desc = ' '.join(str(v) for v in item.values() if v)

            # Normalise price: IBAPI stores raw INR (e.g. 15000000 = 1.5 cr)
            price_crore = None
            if price_raw:
                try:
                    raw = float(str(price_raw).replace(',', ''))
                    price_crore = round(raw / 1e7, 2) if raw > 1000 else round(raw, 2)
                except (ValueError, TypeError):
                    price_crore = extract_price_inr(str(price_raw))

            if city:
                location, is_mmr = detect_location(str(city))
            else:
                location, is_mmr = detect_location(prop_desc)

            asset_class = detect_asset_class(prop_desc)
            score       = deal_score(price_crore, location, is_mmr, asset_class)

            if not item_url or not str(item_url).startswith('http'):
                item_url = self.SOURCE_URL
            if not bank:
                bank = extract_bank_name(prop_desc) or 'IBAPI Bank'

            headline = prop_desc[:180]
            if price_crore:
                headline = f'₹{price_crore} Cr | {headline}'
            if auction_date:
                headline = f'{headline} | Auction: {auction_date}'

            loc_label   = f' — {location}' if location else ''
            asset_label = asset_class.title()

            events.append(self.make_event(
                company_name=f'{bank} Auction{loc_label} [{asset_label}]',
                keyword='asset auction',
                category='auction',
                url=str(item_url),
                headline=headline,
                snippet=prop_desc[:800],
                metadata={
                    'bank': bank,
                    'reserve_price_crore': price_crore,
                    'location': location,
                    'is_mmr': is_mmr,
                    'asset_class': asset_class,
                    'auction_date': str(auction_date) if auction_date else None,
                    'deal_score': score,
                    'source': 'ibapi_json',
                },
            ))
        return events


class BankAuctionsCoInCrawler(BaseCrawler):
    """
    bankauctions.co.in — third-party aggregator for 40+ banks.
    Scrapeable HTML cards with price, location, asset type, bank name.

    Targets Mumbai/MMR commercial pages first (highest CRE deal value),
    then Pune, then all-India commercial catch-all.
    """
    SOURCE_NAME = 'BankAuctions.co.in'
    SOURCE_URL  = 'https://www.bankauctions.co.in'
    CATEGORY    = 'auction'

    CRAWL_URLS = [
        ('https://www.bankauctions.co.in/bank-auction-property/maharashtra/mumbai/commercial', 'Mumbai',     'commercial'),
        ('https://www.bankauctions.co.in/bank-auction-property/maharashtra/thane/commercial',  'Thane',      'commercial'),
        ('https://www.bankauctions.co.in/bank-auction-property/maharashtra/navi-mumbai',       'Navi Mumbai', None),
        ('https://www.bankauctions.co.in/bank-auction-property/maharashtra/pune/commercial',   'Pune',       'commercial'),
        ('https://www.bankauctions.co.in/commercial-properties',                               None,         'commercial'),
        ('https://www.bankauctions.co.in/bank-auction-property/maharashtra',                   None,          None),
    ]

    CARD_SELS = [
        '.property-card', '.auction-property', '.listing-box',
        '.prop-box', '.card.property', '.result-item',
        'article.property', '.col-property',
    ]

    def crawl(self):
        events  = []
        seen    = set()
        session = requests.Session()
        session.headers.update({**self.HEADERS, 'Referer': self.SOURCE_URL})

        for url, hint_city, hint_class in self.CRAWL_URLS:
            resp = self.safe_get(session, url, timeout=20)
            if not resp:
                continue

            soup  = BeautifulSoup(resp.text, 'html.parser')
            cards = []
            for sel in self.CARD_SELS:
                cards = soup.select(sel)
                if cards:
                    break
            if not cards:
                cards = (
                    soup.find_all('div', class_=re.compile(r'property|auction|listing|result|card', re.I)) or
                    soup.find_all('tr') or
                    soup.find_all('li')
                )

            page_count = 0
            for card in cards[:self.MAX_ARTICLES]:
                text = card.get_text(separator=' ', strip=True)
                if len(text) < 50:
                    continue

                fp = text[:80].lower().strip()
                if fp in seen:
                    continue
                seen.add(fp)

                link     = card.find('a', href=True)
                card_url = link['href'] if link else url
                if card_url and not card_url.startswith('http'):
                    card_url = self.SOURCE_URL + card_url

                price        = extract_price_inr(text)
                location, is_mmr = detect_location(text)
                asset_class  = detect_asset_class(text)
                auction_date = extract_auction_date(text)
                bank         = extract_bank_name(text)

                if not location and hint_city:
                    location = hint_city
                    _, is_mmr = detect_location(hint_city.lower())
                if asset_class == 'other' and hint_class:
                    asset_class = hint_class

                score = deal_score(price, location, is_mmr, asset_class)
                if score < 15:
                    continue

                loc_label   = f' — {location}' if location else ''
                asset_label = asset_class.title()
                headline    = text[:180]
                if price:
                    headline = f'₹{price} Cr | {headline}'

                events.append(self.make_event(
                    company_name=f'{bank} Auction{loc_label} [{asset_label}]',
                    keyword='asset auction',
                    category='auction',
                    url=card_url,
                    headline=headline,
                    snippet=text[:700],
                    metadata={
                        'bank': bank,
                        'reserve_price_crore': price,
                        'location': location,
                        'is_mmr': is_mmr,
                        'asset_class': asset_class,
                        'auction_date': auction_date,
                        'deal_score': score,
                        'source_page': url,
                    },
                ))
                page_count += 1

            self.logger.info(f'  bankauctions.co.in [{url.split("/")[-1]}]: {page_count} events')

        self.logger.info(f'BankAuctions.co.in total: {len(events)} events')
        return events


class SarfaesiDotComCrawler(BaseCrawler):
    """
    sarfaesi.com — SARFAESI notice aggregator.
    Captures Section 13(2)/13(4) possession notices with borrower names.
    Valuable as pre-auction early-warning signal.
    """
    SOURCE_NAME = 'Sarfaesi.com'
    SOURCE_URL  = 'https://www.sarfaesi.com'
    CATEGORY    = 'auction'

    CRAWL_URLS = [
        'https://www.sarfaesi.com/auction_notice.asp',
        'https://www.sarfaesi.com/possession_notice.asp',
        'https://www.sarfaesi.com/auction_notice.asp?city=Mumbai',
        'https://www.sarfaesi.com/auction_notice.asp?state=Maharashtra',
    ]

    def crawl(self):
        events  = []
        seen    = set()
        session = requests.Session()
        session.headers.update(self.HEADERS)

        for url in self.CRAWL_URLS:
            resp = self.safe_get(session, url, timeout=20)
            if not resp:
                continue

            soup = BeautifulSoup(resp.text, 'html.parser')
            page_count = 0

            for row in soup.find_all('tr'):
                text = row.get_text(separator=' ', strip=True)
                if len(text) < 40:
                    continue
                t_lower = text.lower()
                if not any(sig in t_lower for sig in AUCTION_SIGNALS):
                    continue

                fp = text[:80].lower().strip()
                if fp in seen:
                    continue
                seen.add(fp)

                link    = row.find('a', href=True)
                row_url = link['href'] if link else url
                if row_url and not row_url.startswith('http'):
                    row_url = self.SOURCE_URL + '/' + row_url.lstrip('/')

                # First cell often has borrower name
                cells    = row.find_all('td')
                borrower = ''
                if cells:
                    borrower = cells[0].get_text(strip=True)
                    if len(borrower) < 4 or borrower.lower() in ('sr', 'no', '#', 'sno'):
                        borrower = cells[1].get_text(strip=True) if len(cells) > 1 else ''

                price        = extract_price_inr(text)
                location, is_mmr = detect_location(text)
                asset_class  = detect_asset_class(text)
                bank         = extract_bank_name(text)
                auction_date = extract_auction_date(text)
                score        = deal_score(price, location, is_mmr, asset_class)
                is_possession = 'possession' in url
                category     = 'sarfaesi' if is_possession else 'auction'
                keyword      = 'sarfaesi' if is_possession else 'asset auction'

                company  = borrower[:60] if len(borrower) > 4 else f'{bank} SARFAESI Notice'
                headline = f'SARFAESI Notice | {bank} | {text[:150]}'
                if price:
                    headline = f'₹{price} Cr | {headline}'

                events.append(self.make_event(
                    company_name=company,
                    keyword=keyword,
                    category=category,
                    url=row_url,
                    headline=headline,
                    snippet=text[:700],
                    metadata={
                        'bank': bank,
                        'borrower': borrower,
                        'reserve_price_crore': price,
                        'location': location,
                        'is_mmr': is_mmr,
                        'asset_class': asset_class,
                        'auction_date': auction_date,
                        'deal_score': score,
                        'notice_type': 'possession' if is_possession else 'auction',
                    },
                ))
                page_count += 1

            self.logger.info(f'  sarfaesi.com [{url.split("?")[0].split("/")[-1]}]: {page_count} events')

        self.logger.info(f'Sarfaesi.com total: {len(events)} events')
        return events


# ─────────────────────────────────────────────────────────────
# TIER 2 — DIRECT BANK PORTALS
# ─────────────────────────────────────────────────────────────

class _DirectBankCrawler(BaseCrawler):
    """Base for direct bank portal crawlers. Subclasses set name/urls."""
    BANK_LABEL  = 'Bank'
    CRAWL_URLS  = []
    CATEGORY    = 'auction'

    def crawl(self):
        events  = []
        session = requests.Session()
        session.headers.update(self.HEADERS)
        for url in self.CRAWL_URLS:
            resp = self.safe_get(session, url, timeout=20)
            if not resp:
                continue
            soup = BeautifulSoup(resp.text, 'html.parser')
            rows = parse_html_auction_page(soup, url, self.BANK_LABEL, min_score=0)
            for row in rows[:self.MAX_ARTICLES]:
                events.append(row_to_event(self, row))
        self.logger.info(f'{self.SOURCE_NAME}: {len(events)} events')
        return events


class BankOfBarodaAuctionCrawler(_DirectBankCrawler):
    SOURCE_NAME = 'Bank of Baroda Auctions'
    SOURCE_URL  = 'https://www.bankofbaroda.in'
    BANK_LABEL  = 'Bank of Baroda'
    CRAWL_URLS  = [
        'https://www.bankofbaroda.in/about-us/notices-circulars/auction-sale-notice',
        'https://www.bankofbaroda.in/about-us/notices-circulars',
    ]


class PNBAuctionCrawler(_DirectBankCrawler):
    SOURCE_NAME = 'PNB e-Auctions'
    SOURCE_URL  = 'https://www.pnbindia.in'
    BANK_LABEL  = 'Punjab National Bank'
    CRAWL_URLS  = [
        'https://www.pnbindia.in/auctionNotices.html',
        'https://www.pnbindia.in/misc/AuctionNotice',
    ]


class CanaraBankAuctionCrawler(_DirectBankCrawler):
    SOURCE_NAME = 'Canara Bank Auctions'
    SOURCE_URL  = 'https://www.canarabank.com'
    BANK_LABEL  = 'Canara Bank'
    CRAWL_URLS  = [
        'https://www.canarabank.com/english/auction-notices.aspx',
        'https://www.canarabank.com/User_page.aspx?menuid=9&submenuid=91&CatID=2',
    ]


class UnionBankAuctionCrawler(_DirectBankCrawler):
    SOURCE_NAME = 'Union Bank Auctions'
    SOURCE_URL  = 'https://www.unionbankofindia.co.in'
    BANK_LABEL  = 'Union Bank of India'
    CRAWL_URLS  = [
        'https://www.unionbankofindia.co.in/english/auction-notice.aspx',
        'https://www.unionbankofindia.co.in/english/notices.aspx',
    ]


class BankOfMaharashtraAuctionCrawler(_DirectBankCrawler):
    SOURCE_NAME = 'Bank of Maharashtra Auctions'
    SOURCE_URL  = 'https://www.bankofmaharashtra.in'
    BANK_LABEL  = 'Bank of Maharashtra'
    CRAWL_URLS  = [
        'https://www.bankofmaharashtra.in/e-auction.asp',
        'https://www.bankofmaharashtra.in/notices.asp',
    ]


class CentralBankAuctionCrawler(_DirectBankCrawler):
    SOURCE_NAME = 'Central Bank Auctions'
    SOURCE_URL  = 'https://www.centralbankofindia.co.in'
    BANK_LABEL  = 'Central Bank of India'
    CRAWL_URLS  = [
        'https://www.centralbankofindia.co.in/en/auction-notices',
        'https://www.centralbankofindia.co.in/en/e-auction',
    ]


class IndianOverseasBankAuctionCrawler(_DirectBankCrawler):
    SOURCE_NAME = 'IOB Auctions'
    SOURCE_URL  = 'https://www.iob.in'
    BANK_LABEL  = 'Indian Overseas Bank'
    CRAWL_URLS  = [
        'https://www.iob.in/Auction_Notice',
        'https://www.iob.in/e_auction',
    ]


# ─────────────────────────────────────────────────────────────
# TIER 3 — PDF LINK HARVESTERS
# ─────────────────────────────────────────────────────────────

class SBIAuctionCrawler(BaseCrawler):
    """
    SBI publishes auction notices as PDF links.
    We harvest anchor title + href — enough for signal detection,
    location, price (when in the title), and auction date.
    """
    SOURCE_NAME = 'SBI e-Auctions'
    SOURCE_URL  = 'https://sbi.co.in'
    CATEGORY    = 'auction'

    CRAWL_URLS = [
        'https://sbi.co.in/web/sbi-in-the-news/auction-notices',
        'https://sbi.co.in/web/interest-rates/auction-notice',
    ]

    def crawl(self):
        events  = []
        seen    = set()
        session = requests.Session()
        session.headers.update(self.HEADERS)

        for url in self.CRAWL_URLS:
            resp = self.safe_get(session, url, timeout=20)
            if not resp:
                continue

            soup = BeautifulSoup(resp.text, 'html.parser')
            for a in soup.find_all('a', href=True):
                text = a.get_text(strip=True)
                href = a['href']
                if len(text) < 15:
                    continue
                t_lower = text.lower()
                if not any(sig in t_lower for sig in ['auction', 'sarfaesi', 'property', 'asset', 'npa', 'sale']):
                    continue

                fp = text[:60].lower()
                if fp in seen:
                    continue
                seen.add(fp)

                full_url    = href if href.startswith('http') else self.SOURCE_URL + href
                price       = extract_price_inr(text)
                location, is_mmr = detect_location(text)
                asset_class = detect_asset_class(text)
                auction_date = extract_auction_date(text)
                score       = deal_score(price, location, is_mmr, asset_class)

                loc_label   = f' — {location}' if location else ''
                asset_label = asset_class.title()
                headline    = text[:250]
                if price:
                    headline = f'₹{price} Cr | {headline}'

                events.append(self.make_event(
                    company_name=f'SBI Auction{loc_label} [{asset_label}]',
                    keyword='asset auction',
                    category='auction',
                    url=full_url,
                    headline=headline,
                    snippet=text,
                    metadata={
                        'bank': 'State Bank of India',
                        'reserve_price_crore': price,
                        'location': location,
                        'is_mmr': is_mmr,
                        'asset_class': asset_class,
                        'auction_date': auction_date,
                        'deal_score': score,
                        'is_pdf': href.lower().endswith('.pdf'),
                    },
                ))

        self.logger.info(f'SBI e-Auctions: {len(events)} events')
        return events


# ─────────────────────────────────────────────────────────────
# REGISTRY
# ─────────────────────────────────────────────────────────────

ALL_BANK_AUCTION_CRAWLERS = [
    IBAPIAuctionCrawler,
    BankAuctionsCoInCrawler,
    SarfaesiDotComCrawler,
    BankOfBarodaAuctionCrawler,
    PNBAuctionCrawler,
    CanaraBankAuctionCrawler,
    UnionBankAuctionCrawler,
    BankOfMaharashtraAuctionCrawler,
    CentralBankAuctionCrawler,
    IndianOverseasBankAuctionCrawler,
    SBIAuctionCrawler,
]


# ─────────────────────────────────────────────────────────────
# BACKWARDS COMPATIBILITY ALIAS
# narcl_arc.py and pre_leased_cre.py import detect_mmr_location
# from this module. We renamed it to detect_location but keep
# this alias so those imports don't break.
# ─────────────────────────────────────────────────────────────
def detect_mmr_location(text: str) -> str | None:
    """Alias for detect_location() — returns location name only (no is_mmr)."""
    loc, _ = detect_location(text)
    return loc
