"""
crawlers/multi_bank_auctions.py
═══════════════════════════════════════════════════════════════════════════
Crawls 9 Indian bank auction portals for SARFAESI & NPA property auctions.

Why this matters (from deal intel):
  - Banks have public websites listing auction properties at distressed prices
  - Filter for 10–15 crore range in Mumbai/MMR secondary markets
  - These are the fastest path to desperate-seller inventory
  - Grade A commercial assets show up here when strata-titled buildings default

Banks covered:
  1. Bank of Baroda   — e-Auction portal
  2. Punjab National Bank — IBAPI + direct listing
  3. Canara Bank      — Auction notices
  4. Union Bank       — e-Auction
  5. Bank of Maharashtra — Auction page
  6. Central Bank     — Auction notices
  7. Indian Bank      — Auction notices
  8. Indian Overseas Bank — Direct auction
  9. UCO Bank         — Auction listing
═══════════════════════════════════════════════════════════════════════════
"""

import re
import logging
import requests
from bs4 import BeautifulSoup
from .base import BaseCrawler, DistressEvent

logger = logging.getLogger(__name__)

# Price extraction — handles ₹10 lakh, 1.5 crore, Rs 25 lac etc.
PRICE_RE = re.compile(
    r'(?:rs\.?|₹|inr)?\s*([\d,]+(?:\.\d+)?)\s*(crore|cr\.?|lakh|lac|lakhs|l)',
    re.IGNORECASE
)

# Location signals for Mumbai / MMR market
MMR_LOCATIONS = [
    'mumbai', 'thane', 'navi mumbai', 'kalyan', 'dombivli',
    'panvel', 'vasai', 'virar', 'mira road', 'bhiwandi',
    'kurla', 'bandra', 'andheri', 'malad', 'goregaon',
    'borivali', 'kandivali', 'dahisar', 'mulund', 'ghatkopar',
    'chembur', 'wadala', 'worli', 'lower parel', 'bkc',
    'dharavi', 'sion', 'matunga', 'dadar', 'prabhadevi',
    'powai', 'vikhroli', 'kanjurmarg', 'bhandup',
]

# Asset class signals
ASSET_TYPE_RE = {
    'commercial': ['office', 'commercial', 'shop', 'showroom', 'godown',
                   'warehouse', 'factory', 'industrial', 'plot', 'it park',
                   'it/ites', 'bpo', 'business park'],
    'residential': ['flat', 'apartment', 'bungalow', 'villa', 'residential',
                    'row house', 'tenement', 'chawl'],
    'land': ['land', 'plot', 'open land', 'na plot', 'agricultural land'],
}


def extract_price_inr(text: str) -> float | None:
    """Extract price in INR crores from text. Returns float crore value or None."""
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


def detect_mmr_location(text: str) -> str | None:
    """Check if text mentions a Mumbai/MMR location."""
    t = text.lower()
    for loc in MMR_LOCATIONS:
        if loc in t:
            return loc.title()
    return None


def detect_asset_class(text: str) -> str:
    """Classify asset as commercial / residential / land."""
    t = text.lower()
    for asset_class, keywords in ASSET_TYPE_RE.items():
        if any(k in t for k in keywords):
            return asset_class
    return 'other'


def parse_auction_table(soup: BeautifulSoup, source_url: str, bank_name: str) -> list[dict]:
    """Generic parser for bank auction HTML tables."""
    results = []
    for tag in soup.find_all(['tr', 'li', 'div', 'article']):
        text = tag.get_text(separator=' ', strip=True)
        if len(text) < 30:
            continue

        # Must mention auction-related terms
        if not any(w in text.lower() for w in [
            'auction', 'sarfaesi', 'property', 'reserve price',
            'e-auction', 'bid', 'sale notice', 'possession',
        ]):
            continue

        link_tag = tag.find('a', href=True)
        url = source_url
        if link_tag:
            href = link_tag['href']
            url = href if href.startswith('http') else source_url.rstrip('/') + '/' + href.lstrip('/')

        price = extract_price_inr(text)
        location = detect_mmr_location(text)
        asset_class = detect_asset_class(text)

        # Date extraction
        date_match = re.search(
            r'(\d{1,2}[\/\-\.]\d{1,2}[\/\-\.]\d{2,4})',
            text
        )
        auction_date = date_match.group(1) if date_match else None

        results.append({
            'text': text[:600],
            'url': url,
            'price_crore': price,
            'location': location,
            'asset_class': asset_class,
            'auction_date': auction_date,
            'bank': bank_name,
        })

    return results


class BankOfBarodaAuctionCrawler(BaseCrawler):
    """Bank of Baroda SARFAESI & NPA auction notices."""
    SOURCE_NAME = 'Bank of Baroda Auctions'
    SOURCE_URL = 'https://www.bankofbaroda.in'
    CATEGORY = 'auction'

    CRAWL_URLS = [
        'https://www.bankofbaroda.in/about-us/notices-circulars/auction-sale-notice',
        'https://www.bankofbaroda.in/about-us/notices-circulars',
    ]

    def crawl(self) -> list[DistressEvent]:
        events = []
        session = requests.Session()

        for url in self.CRAWL_URLS:
            resp = self.safe_get(session, url)
            if not resp:
                continue
            soup = BeautifulSoup(resp.text, 'html.parser')
            rows = parse_auction_table(soup, url, 'Bank of Baroda')

            for row in rows[:self.MAX_ARTICLES]:
                events.append(self._row_to_event(row, url))

        self.logger.info(f'BoB Auctions: {len(events)} events')
        return events

    def _row_to_event(self, row: dict, fallback_url: str) -> DistressEvent:
        return self.make_event(
            company_name=row['bank'] + ' Auction Property',
            keyword='asset auction',
            category='asset_auction',
            url=row['url'],
            headline=row['text'][:200],
            snippet=row['text'],
            metadata={
                'bank': row['bank'],
                'reserve_price_crore': row['price_crore'],
                'location': row['location'],
                'asset_class': row['asset_class'],
                'auction_date': row['auction_date'],
                'is_mmr': row['location'] is not None,
            },
        )


class PNBauctionCrawler(BaseCrawler):
    """Punjab National Bank e-Auction listings."""
    SOURCE_NAME = 'PNB e-Auctions'
    SOURCE_URL = 'https://www.pnbindia.in'
    CATEGORY = 'auction'

    CRAWL_URLS = [
        'https://www.pnbindia.in/auctionNotices.html',
        'https://www.pnbindia.in/misc/AuctionNotice',
    ]

    def crawl(self) -> list[DistressEvent]:
        events = []
        session = requests.Session()

        for url in self.CRAWL_URLS:
            resp = self.safe_get(session, url)
            if not resp:
                continue
            soup = BeautifulSoup(resp.text, 'html.parser')
            # PNB lists PDFs and notices
            for a in soup.find_all('a', href=True):
                text = a.get_text(strip=True)
                href = a['href']
                if len(text) < 15:
                    continue
                if any(w in text.lower() for w in ['auction', 'sarfaesi', 'sale notice', 'property']):
                    full_url = href if href.startswith('http') else self.SOURCE_URL + href
                    price = extract_price_inr(text)
                    location = detect_mmr_location(text)
                    events.append(self.make_event(
                        company_name='PNB Auction Property',
                        keyword='asset auction',
                        category='asset_auction',
                        url=full_url,
                        headline=text,
                        snippet=text,
                        metadata={
                            'bank': 'Punjab National Bank',
                            'reserve_price_crore': price,
                            'location': location,
                            'is_mmr': location is not None,
                            'asset_class': detect_asset_class(text),
                        },
                    ))

        self.logger.info(f'PNB Auctions: {len(events)} events')
        return events


class CanaraBankAuctionCrawler(BaseCrawler):
    """Canara Bank auction notices (high volume NPA portfolio)."""
    SOURCE_NAME = 'Canara Bank Auctions'
    SOURCE_URL = 'https://canarabank.com'
    CATEGORY = 'auction'

    CRAWL_URLS = [
        'https://canarabank.com/User_page.aspx?menulevel=2&menuid=15&CatId=8',
        'https://canarabank.com/auction-notice',
    ]

    def crawl(self) -> list[DistressEvent]:
        events = []
        session = requests.Session()

        for url in self.CRAWL_URLS:
            resp = self.safe_get(session, url)
            if not resp:
                continue
            soup = BeautifulSoup(resp.text, 'html.parser')
            rows = parse_auction_table(soup, url, 'Canara Bank')
            for row in rows[:self.MAX_ARTICLES]:
                events.append(self.make_event(
                    company_name='Canara Bank Auction Property',
                    keyword='asset auction',
                    category='asset_auction',
                    url=row['url'],
                    headline=row['text'][:200],
                    snippet=row['text'],
                    metadata={
                        'bank': 'Canara Bank',
                        'reserve_price_crore': row['price_crore'],
                        'location': row['location'],
                        'asset_class': row['asset_class'],
                        'auction_date': row['auction_date'],
                        'is_mmr': row['location'] is not None,
                    },
                ))

        self.logger.info(f'Canara Bank Auctions: {len(events)} events')
        return events


class UnionBankAuctionCrawler(BaseCrawler):
    """Union Bank of India e-auction listings."""
    SOURCE_NAME = 'Union Bank Auctions'
    SOURCE_URL = 'https://www.unionbankofindia.co.in'
    CATEGORY = 'auction'

    CRAWL_URLS = [
        'https://www.unionbankofindia.co.in/english/auction-notice.aspx',
        'https://www.unionbankofindia.co.in/english/notices.aspx',
    ]

    def crawl(self) -> list[DistressEvent]:
        events = []
        session = requests.Session()

        for url in self.CRAWL_URLS:
            resp = self.safe_get(session, url)
            if not resp:
                continue
            soup = BeautifulSoup(resp.text, 'html.parser')
            rows = parse_auction_table(soup, url, 'Union Bank of India')
            for row in rows[:self.MAX_ARTICLES]:
                events.append(self.make_event(
                    company_name='Union Bank Auction Property',
                    keyword='asset auction',
                    category='asset_auction',
                    url=row['url'],
                    headline=row['text'][:200],
                    snippet=row['text'],
                    metadata={
                        'bank': 'Union Bank of India',
                        'reserve_price_crore': row['price_crore'],
                        'location': row['location'],
                        'asset_class': row['asset_class'],
                        'auction_date': row['auction_date'],
                        'is_mmr': row['location'] is not None,
                    },
                ))

        self.logger.info(f'Union Bank Auctions: {len(events)} events')
        return events


class BankOfMaharashtraAuctionCrawler(BaseCrawler):
    """Bank of Maharashtra — important for MMR / Pune market."""
    SOURCE_NAME = 'Bank of Maharashtra Auctions'
    SOURCE_URL = 'https://www.bankofmaharashtra.in'
    CATEGORY = 'auction'

    CRAWL_URLS = [
        'https://www.bankofmaharashtra.in/e-auction.asp',
        'https://www.bankofmaharashtra.in/notices.asp',
    ]

    def crawl(self) -> list[DistressEvent]:
        events = []
        session = requests.Session()

        for url in self.CRAWL_URLS:
            resp = self.safe_get(session, url)
            if not resp:
                continue
            soup = BeautifulSoup(resp.text, 'html.parser')
            rows = parse_auction_table(soup, url, 'Bank of Maharashtra')
            for row in rows[:self.MAX_ARTICLES]:
                events.append(self.make_event(
                    company_name='Bank of Maharashtra Auction Property',
                    keyword='asset auction',
                    category='asset_auction',
                    url=row['url'],
                    headline=row['text'][:200],
                    snippet=row['text'],
                    metadata={
                        'bank': 'Bank of Maharashtra',
                        'reserve_price_crore': row['price_crore'],
                        'location': row['location'],
                        'asset_class': row['asset_class'],
                        'auction_date': row['auction_date'],
                        'is_mmr': row['location'] is not None,
                    },
                ))

        self.logger.info(f'BoM Auctions: {len(events)} events')
        return events


class CentralBankAuctionCrawler(BaseCrawler):
    """Central Bank of India auction notices."""
    SOURCE_NAME = 'Central Bank Auctions'
    SOURCE_URL = 'https://www.centralbankofindia.co.in'
    CATEGORY = 'auction'

    CRAWL_URLS = [
        'https://www.centralbankofindia.co.in/en/auction-notices',
        'https://www.centralbankofindia.co.in/en/e-auction',
    ]

    def crawl(self) -> list[DistressEvent]:
        events = []
        session = requests.Session()

        for url in self.CRAWL_URLS:
            resp = self.safe_get(session, url)
            if not resp:
                continue
            soup = BeautifulSoup(resp.text, 'html.parser')
            rows = parse_auction_table(soup, url, 'Central Bank of India')
            for row in rows[:self.MAX_ARTICLES]:
                events.append(self.make_event(
                    company_name='Central Bank Auction Property',
                    keyword='asset auction',
                    category='asset_auction',
                    url=row['url'],
                    headline=row['text'][:200],
                    snippet=row['text'],
                    metadata={
                        'bank': 'Central Bank of India',
                        'reserve_price_crore': row['price_crore'],
                        'location': row['location'],
                        'asset_class': row['asset_class'],
                        'is_mmr': row['location'] is not None,
                    },
                ))

        self.logger.info(f'Central Bank Auctions: {len(events)} events')
        return events


class IndianOverseasBankAuctionCrawler(BaseCrawler):
    """Indian Overseas Bank auction listings."""
    SOURCE_NAME = 'IOB Auctions'
    SOURCE_URL = 'https://www.iob.in'
    CATEGORY = 'auction'

    CRAWL_URLS = [
        'https://www.iob.in/Auction_Notice',
        'https://www.iob.in/e_auction',
    ]

    def crawl(self) -> list[DistressEvent]:
        events = []
        session = requests.Session()

        for url in self.CRAWL_URLS:
            resp = self.safe_get(session, url)
            if not resp:
                continue
            soup = BeautifulSoup(resp.text, 'html.parser')
            rows = parse_auction_table(soup, url, 'Indian Overseas Bank')
            for row in rows[:self.MAX_ARTICLES]:
                events.append(self.make_event(
                    company_name='IOB Auction Property',
                    keyword='asset auction',
                    category='asset_auction',
                    url=row['url'],
                    headline=row['text'][:200],
                    snippet=row['text'],
                    metadata={
                        'bank': 'Indian Overseas Bank',
                        'reserve_price_crore': row['price_crore'],
                        'location': row['location'],
                        'asset_class': row['asset_class'],
                        'is_mmr': row['location'] is not None,
                    },
                ))

        self.logger.info(f'IOB Auctions: {len(events)} events')
        return events


class BankAuctionDotInCrawler(BaseCrawler):
    """
    bankauction.in — aggregator listing auctions from 30+ banks.
    Best single source for bulk Mumbai commercial property auctions.
    Filter: commercial + MMR + 5-50 crore range.
    """
    SOURCE_NAME = 'BankAuction.in'
    SOURCE_URL = 'https://www.bankauction.in'
    CATEGORY = 'auction'

    CRAWL_URLS = [
        'https://www.bankauction.in/bank-auction-property-in-mumbai.html',
        'https://www.bankauction.in/commercial-property-auction.html',
        'https://www.bankauction.in/bank-auction-property-in-thane.html',
        'https://www.bankauction.in/bank-auction-property-in-navi-mumbai.html',
        'https://www.bankauction.in/bank-auction-property-in-pune.html',
    ]

    def crawl(self) -> list[DistressEvent]:
        events = []
        session = requests.Session()

        for url in self.CRAWL_URLS:
            resp = self.safe_get(session, url)
            if not resp:
                continue

            soup = BeautifulSoup(resp.text, 'html.parser')

            # bankauction.in uses property cards
            cards = (
                soup.select('.property-item') or
                soup.select('.auction-card') or
                soup.select('.listing-item') or
                soup.select('article') or
                soup.find_all('div', class_=re.compile(r'card|property|auction|listing', re.I))
            )

            if not cards:
                # Fall back to table rows
                cards = soup.find_all('tr')

            for card in cards[:self.MAX_ARTICLES]:
                text = card.get_text(separator=' ', strip=True)
                if len(text) < 40:
                    continue

                link = card.find('a', href=True)
                card_url = link['href'] if link else url
                if card_url and not card_url.startswith('http'):
                    card_url = self.SOURCE_URL + card_url

                price = extract_price_inr(text)
                location = detect_mmr_location(text)
                asset_class = detect_asset_class(text)

                # Score: prioritise commercial assets in MMR range 5–150 cr
                score = 0
                if asset_class == 'commercial':
                    score += 40
                if location:
                    score += 30
                if price and 5 <= price <= 150:
                    score += 30

                if score < 30:  # Skip low-signal rows
                    continue

                # Extract bank name from text
                bank_match = re.search(
                    r'\b(SBI|HDFC|ICICI|Axis|Canara|PNB|BOB|Union|Bank of India|'
                    r'Indian Bank|IOB|UCO|Central Bank|BOI|Kotak|YES Bank)\b',
                    text, re.IGNORECASE
                )
                bank_name = bank_match.group(0) if bank_match else 'Bank Auction'

                events.append(self.make_event(
                    company_name=f'{bank_name} Auction — {asset_class.title()}',
                    keyword='asset auction',
                    category='asset_auction',
                    url=card_url,
                    headline=text[:200],
                    snippet=text[:600],
                    metadata={
                        'bank': bank_name,
                        'reserve_price_crore': price,
                        'location': location,
                        'asset_class': asset_class,
                        'deal_score': score,
                        'is_mmr': location is not None,
                        'source_page': url,
                    },
                ))

        self.logger.info(f'BankAuction.in: {len(events)} events')
        return events


# Convenience alias — all bank auction crawlers
ALL_BANK_AUCTION_CRAWLERS = [
    BankOfBarodaAuctionCrawler,
    PNBauctionCrawler,
    CanaraBankAuctionCrawler,
    UnionBankAuctionCrawler,
    BankOfMaharashtraAuctionCrawler,
    CentralBankAuctionCrawler,
    IndianOverseasBankAuctionCrawler,
    BankAuctionDotInCrawler,
]
