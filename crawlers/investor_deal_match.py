"""
crawlers/investor_deal_match.py
═══════════════════════════════════════════════════════════════════════════
PE Fund & Family Office Deal-Matching Intelligence

Why this matters (from deal intel):
  - International investors (London-based group) seeking 8.5–9% cap rate
  - Local PE funds and family offices actively scouting Mumbai commercial
  - Stock market decline → capital rotation into real assets
  - Build database BEFORE market worsens → first call when desperation hits
  - Don't compete with other IPCs on small deals → focus on differentiated,
    larger, difficult assets with higher payoffs

This crawler:
  1. Tracks PE fund India real estate announcements (fundraise, deployment)
  2. Monitors family office India CRE interest signals
  3. Scrapes SEBI AIF registrations (Alternative Investment Funds)
  4. Tracks distressed RE exits — when PE funds need to offload
  5. Maps international capital inflows into Indian CRE
═══════════════════════════════════════════════════════════════════════════
"""

import re
import time
import logging
import requests
from bs4 import BeautifulSoup
from urllib.parse import quote
from .base import BaseCrawler, DistressEvent

logger = logging.getLogger(__name__)

# PE / FO investor keywords
INVESTOR_KEYWORDS = [
    # PE funds
    'pe fund', 'private equity', 'blackstone real estate', 'brookfield',
    'gdivision', 'gic singapore', 'adia', 'abu dhabi investment',
    'warburg pincus real estate', 'kkr india real estate',
    'bain capital india', 'tpg real estate', 'advent india',
    'chrys capital real estate', 'piramal fund', 'kotak realty fund',
    'hdfc capital', 'motilal oswal real estate', 'ifc india',
    'nexus select trust', 'mindspace reit', 'embassy reit',
    # Family offices
    'family office india real estate', 'hni commercial property',
    'ultra hni property investment', 'promoter family office india',
    # International capital
    'foreign direct investment real estate india',
    'london investors india office', 'uk capital india real estate',
    'singapore investors india commercial', 'middle east india property',
    'qatar investment india real estate',
    # SEBI AIF
    'sebi aif registration', 'alternative investment fund real estate',
    'category ii aif real estate', 'category iii aif india',
    # Deal flow
    'cap rate india office', 'yield compression india commercial',
    'office market cap rate mumbai', 'grade a office investment india',
    'commercial real estate fund raise india', 'distressed re fund india',
]

# Investor deal size thresholds (in crore)
MIN_DEAL_CR = 50      # minimum interesting deal
TARGET_MIN  = 100     # target range start
TARGET_MAX  = 500     # target range end
LARGE_DEAL  = 500     # large format deal

PRICE_RE = re.compile(
    r'(?:rs\.?|₹|inr|usd|\$|£)?\s*([\d,]+(?:\.\d+)?)\s*'
    r'(crore|cr\.?|lakh|lac|million|mn|billion|bn)',
    re.IGNORECASE
)


def extract_deal_size(text: str) -> float | None:
    """Extract deal size in INR crore."""
    m = PRICE_RE.search(text)
    if not m:
        return None
    try:
        val = float(m.group(1).replace(',', ''))
        unit = m.group(2).lower()
        if 'cr' in unit:
            return round(val, 2)
        elif 'lakh' in unit or 'lac' in unit:
            return round(val / 100, 4)
        elif 'million' in unit or 'mn' in unit:
            return round(val * 8.5 / 100, 2)  # approx USD/GBP to crore
        elif 'billion' in unit or 'bn' in unit:
            return round(val * 8500 / 100, 2)
    except (ValueError, AttributeError):
        pass
    return None


class PEFundActivityCrawler(BaseCrawler):
    """
    Tracks PE fund real estate activity in India.
    Identifies when funds are deploying capital (=buyer opportunity)
    or when funds are exiting (= seller opportunity).
    
    Meeting context: "stock market decline expected to create distress
    opportunities — build database of properties to capitalize on desperate
    sellers when market conditions worsen."
    """
    SOURCE_NAME = 'PE Fund Activity'
    SOURCE_URL = 'https://www.google.com'
    CATEGORY = 'other'

    PE_GNEWS_QUERIES = [
        # Fundraise signals → these funds will deploy into CRE
        'private equity fund India real estate fundraise 2024',
        'PE fund India commercial real estate investment 2024',
        'Blackstone Brookfield India office acquisition 2024',
        'SEBI AIF real estate fund registration 2024',
        'family office India Grade A office purchase 2024',
        # Exit signals → motivated sellers
        'PE fund exit India commercial real estate 2024',
        'private equity exit India office building sale',
        'reit listing India office space 2024',
        # International capital inflows
        'foreign investors India commercial real estate 2024',
        'London investors India office market 2024',
        'Singapore GIC India real estate 2024',
        'ADIA India commercial property investment',
        'Middle East sovereign fund India real estate',
        # Market stress → distress creation
        'India real estate market stress correction 2024',
        'commercial real estate distress India stock market decline',
        'office market oversupply India vacancy 2024',
    ]

    # SEBI AIF registration portal
    SEBI_AIF_URL = 'https://www.sebi.gov.in/sebiweb/other/OtherAction.do?doRecognisedFpi=yes&intmId=13'

    GNEWS_BASE = 'https://news.google.com/rss/search?q={q}&hl=en-IN&gl=IN&ceid=IN:en'

    # PE/family office curated news sources
    PE_FEEDS = [
        ('VCCircle',     'https://www.vccircle.com/feed'),
        ('DealStreetAsia','https://www.dealstreetasia.com/rss'),
        ('ET Realty',    'https://realty.economictimes.indiatimes.com/rss/topstories'),
        ('MoneyControl CRE', 'https://www.moneycontrol.com/rss/business.xml'),
    ]

    def crawl(self) -> list[DistressEvent]:
        events = []
        session = requests.Session()

        # 1. PE/VC news RSS feeds
        for source_name, feed_url in self.PE_FEEDS:
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

                    full = f'{title} {desc}'
                    full_lower = full.lower()

                    # Check for PE/investor signals
                    investor_hit = any(k in full_lower for k in INVESTOR_KEYWORDS)
                    kws = self.detect_keywords(full)

                    if not investor_hit and not kws:
                        continue

                    deal_size = extract_deal_size(full)
                    from .multi_bank_auctions import detect_mmr_location
                    location = detect_mmr_location(full)

                    # Classify: deploy (buy) vs exit (sell)
                    signal_type = 'deployment'
                    if any(w in full_lower for w in [
                        'exit', 'sell', 'sale', 'divest', 'offload',
                        'list', 'reit', 'monetize', 'liquidate',
                    ]):
                        signal_type = 'exit'

                    companies = self.extract_company_names(title)

                    events.append(self.make_event(
                        company_name=companies[0] if companies else f'PE Signal — {source_name}',
                        keyword=kws[0][0] if kws else 'distressed_asset',
                        category=kws[0][1] if kws else 'distressed_asset',
                        url=link,
                        headline=title,
                        snippet=desc[:400],
                        metadata={
                            'investor_channel': True,
                            'signal_type': signal_type,
                            'deal_size_cr': deal_size,
                            'location': location,
                            'source': source_name,
                            'in_target_range': (
                                deal_size is not None and
                                TARGET_MIN <= deal_size <= TARGET_MAX
                            ),
                        },
                    ))
            except Exception as e:
                self.logger.debug(f'{source_name} feed error: {e}')
            time.sleep(0.8)

        # 2. SEBI AIF portal
        resp = self.safe_get(session, self.SEBI_AIF_URL)
        if resp:
            soup = BeautifulSoup(resp.text, 'html.parser')
            for row in soup.find_all('tr', limit=100):
                text = row.get_text(separator=' ', strip=True)
                if 'real estate' in text.lower() or 'infrastructure' in text.lower():
                    link = row.find('a', href=True)
                    row_url = self.SEBI_AIF_URL
                    if link:
                        href = link['href']
                        row_url = href if href.startswith('http') else 'https://www.sebi.gov.in' + href
                    companies = self.extract_company_names(text)
                    events.append(self.make_event(
                        company_name=companies[0] if companies else 'SEBI AIF',
                        keyword='distressed_asset',
                        category='distressed_asset',
                        url=row_url,
                        headline=text[:200],
                        snippet=text[:500],
                        metadata={
                            'investor_channel': True,
                            'signal_type': 'aif_registration',
                            'source': 'SEBI AIF Registry',
                        },
                    ))

        # 3. Google News PE activity
        for query in self.PE_GNEWS_QUERIES:
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
                    full_lower = full.lower()

                    investor_hit = any(k in full_lower for k in INVESTOR_KEYWORDS)
                    kws = self.detect_keywords(full)
                    if not investor_hit and not kws:
                        continue

                    deal_size = extract_deal_size(full)
                    from .multi_bank_auctions import detect_mmr_location
                    location = detect_mmr_location(full)

                    signal_type = 'exit' if any(
                        w in full_lower for w in ['exit', 'sell', 'divest', 'sale', 'offload']
                    ) else 'deployment'

                    companies = self.extract_company_names(title)
                    events.append(self.make_event(
                        company_name=companies[0] if companies else 'PE/FO CRE Signal',
                        keyword=kws[0][0] if kws else 'distressed_asset',
                        category=kws[0][1] if kws else 'distressed_asset',
                        url=link,
                        headline=title,
                        snippet=desc[:400],
                        metadata={
                            'investor_channel': True,
                            'signal_type': signal_type,
                            'deal_size_cr': deal_size,
                            'location': location,
                            'gnews_query': query,
                            'in_target_range': (
                                deal_size is not None and
                                TARGET_MIN <= deal_size <= TARGET_MAX
                            ),
                        },
                    ))
            except Exception as e:
                self.logger.debug(f'PE GNews error: {e}')
            time.sleep(0.8)

        self.logger.info(f'PE Fund Activity: {len(events)} events')
        return events


class StockMarketDistressSignalCrawler(BaseCrawler):
    """
    Monitors stock market decline → CRE distress opportunity tracker.

    Meeting insight: "Stock market decline expected to create distress
    opportunities in commercial real estate sector."
    
    Logic: When promoters face margin calls or wealth erosion,
    they often sell commercial real estate holdings for liquidity.
    This is the HIGHEST urgency / most motivated seller scenario.
    
    Signals to watch:
    - Promoter pledged shares + stock price decline (margin call risk)
    - Company cash flow stress → forced CRE liquidation
    - NBFC/HFC stress → commercial RE collateral auctions
    """
    SOURCE_NAME = 'Stock Market Distress Signals'
    SOURCE_URL = 'https://www.google.com'
    CATEGORY = 'other'

    GNEWS_QUERIES = [
        # Margin call → forced CRE sale
        'promoter pledge shares margin call India 2024 real estate',
        'promoter selling commercial property India liquidity 2024',
        'forced sale commercial property India promoter debt',
        # NBFC / HFC stress
        'NBFC stress India commercial real estate collateral 2024',
        'housing finance company NPA commercial property',
        'NBFC loan against property LAP NPA 2024',
        # Market correction
        'India stock market decline commercial property sale 2024',
        'equity market fall India real estate selling pressure',
        'nifty sensex decline property market stress India',
        # Sector-specific stress creating CRE distress
        'IT company India office consolidation sublease 2024',
        'startup shutdown India office space return 2024',
        'corporate downsizing India office space vacancy 2024',
    ]

    # Financial news RSS feeds with equity market coverage
    MARKET_FEEDS = [
        ('Moneycontrol Markets', 'https://www.moneycontrol.com/rss/marketreports.xml'),
        ('ET Markets',           'https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms'),
        ('Business Standard Mkt','https://www.business-standard.com/rss/markets-106.rss'),
        ('Mint Markets',         'https://www.livemint.com/rss/markets'),
    ]

    GNEWS_BASE = 'https://news.google.com/rss/search?q={q}&hl=en-IN&gl=IN&ceid=IN:en'

    def crawl(self) -> list[DistressEvent]:
        events = []
        session = requests.Session()

        # 1. Market news RSS
        for source_name, feed_url in self.MARKET_FEEDS:
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

                    full = f'{title} {desc}'
                    full_lower = full.lower()

                    # Look for forced sale / distress indicators
                    if not any(w in full_lower for w in [
                        'pledged shares', 'margin call', 'forced sale',
                        'promoter selling', 'promoter pledge',
                        'lac', 'commercial property', 'real estate sell',
                        'npa', 'nbfc stress', 'loan against property',
                    ]):
                        continue

                    kws = self.detect_keywords(full)
                    from .multi_bank_auctions import extract_price_inr, detect_mmr_location
                    price = extract_price_inr(full)
                    location = detect_mmr_location(full)

                    companies = self.extract_company_names(title)
                    events.append(self.make_event(
                        company_name=companies[0] if companies else 'Market Distress Signal',
                        keyword=kws[0][0] if kws else 'default',
                        category=kws[0][1] if kws else 'default',
                        url=link,
                        headline=title,
                        snippet=desc[:400],
                        metadata={
                            'signal_channel': 'equity_market_stress',
                            'price_crore': price,
                            'location': location,
                            'source': source_name,
                            'trigger': 'stock_market_decline',
                        },
                    ))
            except Exception as e:
                self.logger.debug(f'{source_name} error: {e}')
            time.sleep(0.8)

        # 2. Targeted Google News
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

                    kws = self.detect_keywords(f'{title} {desc}')
                    from .multi_bank_auctions import extract_price_inr, detect_mmr_location
                    price = extract_price_inr(f'{title} {desc}')
                    location = detect_mmr_location(f'{title} {desc}')

                    companies = self.extract_company_names(title)
                    events.append(self.make_event(
                        company_name=companies[0] if companies else 'Market-Triggered Distress',
                        keyword=kws[0][0] if kws else 'distressed_asset',
                        category=kws[0][1] if kws else 'distressed_asset',
                        url=link,
                        headline=title,
                        snippet=desc[:400],
                        metadata={
                            'signal_channel': 'equity_market_stress',
                            'price_crore': price,
                            'location': location,
                            'gnews_query': query,
                            'trigger': 'stock_market_decline',
                        },
                    ))
            except Exception as e:
                self.logger.debug(f'GNews market error: {e}')
            time.sleep(0.8)

        self.logger.info(f'Stock Market Distress Signals: {len(events)} events')
        return events
