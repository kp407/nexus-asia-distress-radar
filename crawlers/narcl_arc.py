"""
crawlers/narcl_arc.py
═══════════════════════════════════════════════════════════════════════════
NARCL (National Asset Reconstruction Company Ltd) + ARC Portfolio Tracker

Why this matters (from deal intel):
  - ARCs hold large portfolios of distressed commercial real estate
  - NARCL was specifically set up to absorb ₹2L+ crore of bad loans
  - PE funds and family offices can buy directly from ARC at haircut
  - ARC portfolios include Grade A commercial, industrial, hospitality
  - Deal structure: acquire security receipts → workout → monetise asset

ARCs covered:
  1. NARCL (Govt-backed, largest NPA portfolio in India)
  2. ARCIL (oldest, largest private ARC)
  3. Edelweiss ARC (aggressive acquirer, large commercial RE book)
  4. Phoenix ARC (HDFC Group — quality assets)
  5. JM Financial ARC (mid-size, good deal flow)
  6. CFM ARC
  7. Omkara ARC
  8. Kotak Mahindra ARC
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

# ARC-specific keywords
ARC_KEYWORDS = [
    'asset reconstruction', 'arc portfolio', 'security receipts',
    'sr redemption', 'narcl', 'arcil', 'edelweiss arc',
    'phoenix arc', 'jm financial arc', 'bad bank',
    'national asset reconstruction', 'stressed portfolio acquisition',
    'npa portfolio sale', 'wholesale npa', 'retail npa',
    'stressed book sale', '15:85 structure', 'government guarantee',
]

# Sector weights — real estate ARC deals most relevant
PRIORITY_SECTORS = [
    'real estate', 'realty', 'commercial', 'hospitality',
    'hotels', 'industrial', 'infrastructure',
]


class NARCLCrawler(BaseCrawler):
    """
    National Asset Reconstruction Company Ltd — India's bad bank.
    Holds the largest pool of stressed commercial assets.
    NARCL acquires NPA accounts at 15% cash + 85% government-guaranteed SRs.
    """
    SOURCE_NAME = 'NARCL'
    SOURCE_URL = 'https://narcl.co.in'
    CATEGORY = 'other'

    CRAWL_URLS = [
        'https://narcl.co.in/portfolio',
        'https://narcl.co.in/deals',
        'https://narcl.co.in/news',
        'https://narcl.co.in/press-releases',
        'https://narcl.co.in',
    ]

    GNEWS_QUERIES = [
        'NARCL bad bank acquisition India 2024 commercial real estate',
        'NARCL portfolio stressed asset sale 2024',
        'national asset reconstruction company deal crore',
        'NARCL 15:85 structure acquisition India',
        'NARCL commercial real estate Mumbai Pune Bengaluru',
    ]

    GNEWS_BASE = 'https://news.google.com/rss/search?q={q}&hl=en-IN&gl=IN&ceid=IN:en'

    def crawl(self) -> list[DistressEvent]:
        events = []
        session = requests.Session()

        # 1. Direct NARCL portal
        narcl_dead = False
        for url in self.CRAWL_URLS:
            if narcl_dead:
                break  # site is down — skip all remaining URLs, save 80s
            resp = self.safe_get(session, url, timeout=5)
            if not resp:
                narcl_dead = True
                continue

            soup = BeautifulSoup(resp.text, 'html.parser')

            for tag in soup.find_all(['article', 'tr', 'li', 'div'], limit=100):
                text = tag.get_text(separator=' ', strip=True)
                if len(text) < 30:
                    continue

                t_lower = text.lower()
                if not any(k in t_lower for k in ARC_KEYWORDS + ['acquisition', 'portfolio', 'stressed']):
                    continue

                link = tag.find('a', href=True)
                item_url = url
                if link:
                    href = link['href']
                    item_url = href if href.startswith('http') else self.SOURCE_URL + href

                companies = self.extract_company_names(text)
                company = companies[0] if companies else 'NARCL Portfolio'

                from .multi_bank_auctions import extract_price_inr, detect_mmr_location
                price = extract_price_inr(text)
                location = detect_mmr_location(text)

                events.append(self.make_event(
                    company_name=company,
                    keyword='distressed_asset',
                    category='distressed_asset',
                    url=item_url,
                    headline=text[:250],
                    snippet=text[:600],
                    metadata={
                        'arc_entity': 'NARCL',
                        'deal_structure': '15:85',
                        'government_guarantee': True,
                        'price_crore': price,
                        'location': location,
                        'source_page': url,
                    },
                ))

            time.sleep(1)

        # 2. Google News NARCL signals
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
                    arc_hit = any(k in f'{title} {desc}'.lower() for k in ARC_KEYWORDS)

                    if not kws and not arc_hit:
                        continue

                    companies = self.extract_company_names(title)
                    company = companies[0] if companies else 'NARCL Signal'
                    from .multi_bank_auctions import extract_price_inr, detect_mmr_location
                    price = extract_price_inr(f'{title} {desc}')
                    location = detect_mmr_location(f'{title} {desc}')

                    events.append(self.make_event(
                        company_name=company,
                        keyword=kws[0][0] if kws else 'distressed_asset',
                        category=kws[0][1] if kws else 'distressed_asset',
                        url=link,
                        headline=title,
                        snippet=desc[:400],
                        metadata={
                            'arc_entity': 'NARCL',
                            'price_crore': price,
                            'location': location,
                            'gnews_query': query,
                        },
                    ))
            except Exception as e:
                self.logger.debug(f'NARCL GNews error: {e}')
            time.sleep(0.8)

        self.logger.info(f'NARCL: {len(events)} events')
        return events


class ARCPortfolioCrawler(BaseCrawler):
    """
    Crawls ARCIL, Edelweiss ARC, Phoenix ARC, JM Financial ARC.
    ARCs regularly publish stressed asset sale notices and portfolio updates.
    These represent motivated sellers — they WANT to offload assets.
    """
    SOURCE_NAME = 'ARC Portfolios'
    SOURCE_URL = 'https://www.arcil.com'
    CATEGORY = 'other'

    ARC_SOURCES = [
        # (name, homepage, news/portfolio page)
        ('ARCIL',          'https://www.arcil.com',        'https://www.arcil.com/properties.aspx'),
        ('Edelweiss ARC',  'https://www.edelweissarc.com', 'https://www.edelweissarc.com/properties'),
        ('Phoenix ARC',    'https://www.phoenixarc.co.in', 'https://www.phoenixarc.co.in/properties'),
        ('JM Financial ARC','https://www.jmfarc.com',      'https://www.jmfarc.com/portfolio'),
        ('Kotak ARC',      'https://www.kotakarc.com',     'https://www.kotakarc.com/assets'),
        ('CFM ARC',        'https://www.cfmarc.in',        'https://www.cfmarc.in/portfolio'),
    ]

    GNEWS_QUERIES = [
        'ARCIL property sale India 2024 commercial real estate',
        'Edelweiss ARC property sale Mumbai crore 2024',
        'Phoenix ARC stressed asset sale India',
        'JM Financial ARC portfolio property 2024',
        'asset reconstruction company property auction India 2024',
        'ARC distressed commercial real estate sale India',
        'stressed asset acquisition India ARC PE fund 2024',
    ]

    GNEWS_BASE = 'https://news.google.com/rss/search?q={q}&hl=en-IN&gl=IN&ceid=IN:en'

    def crawl(self) -> list[DistressEvent]:
        events = []
        session = requests.Session()

        # 1. Direct ARC portal crawl
        for arc_name, base_url, portfolio_url in self.ARC_SOURCES:
            for url in [portfolio_url, base_url]:
                resp = self.safe_get(session, url)
                if not resp:
                    continue

                soup = BeautifulSoup(resp.text, 'html.parser')

                # Look for property/portfolio listings
                items = (
                    soup.select('.property-item') or
                    soup.select('.portfolio-item') or
                    soup.select('.asset-card') or
                    soup.find_all(['tr', 'li'], limit=100)
                )

                for item in items:
                    text = item.get_text(separator=' ', strip=True)
                    if len(text) < 30:
                        continue

                    t_lower = text.lower()

                    # ARC portals list property details
                    if not any(w in t_lower for w in [
                        'property', 'asset', 'office', 'commercial', 'land',
                        'industrial', 'factory', 'hotel', 'residential',
                        'crore', 'lakh', 'sqft', 'auction', 'sale',
                    ]):
                        continue

                    link = item.find('a', href=True)
                    item_url = url
                    if link:
                        href = link['href']
                        item_url = href if href.startswith('http') else base_url + href

                    from .multi_bank_auctions import (
                        extract_price_inr, detect_mmr_location, detect_asset_class
                    )
                    price = extract_price_inr(text)
                    location = detect_mmr_location(text)
                    asset_class = detect_asset_class(text)

                    companies = self.extract_company_names(text)
                    company = companies[0] if companies else f'{arc_name} Asset'

                    # Priority score — commercial + MMR = highest priority
                    priority_score = 0
                    if asset_class == 'commercial':
                        priority_score += 40
                    if location:
                        priority_score += 30
                    if price and 5 <= price <= 500:
                        priority_score += 30

                    events.append(self.make_event(
                        company_name=company,
                        keyword='distressed_asset',
                        category='distressed_asset',
                        url=item_url,
                        headline=text[:250],
                        snippet=text[:600],
                        metadata={
                            'arc_entity': arc_name,
                            'price_crore': price,
                            'location': location,
                            'asset_class': asset_class,
                            'priority_score': priority_score,
                            'is_mmr': location is not None,
                            'motivated_seller': True,  # ARCs always motivated
                        },
                    ))

                self.logger.info(f'  {arc_name}: {len(items)} items on {url[-40:]}')
                time.sleep(1.5)

        # 2. Google News ARC deal signals
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
                    arc_hit = any(k in f'{title} {desc}'.lower() for k in ARC_KEYWORDS)

                    if not kws and not arc_hit:
                        continue

                    from .multi_bank_auctions import extract_price_inr, detect_mmr_location
                    price = extract_price_inr(f'{title} {desc}')
                    location = detect_mmr_location(f'{title} {desc}')

                    # Identify which ARC
                    arc_name = 'ARC'
                    for name, _, _ in self.ARC_SOURCES:
                        if name.lower().split()[0] in f'{title} {desc}'.lower():
                            arc_name = name
                            break

                    companies = self.extract_company_names(title)
                    events.append(self.make_event(
                        company_name=companies[0] if companies else f'{arc_name} Portfolio Asset',
                        keyword=kws[0][0] if kws else 'distressed_asset',
                        category=kws[0][1] if kws else 'distressed_asset',
                        url=link,
                        headline=title,
                        snippet=desc[:400],
                        metadata={
                            'arc_entity': arc_name,
                            'price_crore': price,
                            'location': location,
                            'gnews_query': query,
                            'motivated_seller': True,
                        },
                    ))
            except Exception as e:
                self.logger.debug(f'ARC GNews error: {e}')
            time.sleep(0.8)

        self.logger.info(f'ARC Portfolios: {len(events)} events')
        return events
