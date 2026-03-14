"""
crawlers/bank_auctions.py
Crawls bank auction portals for SARFAESI notices and e-auction listings.
"""

import logging
import re
import requests
from bs4 import BeautifulSoup
from .base import BaseCrawler, DistressEvent

logger = logging.getLogger(__name__)


class IBAPIAuctionCrawler(BaseCrawler):
    """Crawls IBAPI (RBI's Indian Banks Auction Portal)."""
    SOURCE_NAME = "IBAPI Auctions"
    SOURCE_URL = "https://ibapi.in"
    CATEGORY = "auction"

    CRAWL_URLS = [
        "https://ibapi.in/Auctions/GetAuctions?type=active",
        "https://ibapi.in",
    ]

    def crawl(self) -> list[DistressEvent]:
        events = []
        session = requests.Session()

        try:
            resp = self.safe_get(session, "https://ibapi.in")
            if not resp:
                return events

            soup = BeautifulSoup(resp.text, "html.parser")
            items = soup.find_all(["tr", "div", "article"], limit=self.MAX_ARTICLES)

            for item in items:
                text = item.get_text(separator=" ", strip=True)
                if len(text) < 20:
                    continue

                keywords = self.detect_keywords(text)
                if not keywords:
                    if any(w in text.lower() for w in ["auction", "sarfaesi", "npa", "reserve price"]):
                        keywords = [("auction", "asset_auction")]
                    else:
                        continue

                companies = self.extract_company_names(text)
                company_name = companies[0] if companies else "Bank Auction Property"

                link = item.find("a", href=True)
                url = self.SOURCE_URL
                if link:
                    href = link["href"]
                    url = href if href.startswith("http") else self.SOURCE_URL + href

                # Try to extract price
                price_match = re.search(r'₹?\s*([\d,]+(?:\.\d+)?)\s*(?:lakh|crore|cr\.?|lac)', text, re.IGNORECASE)
                metadata = {}
                if price_match:
                    metadata["reserve_price_text"] = price_match.group(0)

                kw, category = keywords[0]
                events.append(self.make_event(
                    company_name=company_name,
                    keyword=kw,
                    category=category,
                    url=url,
                    headline=text[:200],
                    snippet=text[:500],
                    metadata=metadata,
                ))

        except Exception as e:
            self.logger.error(f"IBAPI crawl error: {e}")

        self.logger.info(f"IBAPI Auctions: {len(events)} events detected")
        return events


class SBIAuctionCrawler(BaseCrawler):
    """Crawls SBI's e-auction notices."""
    SOURCE_NAME = "SBI e-Auctions"
    SOURCE_URL = "https://sbi.co.in"
    CATEGORY = "auction"

    def crawl(self) -> list[DistressEvent]:
        events = []
        session = requests.Session()

        try:
            url = "https://sbi.co.in/web/sbi-in-the-news/auction-notices"
            resp = self.safe_get(session, url)
            if not resp:
                return events

            soup = BeautifulSoup(resp.text, "html.parser")

            for tag in soup.find_all("a", href=True):
                text = tag.get_text(strip=True)
                href = tag.get("href", "")

                if len(text) < 15:
                    continue

                if any(w in text.lower() for w in ["auction", "sarfaesi", "property", "asset", "npa"]):
                    full_url = href if href.startswith("http") else self.SOURCE_URL + href
                    companies = self.extract_company_names(text)
                    company_name = companies[0] if companies else "SBI Auction Property"

                    events.append(self.make_event(
                        company_name=company_name,
                        keyword="asset auction",
                        category="asset_auction",
                        url=full_url,
                        headline=text,
                        snippet=text,
                        metadata={"bank": "State Bank of India"},
                    ))

        except Exception as e:
            self.logger.error(f"SBI auction crawl error: {e}")

        self.logger.info(f"SBI e-Auctions: {len(events)} events detected")
        return events
