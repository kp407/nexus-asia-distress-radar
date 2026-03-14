"""
crawlers/regulatory.py
Crawls MCA and NCLT for regulatory distress signals.
"""

import logging
import requests
from .firecrawl_client import FirecrawlSession
from bs4 import BeautifulSoup
from .base import BaseCrawler, DistressEvent

logger = logging.getLogger(__name__)


class NCLTCrawler(BaseCrawler):
    SOURCE_NAME = "NCLT"
    SOURCE_URL = "https://nclt.gov.in"
    CATEGORY = "regulatory"

    CRAWL_URLS = [
        "https://nclt.gov.in/latest-update",
        "https://nclt.gov.in/order-judgment",
    ]

    def crawl(self) -> list[DistressEvent]:
        events = []
        session = FirecrawlSession()

        for url in self.CRAWL_URLS:
            try:
                resp = self.safe_get(session, url)
                if not resp:
                    continue

                soup = BeautifulSoup(resp.text, "html.parser")

                for tag in soup.find_all(["li", "tr", "p", "a"]):
                    text = tag.get_text(strip=True)
                    if len(text) < 20:
                        continue

                    keywords = self.detect_keywords(text)
                    if not keywords:
                        # All NCLT content is inherently distress-related
                        keywords = [("nclt", "nclt")]

                    companies = self.extract_company_names(text)
                    company_name = companies[0] if companies else "NCLT Matter"

                    link = tag.find("a", href=True) if tag.name != "a" else tag
                    article_url = url
                    if link and link.get("href"):
                        href = link["href"]
                        article_url = href if href.startswith("http") else self.SOURCE_URL + href

                    kw, category = keywords[0]
                    events.append(self.make_event(
                        company_name=company_name,
                        keyword=kw,
                        category=category,
                        url=article_url,
                        headline=text[:200],
                        snippet=text[:500],
                        metadata={"regulatory_body": "NCLT"},
                    ))

                    if len(events) >= self.MAX_ARTICLES:
                        break

            except Exception as e:
                self.logger.error(f"NCLT crawl error on {url}: {e}")

        self.logger.info(f"NCLT: {len(events)} events detected")
        return events


class MCACrawler(BaseCrawler):
    SOURCE_NAME = "MCA"
    SOURCE_URL = "https://www.mca.gov.in"
    CATEGORY = "regulatory"

    CRAWL_URLS = [
        "https://www.mca.gov.in/content/mca/global/en/acts-rules/ebooks/notifications.html",
        "https://www.mca.gov.in/content/mca/global/en/about-us/press-notes.html",
    ]

    def crawl(self) -> list[DistressEvent]:
        events = []
        session = FirecrawlSession()

        for url in self.CRAWL_URLS:
            try:
                resp = self.safe_get(session, url)
                if not resp:
                    continue

                soup = BeautifulSoup(resp.text, "html.parser")

                for tag in soup.find_all("a", href=True):
                    text = tag.get_text(strip=True)
                    href = tag.get("href", "")

                    if len(text) < 20:
                        continue

                    keywords = self.detect_keywords(text)
                    if not keywords:
                        continue

                    full_url = href if href.startswith("http") else self.SOURCE_URL + href
                    companies = self.extract_company_names(text)
                    company_name = companies[0] if companies else "MCA Notice"

                    kw, category = keywords[0]
                    events.append(self.make_event(
                        company_name=company_name,
                        keyword=kw,
                        category=category,
                        url=full_url,
                        headline=text,
                        snippet=text,
                        metadata={"regulatory_body": "MCA"},
                    ))

            except Exception as e:
                self.logger.error(f"MCA crawl error on {url}: {e}")

        self.logger.info(f"MCA: {len(events)} events detected")
        return events
