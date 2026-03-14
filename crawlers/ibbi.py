"""
crawlers/ibbi.py
Crawls IBBI (Insolvency & Bankruptcy Board of India) for CIRP, liquidation orders, and resolution notices.
"""

import logging
import requests
from bs4 import BeautifulSoup
from .base import BaseCrawler, DistressEvent

logger = logging.getLogger(__name__)


class IBBICrawler(BaseCrawler):
    SOURCE_NAME = "IBBI"
    SOURCE_URL = "https://ibbi.gov.in"
    CATEGORY = "regulatory"

    CRAWL_URLS = [
        "https://ibbi.gov.in/home/publicnotice",
        "https://ibbi.gov.in/home/order",
        "https://ibbi.gov.in/home/press_release",
        "https://ibbi.gov.in/home/liqnotice",
    ]

    def crawl(self) -> list[DistressEvent]:
        events = []
        session = requests.Session()

        for url in self.CRAWL_URLS:
            try:
                resp = self.safe_get(session, url)
                if not resp:
                    continue

                soup = BeautifulSoup(resp.text, "html.parser")
                rows = soup.find_all("tr")

                for row in rows[:self.MAX_ARTICLES]:
                    text = row.get_text(separator=" ", strip=True)
                    if len(text) < 10:
                        continue

                    keywords = self.detect_keywords(text)
                    if not keywords:
                        # IBBI content is inherently distress-related
                        keywords = [("cirp", "cirp")]

                    companies = self.extract_company_names(text)
                    company_name = companies[0] if companies else "IBBI Notice"

                    # Find link in row
                    link_tag = row.find("a", href=True)
                    article_url = url
                    if link_tag:
                        href = link_tag["href"]
                        article_url = href if href.startswith("http") else self.SOURCE_URL + href

                    headline = text[:200]
                    kw, category = keywords[0]

                    events.append(self.make_event(
                        company_name=company_name,
                        keyword=kw,
                        category=category,
                        url=article_url,
                        headline=headline,
                        snippet=text[:500],
                        metadata={"source_page": url},
                    ))

            except Exception as e:
                self.logger.error(f"IBBI crawl error on {url}: {e}")

        self.logger.info(f"IBBI: {len(events)} events detected")
        return events
