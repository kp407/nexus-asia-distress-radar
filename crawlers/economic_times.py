"""
crawlers/economic_times.py
Crawls Economic Times for distress signals.
"""

import logging
import requests
from bs4 import BeautifulSoup
from .base import BaseCrawler, DistressEvent

logger = logging.getLogger(__name__)


class EconomicTimesCrawler(BaseCrawler):
    SOURCE_NAME = "Economic Times"
    SOURCE_URL = "https://economictimes.indiatimes.com"
    CATEGORY = "financial_media"

    CRAWL_URLS = [
        "https://economictimes.indiatimes.com/markets/stocks/news",
        "https://economictimes.indiatimes.com/industry/banking/finance",
        "https://economictimes.indiatimes.com/small-biz/legal",
        "https://economictimes.indiatimes.com/industry",
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

                # ET uses various article link patterns
                article_links = []
                for tag in soup.find_all("a", href=True):
                    href = tag.get("href", "")
                    text = tag.get_text(strip=True)
                    if len(text) > 30 and ("/articleshow/" in href or "/news/" in href):
                        full_url = href if href.startswith("http") else self.SOURCE_URL + href
                        article_links.append((full_url, text))

                self.logger.info(f"ET: Found {len(article_links)} articles on {url}")

                for article_url, headline in article_links[:self.MAX_ARTICLES]:
                    keywords = self.detect_keywords(headline)
                    if keywords:
                        companies = self.extract_company_names(headline)
                        company_name = companies[0] if companies else "Unknown"

                        for kw, category in keywords[:1]:  # Top keyword per article
                            events.append(self.make_event(
                                company_name=company_name,
                                keyword=kw,
                                category=category,
                                url=article_url,
                                headline=headline,
                                snippet=headline,
                            ))

            except Exception as e:
                self.logger.error(f"ET crawl error on {url}: {e}")

        self.logger.info(f"Economic Times: {len(events)} events detected")
        return events
