import logging
import requests
from .firecrawl_client import FirecrawlSession
from bs4 import BeautifulSoup
from .base import BaseCrawler, DistressEvent

class MintCrawler(BaseCrawler):
    SOURCE_NAME = "Mint"
    SOURCE_URL = "https://www.livemint.com"
    CATEGORY = "financial_media"

    CRAWL_URLS = [
        "https://www.livemint.com/companies",
        "https://www.livemint.com/money",
        "https://www.livemint.com/industry",
    ]

    def crawl(self):
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
                    if len(text) < 30:
                        continue
                    keywords = self.detect_keywords(text)
                    if not keywords:
                        continue
                    full_url = href if href.startswith("http") else self.SOURCE_URL + href
                    companies = self.extract_company_names(text)
                    company_name = companies[0] if companies else "Unknown"
                    kw, category = keywords[0]
                    events.append(self.make_event(company_name=company_name, keyword=kw, category=category, url=full_url, headline=text, snippet=text))
            except Exception as e:
                self.logger.error(f"Mint crawl error on {url}: {e}")
        return events
