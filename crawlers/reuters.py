import logging
import requests
from bs4 import BeautifulSoup
from .base import BaseCrawler, DistressEvent

class ReutersCrawler(BaseCrawler):
    SOURCE_NAME = "Reuters"
    SOURCE_URL = "https://www.reuters.com"
    CATEGORY = "financial_media"

    CRAWL_URLS = [
        "https://www.reuters.com/world/india/",
        "https://www.reuters.com/business/finance/",
        "https://www.reuters.com/markets/asia/",
    ]

    def crawl(self):
        events = []
        session = requests.Session()
        for url in self.CRAWL_URLS:
            try:
                resp = self.safe_get(session, url)
                if not resp:
                    continue
                soup = BeautifulSoup(resp.text, "html.parser")
                articles = soup.find_all(["a", "h3"], attrs={"data-testid": True})
                if not articles:
                    articles = soup.find_all("a", href=True)
                for tag in articles[:self.MAX_ARTICLES]:
                    text = tag.get_text(strip=True)
                    href = tag.get("href", "")
                    if len(text) < 25:
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
                self.logger.error(f"Reuters crawl error on {url}: {e}")
        return events