import logging
import requests
from bs4 import BeautifulSoup
from .base import BaseCrawler, DistressEvent

class BusinessStandardCrawler(BaseCrawler):
    SOURCE_NAME = "Business Standard"
    SOURCE_URL = "https://www.business-standard.com"
    CATEGORY = "financial_media"

    CRAWL_URLS = [
        "https://www.business-standard.com/finance/news",
        "https://www.business-standard.com/companies",
        "https://www.business-standard.com/economy-policy/news",
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
                for tag in soup.find_all(["h2", "h3", "a"], href=True):
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
                self.logger.error(f"BS crawl error on {url}: {e}")
        return events