"""
crawlers/base.py
Abstract base class for all Nexus Asia crawlers.
"""

import re
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# DISTRESS SIGNAL KEYWORDS & CATEGORIES
# ─────────────────────────────────────────────
SIGNAL_KEYWORDS = {
    "insolvency": ["insolvency", "insolvent"],
    "cirp": ["cirp", "corporate insolvency resolution process", "resolution professional", "resolution applicant"],
    "liquidation": ["liquidation", "liquidator", "winding up", "wound up"],
    "sarfaesi": ["sarfaesi", "symbolic possession", "physical possession", "secured creditor notice"],
    "default": ["default", "defaulted", "npa", "non-performing", "stressed loan", "bad loan"],
    "distressed_asset": ["distressed asset", "distressed sale", "stressed asset"],
    "restructuring": ["restructuring", "debt restructuring", "ots", "one time settlement", "haircut"],
    "debt_resolution": ["debt resolution", "resolution plan", "settlement plan"],
    "creditor_action": ["creditor action", "lender action", "debt recovery tribunal", "drt", "enforcement action"],
    "asset_auction": ["auction", "e-auction", "bank auction", "asset auction", "property auction", "reserve price"],
    "nclt": ["nclt", "national company law tribunal", "iba 2016", "ibc", "insolvency code"],
    "ibbi": ["ibbi", "insolvency board", "resolution applicant"],
    "bankruptcy": ["bankruptcy", "bankrupt"],
}

ALL_KEYWORDS = [kw for kws in SIGNAL_KEYWORDS.values() for kw in kws]

SEVERITY_MAP = {
    "liquidation": "critical",
    "cirp": "critical",
    "nclt": "high",
    "sarfaesi": "high",
    "asset_auction": "high",
    "insolvency": "high",
    "default": "medium",
    "distressed_asset": "medium",
    "restructuring": "medium",
    "debt_resolution": "medium",
    "creditor_action": "medium",
    "ibbi": "low",
    "bankruptcy": "critical",
}


@dataclass
class DistressEvent:
    """Structured distress signal event."""
    company_name: str
    signal_keyword: str
    signal_category: str
    source: str
    url: str
    headline: str = ""
    snippet: str = ""
    detected_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    published_at: Optional[str] = None
    severity: str = "medium"
    metadata: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "company_name": self.company_name,
            "signal_keyword": self.signal_keyword,
            "signal_category": self.signal_category,
            "source": self.source,
            "url": self.url,
            "headline": self.headline[:500] if self.headline else "",
            "snippet": self.snippet[:1000] if self.snippet else "",
            "detected_at": self.detected_at,
            "published_at": self.published_at,
            "severity": self.severity,
            "metadata": self.metadata,
        }


class BaseCrawler(ABC):
    """Abstract base for all crawlers."""

    SOURCE_NAME: str = "unknown"
    SOURCE_URL: str = ""
    CATEGORY: str = "other"

    REQUEST_TIMEOUT: int = 20
    MAX_ARTICLES: int = 50

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
    }

    def __init__(self):
        self.logger = logging.getLogger(f"crawler.{self.SOURCE_NAME}")

    @abstractmethod
    def crawl(self) -> list[DistressEvent]:
        """Crawl source and return list of distress events."""
        ...

    # ─────────────────────────────────────────────
    # SHARED UTILITIES
    # ─────────────────────────────────────────────

    def detect_keywords(self, text: str) -> list[tuple[str, str]]:
        """
        Scan text for distress keywords.
        Returns list of (keyword, category) tuples.
        """
        text_lower = text.lower()
        found = []
        seen = set()

        for category, keywords in SIGNAL_KEYWORDS.items():
            for kw in keywords:
                if kw in text_lower and kw not in seen:
                    found.append((kw, category))
                    seen.add(kw)

        return found

    def extract_company_names(self, text: str) -> list[str]:
        """
        Heuristic company name extractor.
        Looks for patterns like "XYZ Ltd", "ABC Private Limited", etc.
        """
        patterns = [
            r'\b([A-Z][A-Za-z\s&]+(?:Ltd\.?|Limited|Pvt\.?\s*Ltd\.?|Private\s+Limited|'
            r'Industries|Corporation|Corp\.?|Inc\.?|LLP|Holdings|Enterprises|'
            r'Infrastructure|Finance|Capital|Solutions|Technologies|Energy|Power|'
            r'Realty|Real\s*Estate|Steel|Cement|Chemicals|Pharma|Textiles))\b',
        ]
        companies = []
        for pattern in patterns:
            matches = re.findall(pattern, text)
            companies.extend([m.strip() for m in matches if len(m.strip()) > 4])

        # Deduplicate while preserving order
        seen = set()
        unique = []
        for c in companies:
            key = c.lower()
            if key not in seen:
                seen.add(key)
                unique.append(c)
        return unique[:5]  # Return top 5 candidates

    def get_severity(self, category: str) -> str:
        return SEVERITY_MAP.get(category, "medium")

    def make_event(
        self,
        company_name: str,
        keyword: str,
        category: str,
        url: str,
        headline: str = "",
        snippet: str = "",
        published_at: str = None,
        metadata: dict = None,
    ) -> DistressEvent:
        return DistressEvent(
            company_name=company_name,
            signal_keyword=keyword,
            signal_category=category,
            source=self.SOURCE_NAME,
            url=url,
            headline=headline,
            snippet=snippet,
            published_at=published_at,
            severity=self.get_severity(category),
            metadata=metadata or {},
        )

    def safe_get(self, session, url: str, **kwargs) -> object | None:
        """HTTP GET with error handling. kwargs override defaults (incl. timeout)."""
        timeout = kwargs.pop('timeout', self.REQUEST_TIMEOUT)
        try:
            resp = session.get(
                url,
                headers=self.HEADERS,
                timeout=timeout,
                **kwargs,
            )
            if resp.status_code == 200:
                return resp
            self.logger.warning(f"HTTP {resp.status_code} for {url}")
            return None
        except Exception as e:
            self.logger.error(f"Request failed for {url}: {e}")
            return None
