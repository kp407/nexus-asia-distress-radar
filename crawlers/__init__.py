# crawlers/__init__.py
# ═══════════════════════════════════════════════════════════════════════════
# NEXUS ASIA — Complete Crawler Registry
# ═══════════════════════════════════════════════════════════════════════════

# ── Original crawlers ─────────────────────────────────────────────────────
from .economic_times import EconomicTimesCrawler
from .business_standard import BusinessStandardCrawler
from .mint import MintCrawler
from .reuters import ReutersCrawler
from .ibbi import IBBICrawler
from .regulatory import NCLTCrawler, MCACrawler
from .bank_auctions import IBAPIAuctionCrawler, SBIAuctionCrawler

# ── Multi-bank auction crawlers (8 banks + aggregator) ───────────────────
from .multi_bank_auctions import (
    BankOfBarodaAuctionCrawler,
    PNBauctionCrawler,
    CanaraBankAuctionCrawler,
    UnionBankAuctionCrawler,
    BankOfMaharashtraAuctionCrawler,
    CentralBankAuctionCrawler,
    IndianOverseasBankAuctionCrawler,
    BankAuctionDotInCrawler,
)

# ── DRT / SARFAESI / Legal NPA crawlers ──────────────────────────────────
from .drt_sarfaesi import (
    DRTPortalCrawler,
    SARFAESINoticeCrawler,
    NPALawyerNetworkCrawler,
)

# ── Pre-leased CRE intelligence ──────────────────────────────────────────
from .pre_leased_cre import (
    PreLeasedCommercialCrawler,
    GradeAOfficeVacancyCrawler,
)

# ── NARCL / ARC portfolio crawlers ───────────────────────────────────────
from .narcl_arc import (
    NARCLCrawler,
    ARCPortfolioCrawler,
)

# ── PE / Family Office / Market distress ─────────────────────────────────
from .investor_deal_match import (
    PEFundActivityCrawler,
    StockMarketDistressSignalCrawler,
)


# ═══════════════════════════════════════════════════════════════════════════
# CRAWLER GROUPS — use these to run subsets
# ═══════════════════════════════════════════════════════════════════════════

# Core distress signal crawlers (run every 30 min)
DISTRESS_CRAWLERS = [
    EconomicTimesCrawler,
    BusinessStandardCrawler,
    MintCrawler,
    ReutersCrawler,
    IBBICrawler,
    NCLTCrawler,
    MCACrawler,
]

# Bank auction crawlers (run every 4 hours)
BANK_AUCTION_CRAWLERS = [
    IBAPIAuctionCrawler,
    SBIAuctionCrawler,
    BankOfBarodaAuctionCrawler,
    PNBauctionCrawler,
    CanaraBankAuctionCrawler,
    UnionBankAuctionCrawler,
    BankOfMaharashtraAuctionCrawler,
    CentralBankAuctionCrawler,
    IndianOverseasBankAuctionCrawler,
    BankAuctionDotInCrawler,
]

# DRT + Legal NPA — pre-auction intelligence (run daily)
LEGAL_CRAWLERS = [
    DRTPortalCrawler,
    SARFAESINoticeCrawler,
    NPALawyerNetworkCrawler,
]

# CRE intelligence — pre-leased assets (run daily)
CRE_CRAWLERS = [
    PreLeasedCommercialCrawler,
    GradeAOfficeVacancyCrawler,
]

# ARC / NARCL portfolio (run daily)
ARC_CRAWLERS = [
    NARCLCrawler,
    ARCPortfolioCrawler,
]

# Market signals (run every 4 hours)
MARKET_CRAWLERS = [
    PEFundActivityCrawler,
    StockMarketDistressSignalCrawler,
]

# ─── Master list ───────────────────────────────────────────────────────────
ALL_CRAWLERS = (
    DISTRESS_CRAWLERS +
    BANK_AUCTION_CRAWLERS +
    LEGAL_CRAWLERS +
    CRE_CRAWLERS +
    ARC_CRAWLERS +
    MARKET_CRAWLERS
)

__all__ = [
    "ALL_CRAWLERS",
    "DISTRESS_CRAWLERS",
    "BANK_AUCTION_CRAWLERS",
    "LEGAL_CRAWLERS",
    "CRE_CRAWLERS",
    "ARC_CRAWLERS",
    "MARKET_CRAWLERS",
]