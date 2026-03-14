# crawlers/__init__.py
# ═══════════════════════════════════════════════════════════════════════════
# NEXUS ASIA — Complete Crawler Registry
# ═══════════════════════════════════════════════════════════════════════════

# ── Core distress crawlers ─────────────────────────────────────────────────
from .economic_times import EconomicTimesCrawler
from .business_standard import BusinessStandardCrawler
from .mint import MintCrawler
from .reuters import ReutersCrawler
from .ibbi import IBBICrawler
from .regulatory import NCLTCrawler, MCACrawler

# ── Bank auction crawlers (Tier 1 aggregators + Tier 2 direct + Tier 3 PDF) ─
from .multi_bank_auctions import (
    IBAPIAuctionCrawler,
    BankAuctionsCoInCrawler,
    SarfaesiDotComCrawler,
    BankOfBarodaAuctionCrawler,
    PNBAuctionCrawler,
    CanaraBankAuctionCrawler,
    UnionBankAuctionCrawler,
    BankOfMaharashtraAuctionCrawler,
    CentralBankAuctionCrawler,
    IndianOverseasBankAuctionCrawler,
    SBIAuctionCrawler,
)

# ── DRT / SARFAESI / Legal NPA crawlers ──────────────────────────────────
from .drt_sarfaesi import (
    DRTPortalCrawler,
    SARFAESINoticeCrawler,
    NPALawyerNetworkCrawler,
)

# ── Pre-leased CRE intelligence ──────────────────────────────────────────
from .cap_rate_market import CapRateMarketCrawler
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
# CRAWLER GROUPS
# ═══════════════════════════════════════════════════════════════════════════

DISTRESS_CRAWLERS = [
    EconomicTimesCrawler,
    BusinessStandardCrawler,
    MintCrawler,
    ReutersCrawler,
    IBBICrawler,
    NCLTCrawler,
    MCACrawler,
]

# Tier 1 aggregators first (highest yield), then direct banks, then PDF harvesters
BANK_AUCTION_CRAWLERS = [
    IBAPIAuctionCrawler,        # RBI aggregator — JSON API
    BankAuctionsCoInCrawler,    # Third-party aggregator — HTML cards
    SarfaesiDotComCrawler,      # SARFAESI notice aggregator
    BankOfBarodaAuctionCrawler,
    PNBAuctionCrawler,
    CanaraBankAuctionCrawler,
    UnionBankAuctionCrawler,
    BankOfMaharashtraAuctionCrawler,
    CentralBankAuctionCrawler,
    IndianOverseasBankAuctionCrawler,
    SBIAuctionCrawler,          # PDF link harvester
]

LEGAL_CRAWLERS = [
    DRTPortalCrawler,
    SARFAESINoticeCrawler,
    NPALawyerNetworkCrawler,
]

CRE_CRAWLERS = [
    CapRateMarketCrawler,   # weekly cap rate market intelligence
    PreLeasedCommercialCrawler,
    GradeAOfficeVacancyCrawler,
]

ARC_CRAWLERS = [
    NARCLCrawler,
    ARCPortfolioCrawler,
]

MARKET_CRAWLERS = [
    PEFundActivityCrawler,
    StockMarketDistressSignalCrawler,
]

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
