"""
crawler.py — Nexus Asia Distress Radar
═══════════════════════════════════════════════════════════════════
USAGE:
  python crawler.py                        # distress group (default)
  python crawler.py --group distress       # LiveLaw + Google News + Media RSS
  python crawler.py --group bank_auction   # bank portals only
  python crawler.py --group legal          # DRT + SARFAESI
  python crawler.py --group cre            # pre-leased assets
  python crawler.py --group arc            # NARCL + ARC
  python crawler.py --group market         # PE + market signals
  python crawler.py --group all            # everything
  python crawler.py --dry-run              # no DB writes

INTELLIGENCE PIPELINE (--group distress or --group all):
  1. LiveLaw          — Real-time NCLT/NCLAT order reporting
  2. Bar & Bench      — NCLT/NCLAT/HC insolvency coverage
  3. RBI              — Enforcement actions, wilful defaulters
  4. Google News RSS  — 25 targeted IBBI/NCLT/distress searches
  5. Financial Media  — ET, BS, Mint, Moneycontrol, BusinessLine

REMOVED (block GitHub Actions IPs):
  ✗ NCLT cause list PDFs  — Cloudflare blocks datacenter IPs
  ✗ IBBI Excel downloads  — Same
  ✗ Indian Kanoon         — Rate limits + blocks
  ✗ MCA direct            — Blocks
"""

import os, sys, uuid, logging, time, re, argparse
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, date
from urllib.parse import quote
import requests
from crawlers.firecrawl_client import FirecrawlSession
from bs4 import BeautifulSoup

# ═══════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("nexus.main")

# ═══════════════════════════════════════════════
# SUPABASE
# ═══════════════════════════════════════════════
SUPABASE_URL         = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_ANON_KEY    = os.environ.get("SUPABASE_ANON_KEY", "")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "") or SUPABASE_ANON_KEY

def _dbh(write=False):
    key = SUPABASE_SERVICE_KEY if write else SUPABASE_ANON_KEY
    return {
        "apikey":        key,
        "Authorization": f"Bearer {key}",
        "Content-Type":  "application/json",
        "Prefer":        "return=minimal",
    }

# All valid signal_category values — must match Supabase CHECK constraint
_VALID_CATEGORIES = {
    'insolvency', 'auction', 'restructuring', 'default', 'legal',
    'regulatory', 'general', 'sarfaesi', 'creditor_action', 'rbi_action',
    'distressed_asset', 'cirp', 'liquidation', 'pre_leased_asset',
    'cre_vacancy', 'arc_portfolio', 'pe_activity', 'market_stress',
    'financial_media', 'nclt', 'ibbi', 'bankruptcy', 'debt_resolution',
    'asset_auction', 'other',
}

def _sanitise_event(row: dict) -> dict:
    """Ensure signal_category is always a valid DB value."""
    row = dict(row)
    cat = row.get('signal_category', 'other')
    if cat not in _VALID_CATEGORIES:
        # Map common crawler values that aren't in constraint
        _CAT_MAP = {
            'pre_leased_cre':    'pre_leased_asset',
            'cre':               'pre_leased_asset',
            'arc':               'arc_portfolio',
            'pe_fund':           'pe_activity',
            'market_distress':   'market_stress',
            'financial media':   'financial_media',
        }
        row['signal_category'] = _CAT_MAP.get(cat, 'other')
    return row

def db_insert(rows):
    if not SUPABASE_URL: return False
    try:
        payload = rows if isinstance(rows, list) else [rows]
        payload = [_sanitise_event(r) for r in payload]
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/distress_events",
            headers=_dbh(write=True),
            json=payload,
            timeout=15,
        )
        if r.status_code not in (200, 201):
            logger.error(f"DB insert failed {r.status_code}: {r.text[:200]}")
            return False
        return True
    except Exception as e:
        logger.error(f"DB insert: {e}")
        return False

def db_is_duplicate(company, keyword, source):
    try:
        today = datetime.now(timezone.utc).strftime('%Y-%m-%dT00:00:00Z')
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/distress_events",
            headers=_dbh(),
            params={
                "company_name":   f"ilike.{company}",
                "signal_keyword": f"eq.{keyword}",
                "source":         f"eq.{source}",
                "detected_at":    f"gte.{today}",
                "select": "id", "limit": "1",
            },
            timeout=10,
        )
        return r.status_code == 200 and len(r.json()) > 0
    except:
        return False

def db_upsert_company(name):
    if name in ("Unknown", ""): return
    try:
        now = datetime.now(timezone.utc).isoformat()
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/companies",
            headers=_dbh(),
            params={"name": f"eq.{name}", "select": "id,signal_count", "limit": "1"},
            timeout=8,
        )
        if r.status_code == 200 and r.json():
            cid = r.json()[0]["id"]
            cnt = (r.json()[0].get("signal_count") or 0) + 1
            requests.patch(
                f"{SUPABASE_URL}/rest/v1/companies",
                headers=_dbh(write=True),
                params={"id": f"eq.{cid}"},
                json={"last_signal_at": now, "signal_count": cnt, "updated_at": now},
                timeout=8,
            )
        else:
            requests.post(
                f"{SUPABASE_URL}/rest/v1/companies",
                headers={**_dbh(write=True), "Prefer": "return=minimal"},
                json={"name": name, "first_signal_at": now,
                      "last_signal_at": now, "signal_count": 1},
                timeout=8,
            )
    except:
        pass

# ═══════════════════════════════════════════════
# ORDER DATE EXTRACTOR
# ═══════════════════════════════════════════════
_MONTHS = {
    'jan':1,'feb':2,'mar':3,'apr':4,'may':5,'jun':6,
    'jul':7,'aug':8,'sep':9,'oct':10,'nov':11,'dec':12,
    'january':1,'february':2,'march':3,'april':4,'june':6,
    'july':7,'august':8,'september':9,'october':10,
    'november':11,'december':12,
}
_DATE_CONTEXT = [
    'heard on','hearing on','hearing date','order dated','order date',
    'date of order','decided on','listed on','listed for',
    'matter listed','case listed','pronounced on','reserved on',
    'judgment dated','judgement dated','bench heard','court held',
    'pub:','pubdate','published on','date:','| date',
]
_DATE_PATS = [
    (r'\b(20\d{2})[-/](0?[1-9]|1[0-2])[-/](0?[1-9]|[12]\d|3[01])\b', 'ymd'),
    (r'\b(0?[1-9]|[12]\d|3[01])[.\-/](0?[1-9]|1[0-2])[.\-/](20\d{2})\b', 'dmy'),
    (r'\b(0?[1-9]|[12]\d|3[01])(?:st|nd|rd|th)?\s+'
     r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|'
     r'jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)'
     r'\s+(20\d{2})\b', 'dmy_str'),
    (r'\b(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|'
     r'jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)'
     r'\s+(0?[1-9]|[12]\d|3[01]),?\s+(20\d{2})\b', 'mdy_str'),
]

def _parse_date_match(m, fmt):
    try:
        if fmt == 'ymd':    return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        if fmt == 'dmy':    return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        if fmt == 'dmy_str':
            mon = _MONTHS.get(m.group(2).lower()[:3])
            if mon: return date(int(m.group(3)), mon, int(m.group(1)))
        if fmt == 'mdy_str':
            mon = _MONTHS.get(m.group(1).lower()[:3])
            if mon: return date(int(m.group(3)), mon, int(m.group(2)))
    except (ValueError, AttributeError):
        pass
    return None

def extract_order_date(title, snippet="", pub_str=""):
    combined = f"{title} {snippet}"
    tl = combined.lower()
    for kw in _DATE_CONTEXT:
        idx = tl.find(kw)
        if idx < 0: continue
        window = combined[idx: idx + 80]
        for pat, fmt in _DATE_PATS:
            m = re.search(pat, window, re.IGNORECASE)
            if m:
                d = _parse_date_match(m, fmt)
                if d and date(2015, 1, 1) <= d <= date.today():
                    return d.isoformat()
    for pat, fmt in _DATE_PATS:
        m = re.search(pat, title, re.IGNORECASE)
        if m:
            d = _parse_date_match(m, fmt)
            if d and date(2015, 1, 1) <= d <= date.today():
                return d.isoformat()
    if pub_str:
        for pat, fmt in _DATE_PATS:
            m = re.search(pat, pub_str, re.IGNORECASE)
            if m:
                d = _parse_date_match(m, fmt)
                if d and date(2015, 1, 1) <= d <= date.today():
                    return d.isoformat()
    return None

# ═══════════════════════════════════════════════
# KEYWORDS
# ═══════════════════════════════════════════════
KEYWORDS = {
    "cirp": [
        "cirp", "corporate insolvency resolution process",
        "resolution professional", "resolution applicant",
        "committee of creditors", "coc approved",
        "admitted application", "resolution plan approved",
        "section 7 application", "section 9 application",
        "operational creditor", "financial creditor",
        "moratorium", "irp appointed", "rp appointed",
    ],
    "liquidation": [
        "liquidation", "liquidator appointed", "winding up",
        "wound up", "liquidation order", "liquidation estate",
        "section 33 ibc", "going concern sale",
        "liquidation value", "no resolution plan",
    ],
    "insolvency": [
        "insolvency", "insolvent", "insolvency petition",
        "insolvency application", "insolvency proceedings",
        "insolvency resolution",
    ],
    "nclt": [
        "nclt", "national company law tribunal",
        "nclat", "national company law appellate",
        "ibc 2016", "insolvency and bankruptcy code",
        "company petition", "ib/", "cp(ib)",
    ],
    "ibbi": [
        "ibbi", "insolvency and bankruptcy board",
        "ibbi order", "ibbi circular", "ibbi regulation",
        "insolvency professional agency", "ibbi show cause",
        "disciplinary committee ibbi", "ibbi inspection",
        "ibbi annual report",
    ],
    "sarfaesi": [
        "sarfaesi", "sarfaesi act 2002",
        "symbolic possession notice", "physical possession notice",
        "secured creditor notice", "section 13(2)",
        "section 13(4)", "secured asset auction",
        "possession under sarfaesi",
    ],
    "asset_auction": [
        "bank auction", "e-auction", "auction notice",
        "reserve price", "bid document", "auction date",
        "earnest money deposit", "npa auction",
        "asset sale notice", "sale notice bank",
        "highest bidder bank",
    ],
    "default": [
        "wilful defaulter", "wilful default",
        "npa", "non-performing asset",
        "stressed loan", "bad loan", "stressed account",
        "overdue account", "recall notice bank",
        "demand notice bank", "account classified npa",
        "special mention account", "sma-2",
    ],
    "distressed_asset": [
        "distressed asset", "distressed sale",
        "stressed asset", "asset reconstruction company",
        "arc purchase", "security receipts",
        "15 percent haircut", "debt haircut",
        "write-off bank",
    ],
    "restructuring": [
        "debt restructuring", "one time settlement",
        "ots approved", "debt recast",
        "resolution framework rbi",
        "restructuring package",
        "ever-greening loan",
    ],
    "creditor_action": [
        "debt recovery tribunal", "drt order",
        "recovery certificate drt",
        "enforcement action bank",
        "attachment order bank",
        "recovery proceedings initiated",
        "lok adalat settlement",
    ],
    "fraud": [
        "wilful defaulter list rbi",
        "forensic audit findings",
        "fund diversion", "siphoning of funds",
        "look out circular debtor",
        "fir registered company",
        "fraudulent trading",
        "transaction set aside nclt",
    ],
    "rbi_action": [
        "rbi enforcement action",
        "rbi penalty bank",
        "rbi cancels licence",
        "rbi prompt corrective action",
        "pca framework rbi",
        "rbi directions bank",
    ],
}

SEVERITY = {
    "liquidation": "critical", "cirp": "critical", "fraud": "critical",
    "nclt": "high", "sarfaesi": "high", "asset_auction": "high", "insolvency": "high",
    "rbi_action": "high",
    "default": "medium", "distressed_asset": "medium",
    "restructuring": "medium", "creditor_action": "medium", "ibbi": "medium",
}

def detect_keywords(text):
    t = text.lower()
    found, seen = [], set()
    for cat, kws in KEYWORDS.items():
        for kw in kws:
            if kw in t and kw not in seen:
                found.append((kw, cat))
                seen.add(kw)
    return found

# ═══════════════════════════════════════════════
# COMPANY EXTRACTOR
# ═══════════════════════════════════════════════
SUFFIX_RE = re.compile(
    r'\b([A-Z][A-Za-z0-9\s&\-\'\.]{2,50}\s+'
    r'(?:Ltd\.?|Limited|Pvt\.?\s*Ltd\.?|Private\s+Limited|LLP|'
    r'Industries|Corporation|Corp\.?|Holdings|Enterprises|'
    r'Infrastructure|Finance|Capital|Solutions|Technologies|'
    r'Energy|Power|Realty|Real\s*Estate|Steel|Cement|Chemicals|'
    r'Pharma|Pharmaceuticals|Textiles|Motors|Automobiles|'
    r'Bank|Financial\s+Services|Properties|Developers|'
    r'Construction|Engineering|Exports|Group|Media|'
    r'Retail|Aviation|Airlines|Telecom|Towers|Logistics|'
    r'Hospitality|Hotels|Foods|Beverages|Agro|Seeds))\b'
)
ACTION_RE = re.compile(
    r'\b([A-Z][A-Za-z\s&\-\']{3,45})\s+'
    r'(?:has been|was|is being|will be|faces|filed|'
    r'admitted|owes|defaults|moves|seeks|undergoes|'
    r'enters|initiates|challenges|appeals|directed|ordered)'
)
QUOTE_RE = re.compile(r'["\u201c]([A-Z][^"\u201d]{5,60})["\u201d]')

KNOWN_COMPANIES = [
    "Byju's", "BYJU'S", "Think & Learn",
    "IL&FS", "IL&FS Engineering", "IL&FS Transportation",
    "DHFL", "Dewan Housing Finance",
    "Jet Airways", "Videocon Industries", "Videocon d2h",
    "Essar Steel", "Essar Ports", "Essar Power",
    "Bhushan Steel", "Bhushan Power and Steel",
    "Altico Capital", "Sintex Industries", "Sintex Plastics",
    "Srei Equipment Finance", "Srei Infrastructure Finance",
    "Future Retail", "Future Enterprises", "Future Supply Chain",
    "Jaypee Infratech", "Jaiprakash Associates",
    "Unitech", "Amtek Auto", "Monnet Ispat",
    "Era Infra Engineering", "Lanco Infratech",
    "GTL Infrastructure", "Suzlon Energy",
    "Kingfisher Airlines", "United Breweries",
    "Reliance Capital", "Reliance Commercial Finance",
    "Reliance Home Finance", "Reliance Infratel",
    "Yes Bank", "Lakshmi Vilas Bank",
    "CG Power", "Ruchi Soya",
    "Jyoti Structures", "Electrosteel Steels",
    "Punj Lloyd", "Aircel", "Religare Finvest",
    "ABG Shipyard", "Coastal Projects",
    "McNally Bharat", "Sterling Biotech",
    "Rotomac Global", "Winsome Diamonds",
    "Cafe Coffee Day", "Coffee Day Enterprises",
    "ADAG", "Reliance Communications",
    "Eveready Industries", "Vakrangee",
    "Go First", "Go Airlines",
    "SpiceJet", "Vodafone Idea",
    "Adani Wilmar", "Adani Enterprises",
    "Zee Entertainment", "Subhash Chandra",
    "Indiabulls Housing", "HDIL",
    "Peninsula Land", "Omkar Realtors",
    "Supertech", "Amrapali",
    "3i Infotech", "Simbhaoli Sugars",
]

def extract_company(text):
    m = SUFFIX_RE.search(text)
    if m:
        return re.sub(r'\s+', ' ', m.group(1)).strip()
    for c in KNOWN_COMPANIES:
        if c.lower() in text.lower():
            return c
    m2 = ACTION_RE.search(text)
    if m2: return m2.group(1).strip()
    m3 = QUOTE_RE.search(text)
    if m3: return m3.group(1).strip()
    return "Unknown"

# ═══════════════════════════════════════════════
# HTTP
# ═══════════════════════════════════════════════
BASE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

def make_session():
    s = FirecrawlSession()
    s.headers.update(BASE_HEADERS)
    return s

def safe_get(session, url, timeout=20, **kwargs):
    try:
        r = session.get(url, timeout=timeout, **kwargs)
        if r.status_code == 200:
            return r
        logger.warning(f"  HTTP {r.status_code} — {url[:80]}")
        return None
    except requests.exceptions.Timeout:
        logger.warning(f"  Timeout — {url[:70]}")
        return None
    except Exception as e:
        logger.warning(f"  Error — {str(e)[:60]} — {url[:60]}")
        return None

def make_event(company, kw, cat, source, url,
               headline="", snippet="", pub=None, meta=None):
    order_date = extract_order_date(headline, snippet, pub or "")
    return {
        "company_name":    company,
        "signal_keyword":  kw,
        "signal_category": cat,
        "source":          source,
        "url":             url,
        "headline":        headline[:500],
        "snippet":         snippet[:900],
        "detected_at":     datetime.now(timezone.utc).isoformat(),
        "published_at":    pub,
        "order_date":      order_date,
        "severity":        SEVERITY.get(cat, "medium"),
        "is_duplicate":    False,
        "metadata":        meta or {},
    }

# ═══════════════════════════════════════════════
# RSS PARSER
# ═══════════════════════════════════════════════
def parse_rss(xml_text, fallback_url=""):
    items = []
    try:
        root = ET.fromstring(xml_text)
        for item in root.iter("item"):
            t  = (item.findtext("title") or "").strip()
            l  = (item.findtext("link")  or fallback_url).strip()
            d  = item.findtext("pubDate") or ""
            ds_raw = item.findtext("description") or ""
            ds = BeautifulSoup(ds_raw, "html.parser").get_text(separator=" ", strip=True) if len(ds_raw) > 20 else ds_raw
            if t:
                items.append((t, l, d, ds))
        if not items:
            ans = "http://www.w3.org/2005/Atom"
            for e in root.iter(f"{{{ans}}}entry"):
                t  = (e.findtext(f"{{{ans}}}title") or "").strip()
                le = e.find(f"{{{ans}}}link")
                l  = le.get("href", fallback_url) if le is not None else fallback_url
                d  = e.findtext(f"{{{ans}}}updated") or ""
                ds = BeautifulSoup(
                    e.findtext(f"{{{ans}}}summary") or "", "html.parser"
                ).get_text(separator=" ", strip=True)
                if t:
                    items.append((t, l, d, ds))
    except ET.ParseError:
        soup = BeautifulSoup(xml_text, "html.parser")
        for item in soup.find_all(["item", "entry"]):
            t_el = item.find("title")
            l_el = item.find(["link", "guid"])
            t = t_el.get_text(strip=True) if t_el else ""
            l = l_el.get_text(strip=True) if l_el else fallback_url
            if t:
                items.append((t, l, "", ""))
    return items

def events_from_feed(items, source_name, feed_url):
    events = []
    for title, link, pub, desc in items:
        kws = detect_keywords(f"{title} {desc}")
        if not kws:
            continue
        company = extract_company(title)
        kw, cat = kws[0]
        events.append(make_event(
            company=company, kw=kw, cat=cat,
            source=source_name, url=link,
            headline=title, snippet=desc[:500],
            pub=pub[:50] if pub else None,
            meta={"feed_url": feed_url},
        ))
    return events

# ═══════════════════════════════════════════════
# SOURCE 1: LIVELAW
# ═══════════════════════════════════════════════
LIVELAW_RSS = [
    ("LiveLaw", "https://www.livelaw.in/feed/"),           # correct current URL
    ("LiveLaw", "https://www.livelaw.in/rss/"),
    ("LiveLaw", "https://www.livelaw.in/top-stories/rss/"),
]
LIVELAW_SEARCH = [
    ("LiveLaw / NCLT",  "https://www.livelaw.in/?s=NCLT+insolvency"),
    ("LiveLaw / NCLT",  "https://www.livelaw.in/?s=CIRP"),
    ("LiveLaw / NCLT",  "https://www.livelaw.in/?s=liquidation+NCLT"),
    ("LiveLaw / IBBI",  "https://www.livelaw.in/?s=IBBI"),
    ("LiveLaw / NCLAT", "https://www.livelaw.in/?s=NCLAT"),
]

def crawl_livelaw(session):
    events = []
    logger.info("  ── LiveLaw (NCLT/NCLAT primary tracker)")

    for source, url in LIVELAW_RSS:
        r = safe_get(session, url, timeout=15)
        if not r:
            continue
        items = parse_rss(r.text, url)
        ibc_items = [i for i in items if detect_keywords(f"{i[0]} {i[3]}")]
        ev = events_from_feed(ibc_items, source, url)
        events.extend(ev)
        logger.info(f"  LiveLaw RSS: {len(items)} items → {len(ev)} IBC signals")
        if ev:
            break   # got data from this feed, skip fallback URLs
        time.sleep(1)

    for source, url in LIVELAW_SEARCH:
        r = safe_get(session, url, timeout=15)
        if not r:
            continue
        soup = BeautifulSoup(r.text, "html.parser")
        articles = (
            soup.select("article.jeg_post") or
            soup.select(".jeg_post") or
            soup.select("article") or
            soup.select(".post-item")
        )
        for art in articles[:20]:
            title_el = art.find(["h2", "h3", "h4"])
            link_el  = art.find("a", href=True)
            desc_el  = art.find(["p", ".jeg_post_excerpt"])
            if not title_el: continue
            title   = title_el.get_text(strip=True)
            link    = link_el["href"] if link_el else url
            snippet = desc_el.get_text(strip=True) if desc_el else ""
            if not link.startswith("http"):
                link = "https://www.livelaw.in" + link
            kws = detect_keywords(f"{title} {snippet}")
            if not kws: continue
            company = extract_company(title)
            kw, cat = kws[0]
            case_match = re.search(
                r'(?:IB|CP\(IB\)|MA|CA|IA)[\s\/\-]\d+[\s\/\-]\w+[\s\/\-]\d{4}',
                f"{title} {snippet}", re.IGNORECASE
            )
            events.append(make_event(
                company=company, kw=kw, cat=cat,
                source=source, url=link,
                headline=title, snippet=snippet,
                meta={"source_type": "legal_media",
                      "case_number": case_match.group(0) if case_match else "",
                      "authentic_coverage": True},
            ))
        logger.info(f"  {source} [{url.split('=')[-1]}]: {len(articles)} articles scanned")
        time.sleep(1.5)

    logger.info(f"  LiveLaw total: {len(events)} events")
    return events

# ═══════════════════════════════════════════════
# SOURCE 2: BAR & BENCH
# ═══════════════════════════════════════════════
BAR_BENCH_RSS = [
    ("Bar & Bench", "https://www.barandbench.com/feed"),
    ("Bar & Bench", "https://www.barandbench.com/feed/columns"),
]
BAR_BENCH_SEARCH = [
    ("Bar & Bench / NCLT",  "https://www.barandbench.com/?s=NCLT+insolvency"),
    ("Bar & Bench / IBBI",  "https://www.barandbench.com/?s=IBBI"),
    ("Bar & Bench / CIRP",  "https://www.barandbench.com/?s=CIRP"),
    ("Bar & Bench / NCLAT", "https://www.barandbench.com/?s=NCLAT"),
]

def crawl_bar_bench(session):
    events = []
    logger.info("  ── Bar & Bench (NCLT/NCLAT/HC insolvency)")
    for source, url in BAR_BENCH_RSS:
        r = safe_get(session, url, timeout=15)
        if not r: continue
        items = parse_rss(r.text, url)
        ibc_items = [i for i in items if detect_keywords(f"{i[0]} {i[3]}")]
        ev = events_from_feed(ibc_items, source, url)
        events.extend(ev)
        logger.info(f"  Bar & Bench RSS: {len(items)} items → {len(ev)} IBC signals")
        time.sleep(1)
    for source, url in BAR_BENCH_SEARCH:
        r = safe_get(session, url, timeout=15)
        if not r: continue
        soup = BeautifulSoup(r.text, "html.parser")
        articles = soup.select("article, .post, .story-card")
        for art in articles[:15]:
            title_el = art.find(["h2", "h3"])
            link_el  = art.find("a", href=True)
            desc_el  = art.find("p")
            if not title_el: continue
            title   = title_el.get_text(strip=True)
            link    = link_el["href"] if link_el else url
            snippet = desc_el.get_text(strip=True) if desc_el else ""
            if not link.startswith("http"):
                link = "https://www.barandbench.com" + link
            kws = detect_keywords(f"{title} {snippet}")
            if not kws: continue
            company = extract_company(title)
            kw, cat = kws[0]
            events.append(make_event(
                company=company, kw=kw, cat=cat,
                source=source, url=link,
                headline=title, snippet=snippet,
                meta={"source_type": "legal_media"},
            ))
        logger.info(f"  {source}: {len(articles)} articles scanned")
        time.sleep(1.5)
    logger.info(f"  Bar & Bench total: {len(events)} events")
    return events

# ═══════════════════════════════════════════════
# SOURCE 3: RBI
# RSS returns 418 (bot block) — skip RSS, use HTML only
# ═══════════════════════════════════════════════
RBI_HTML_URLS = [
    "https://www.rbi.org.in/Scripts/BS_PressReleaseDisplay.aspx",
    "https://www.rbi.org.in/Scripts/BS_EnforcementDisplay.aspx",
]

def crawl_rbi(session):
    events = []
    logger.info("  ── RBI (enforcement actions, wilful defaulters)")
    for url in RBI_HTML_URLS:
        r = safe_get(session, url, timeout=20)
        if not r: continue
        soup = BeautifulSoup(r.text, "html.parser")
        rows = soup.select("table tr") or soup.select(".tablebg tr")
        count = 0
        for row in rows[:40]:
            text = row.get_text(separator=" ", strip=True)
            if len(text) < 20: continue
            kws = detect_keywords(text)
            if not kws:
                if "enforcement" in url.lower():
                    kws = [("rbi enforcement action", "rbi_action")]
                else:
                    continue
            link_el = row.find("a", href=True)
            link = url
            if link_el:
                href = link_el["href"]
                link = href if href.startswith("http") else f"https://www.rbi.org.in{href}"
            company = extract_company(text)
            kw, cat = kws[0]
            events.append(make_event(
                company=company, kw=kw, cat=cat,
                source="RBI", url=link,
                headline=text[:300], snippet=text[:600],
                meta={"source_type": "rbi_official", "authentic": True},
            ))
            count += 1
        logger.info(f"  RBI {url[-35:]}: {len(rows)} rows → {count} events")
        time.sleep(1)
    logger.info(f"  RBI total: {len(events)} events")
    return events

# ═══════════════════════════════════════════════
# SOURCE 4: GOOGLE NEWS RSS
# ═══════════════════════════════════════════════
GNEWS_QUERIES = [
    ("IBBI",          "IBBI insolvency order India 2024"),
    ("IBBI",          "IBBI circular insolvency professional India"),
    ("IBBI",          "IBBI show cause notice order India"),
    ("IBBI",          "IBBI disciplinary committee order India"),
    ("NCLT",          "NCLT CIRP admitted India 2024"),
    ("NCLT",          "NCLT liquidation order India"),
    ("NCLT",          "NCLT resolution plan approved India"),
    ("NCLT",          "NCLT section 7 petition admitted"),
    ("NCLT",          "NCLT section 9 operational creditor India"),
    ("NCLAT",         "NCLAT insolvency appeal India"),
    ("NCLAT",         "NCLAT upholds liquidation India"),
    ("SARFAESI",      "SARFAESI possession notice India bank 2024"),
    ("Auction",       "bank auction NPA property India 2024"),
    ("Auction",       "e-auction stressed asset India bank"),
    ("Default",       "wilful defaulter India RBI 2024"),
    ("Default",       "NPA account India bank stressed"),
    ("Default",       "loan default NCLT IBC India"),
    ("Restructuring", "one time settlement OTS India bank 2024"),
    ("Restructuring", "debt restructuring RBI India framework"),
    ("Legal",         "DRT debt recovery tribunal order India"),
    ("Legal",         "winding up petition High Court India"),
    ("Fraud",         "forensic audit India company fund diversion"),
    ("Fraud",         "look out circular India debtor"),
    ("RBI",           "RBI cancels bank licence India 2024"),
    ("RBI",           "RBI prompt corrective action bank India"),
]
GNEWS_BASE = "https://news.google.com/rss/search?q={q}&hl=en-IN&gl=IN&ceid=IN:en"

def crawl_google_news(session):
    events = []
    logger.info(f"  ── Google News RSS ({len(GNEWS_QUERIES)} targeted searches)")
    for label, query in GNEWS_QUERIES:
        url = GNEWS_BASE.format(q=quote(query))
        r = safe_get(session, url, timeout=20)
        if not r: continue
        items = parse_rss(r.text, url)
        count = 0
        for title, link, pub, desc in items:
            kws = detect_keywords(f"{title} {desc}")
            if not kws: continue
            company = extract_company(title)
            kw, cat = kws[0]
            events.append(make_event(
                company=company, kw=kw, cat=cat,
                source=label, url=link,
                headline=title, snippet=desc[:400],
                pub=pub[:50] if pub else None,
                meta={"gnews_query": query},
            ))
            count += 1
        logger.info(f"  [{query[:50]}]: {len(items)} items → {count} signals")
        time.sleep(0.6)
    logger.info(f"  Google News total: {len(events)} events")
    return events

# ═══════════════════════════════════════════════
# SOURCE 5: FINANCIAL + LEGAL MEDIA RSS
# ═══════════════════════════════════════════════
MEDIA_FEEDS = [
    ("Economic Times",    "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms"),
    ("Economic Times",    "https://economictimes.indiatimes.com/industry/rssfeeds/13352306.cms"),
    ("Economic Times",    "https://economictimes.indiatimes.com/news/company/rssfeeds/2143429.cms"),
    ("Economic Times",    "https://economictimes.indiatimes.com/small-biz/rssfeeds/7771592.cms"),
    ("Business Standard", "https://www.business-standard.com/rss/finance-16.rss"),
    ("Business Standard", "https://www.business-standard.com/rss/companies-101.rss"),
    ("Mint",              "https://www.livemint.com/rss/companies"),
    ("Mint",              "https://www.livemint.com/rss/money"),
    ("Financial Express", "https://www.financialexpress.com/market/feed/"),
    ("Moneycontrol",      "https://www.moneycontrol.com/rss/latestnews.xml"),
    ("BusinessLine",      "https://www.thehindubusinessline.com/feeder/default.rss"),
    ("BusinessLine",      "https://www.thehindubusinessline.com/markets/feeder/default.rss"),
    ("NDTV Profit",       "https://feeds.feedburner.com/ndtvprofit-latest"),
    ("Zee Business",      "https://www.zeebiz.com/rss/india.xml"),
    ("SCC Online",        "https://www.scconline.com/blog/feed/"),
    ("Verdictum",         "https://verdictum.in/feed/"),
]

def crawl_media_rss(session):
    events = []
    logger.info(f"  ── Financial + Legal Media RSS ({len(MEDIA_FEEDS)} feeds)")
    for name, url in MEDIA_FEEDS:
        r = safe_get(session, url, timeout=15)
        if not r: continue
        items = parse_rss(r.text, url)
        ev = events_from_feed(items, name, url)
        events.extend(ev)
        logger.info(f"  {name}: {len(items)} items → {len(ev)} signals")
        time.sleep(0.5)
    logger.info(f"  Media RSS total: {len(events)} events")
    return events

# ═══════════════════════════════════════════════
# DEDUPLICATION
# ═══════════════════════════════════════════════
def deduplicate(events):
    seen, unique = set(), []
    for e in events:
        key = (
            e["company_name"].lower().strip(),
            e["signal_keyword"].lower(),
            e["source"].lower(),
        )
        if key not in seen:
            seen.add(key)
            unique.append(e)
    return unique

# ═══════════════════════════════════════════════
# DB WRITE LOOP — shared by both paths
# ═══════════════════════════════════════════════
def write_to_db(events, run_id, t0):
    events = deduplicate(events)
    logger.info(f"After dedup  : {len(events)}")

    try:
        from enrichment import enrich_batch
        events = enrich_batch(events)
        logger.info("Enrichment complete (in-memory)")
    except ImportError:
        pass

    inserted = skipped = failed = 0
    for e in events:
        if db_is_duplicate(e.get("company_name", ""), e.get("signal_keyword", ""), e.get("source", "")):
            skipped += 1
            continue
        db_upsert_company(e.get("company_name", ""))
        if db_insert(e):
            inserted += 1
        else:
            failed += 1

    try:
        from enrichment import run_enrichment_on_db
        run_enrichment_on_db(hours_back=1)
    except ImportError:
        pass

    duration = round(time.time() - t0, 1)
    logger.info(f"\n{'═'*55}")
    logger.info(f"COMPLETE — Run {run_id}")
    logger.info(f"Inserted : {inserted}")
    logger.info(f"Skipped  : {skipped} (already today)")
    logger.info(f"Failed   : {failed}")
    logger.info(f"Duration : {duration}s")
    logger.info(f"{'═'*55}")
    return inserted, skipped, failed

# ═══════════════════════════════════════════════
# INTELLIGENCE PIPELINE (distress group)
# ═══════════════════════════════════════════════
def run_intelligence_pipeline(dry_run=False):
    """
    Runs the 5-source intelligence pipeline.
    Called only when group == 'distress' or group == 'all'.
    """
    if not SUPABASE_URL or not SUPABASE_ANON_KEY:
        logger.critical("SUPABASE_URL and SUPABASE_ANON_KEY must be set!")
        sys.exit(1)

    run_id = str(uuid.uuid4())[:8]
    t0     = time.time()
    logger.info(f"{'═'*55}")
    logger.info(f"NEXUS ASIA DISTRESS RADAR — Run {run_id}")
    logger.info(f"Started: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    logger.info(f"{'═'*55}")

    session    = make_session()
    all_events = []

    logger.info("\n━━ [1/5] LiveLaw (primary NCLT/NCLAT tracker)")
    all_events.extend(crawl_livelaw(session))

    logger.info("\n━━ [2/5] Bar & Bench (NCLT/HC insolvency)")
    all_events.extend(crawl_bar_bench(session))

    logger.info("\n━━ [3/5] RBI (enforcement + wilful defaulters)")
    all_events.extend(crawl_rbi(session))

    logger.info("\n━━ [4/5] Google News RSS (targeted IBBI/NCLT searches)")
    all_events.extend(crawl_google_news(session))

    logger.info("\n━━ [5/5] Financial + Legal Media RSS")
    all_events.extend(crawl_media_rss(session))

    logger.info(f"\n{'─'*55}")
    logger.info(f"Raw events   : {len(all_events)}")

    if dry_run:
        all_events = deduplicate(all_events)
        logger.info(f"DRY RUN — {len(all_events)} events (not written)")
        for e in all_events[:10]:
            logger.info(f"  {e.get('signal_category','?')} | {e.get('company_name','?')} | {e.get('headline','')[:80]}")
        return

    write_to_db(all_events, run_id, t0)

# ═══════════════════════════════════════════════
# CRAWLER GROUP RUNNER (all non-distress groups)
# ═══════════════════════════════════════════════
def run_crawler_group(group_name, dry_run=False):
    """
    Runs a specific crawler group from crawlers/.
    Does NOT run the intelligence pipeline.
    """
    if not SUPABASE_URL or not SUPABASE_ANON_KEY:
        logger.critical("SUPABASE_URL and SUPABASE_ANON_KEY must be set!")
        sys.exit(1)

    from crawlers import (
        DISTRESS_CRAWLERS, BANK_AUCTION_CRAWLERS, LEGAL_CRAWLERS,
        CRE_CRAWLERS, ARC_CRAWLERS, MARKET_CRAWLERS, ALL_CRAWLERS,
    )
    GROUP_MAP = {
        "distress":     DISTRESS_CRAWLERS,
        "bank_auction": BANK_AUCTION_CRAWLERS,
        "legal":        LEGAL_CRAWLERS,
        "cre":          CRE_CRAWLERS,
        "arc":          ARC_CRAWLERS,
        "market":       MARKET_CRAWLERS,
        "all":          ALL_CRAWLERS,
    }
    group = GROUP_MAP.get(group_name, DISTRESS_CRAWLERS)

    run_id = str(uuid.uuid4())[:8]
    t0     = time.time()
    logger.info(f"{'═'*55}")
    logger.info(f"NEXUS ASIA | Group: {group_name.upper()} | {len(group)} crawlers")
    logger.info(f"{'═'*55}")

    all_ev = []
    for Cls in group:
        try:
            inst = Cls()
            ev   = inst.crawl()
            evl  = [e.to_dict() if hasattr(e, "to_dict") else e for e in ev]
            all_ev.extend(evl)
            logger.info(f"  {inst.SOURCE_NAME}: {len(ev)} events")
        except Exception as exc:
            logger.error(f"  {Cls.__name__} failed: {exc}")

    logger.info(f"\n{'─'*55}")
    logger.info(f"Raw events   : {len(all_ev)}")

    if dry_run:
        all_ev = deduplicate(all_ev)
        logger.info(f"DRY RUN — {len(all_ev)} events (not written)")
        for e in all_ev[:10]:
            logger.info(f"  {e.get('signal_category','?')} | {e.get('company_name','?')} | {e.get('headline','')[:80]}")
        return

    write_to_db(all_ev, run_id, t0)

# ═══════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Nexus Asia Distress Radar")
    parser.add_argument(
        "--group",
        default="distress",
        choices=["all", "distress", "bank_auction", "legal", "cre", "arc", "market"],
        help="Crawler group to run (default: distress)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        default=os.environ.get("DRY_RUN", "false").lower() == "true",
        help="Skip DB writes, print sample events"
    )
    parser.add_argument(
        "--source", default=None,
        help="Run a single crawler by name substring"
    )
    args = parser.parse_args()

    if args.source:
        # Single-crawler override: load group and filter by name
        from crawlers import ALL_CRAWLERS
        matched = [C for C in ALL_CRAWLERS if args.source.lower() in C.SOURCE_NAME.lower()]
        if not matched:
            logger.error(f"No crawler found matching '{args.source}'")
            sys.exit(1)
        run_id = str(uuid.uuid4())[:8]
        t0     = time.time()
        all_ev = []
        for Cls in matched:
            inst = Cls()
            ev   = inst.crawl()
            all_ev.extend(e.to_dict() if hasattr(e, "to_dict") else e for e in ev)
            logger.info(f"  {inst.SOURCE_NAME}: {len(ev)} events")
        logger.info(f"Raw events: {len(all_ev)}")
        if args.dry_run:
            logger.info("DRY RUN — not written")
        else:
            write_to_db(all_ev, run_id, t0)

    elif args.group == "distress":
        # Intelligence pipeline: LiveLaw + Bar&Bench + RBI + Google News + Media RSS
        run_intelligence_pipeline(dry_run=args.dry_run)

    elif args.group == "all":
        # Intelligence pipeline THEN all crawler groups
        run_intelligence_pipeline(dry_run=args.dry_run)
        run_crawler_group("all", dry_run=args.dry_run)

    else:
        # bank_auction / legal / cre / arc / market
        # Pure crawler group — NO intelligence pipeline overhead
        run_crawler_group(args.group, dry_run=args.dry_run)
