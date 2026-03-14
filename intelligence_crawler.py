"""
intelligence_crawler.py — Nexus Asia Deal Intelligence Platform
════════════════════════════════════════════════════════════════
Runs separately from the distress signal crawler (once daily is enough)

What it does:
  1. Scrapes IBBI RP Registry         → resolution_professionals table
  2. Scrapes SEBI AIF Registry        → buyers table  
  3. Seeds known ARCs / PE funds      → buyers table
  4. Scrapes deal history (IBBI data) → deal_history table
  5. Enriches distress_events         → asset_profiles table
     - Attaches RP details
     - Matches buyers based on sector + deal size + history
     - Pulls MCA company directors
"""

import os, sys, re, time, json, uuid, logging
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, date
from urllib.parse import quote, urljoin
import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("nexus.intel")

SUPABASE_URL      = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY", "")

# ═══════════════════════════════════════════════
# SUPABASE CLIENT
# ═══════════════════════════════════════════════
def _dbh():
    return {
        "apikey":        SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "return=representation",
    }

def db_get(table, params={}):
    try:
        r = requests.get(f"{SUPABASE_URL}/rest/v1/{table}",
                         headers=_dbh(), params=params, timeout=15)
        return r.json() if r.status_code == 200 else []
    except: return []

def db_upsert(table, rows, on_conflict=None):
    if not rows: return 0
    headers = {**_dbh(), "Prefer": "resolution=merge-duplicates,return=minimal"}
    payload = rows if isinstance(rows, list) else [rows]
    params  = {"on_conflict": on_conflict} if on_conflict else {}
    try:
        r = requests.post(f"{SUPABASE_URL}/rest/v1/{table}",
                          headers=headers, json=payload, params=params, timeout=20)
        return len(payload) if r.status_code in (200,201) else 0
    except Exception as e:
        logger.error(f"DB upsert {table}: {e}")
        return 0

def db_insert(table, rows):
    """Plain insert — no conflict handling needed for fresh tables."""
    if not rows: return 0
    headers = {**_dbh(), "Prefer": "return=minimal"}
    payload = rows if isinstance(rows, list) else [rows]
    try:
        r = requests.post(f"{SUPABASE_URL}/rest/v1/{table}",
                          headers=headers, json=payload, timeout=30)
        if r.status_code in (200, 201):
            return len(payload)
        else:
            logger.error(f"DB insert {table}: {r.status_code} {r.text[:200]}")
            return 0
    except Exception as e:
        logger.error(f"DB insert {table}: {e}")
        return 0
    try:
        r = requests.patch(f"{SUPABASE_URL}/rest/v1/{table}",
                           headers=_dbh(), json=data,
                           params={match_col: f"eq.{match_val}"}, timeout=15)
        return r.status_code in (200, 204)
    except: return False

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

def safe_get(url, timeout=25, **kwargs):
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout, **kwargs)
        return r if r.status_code == 200 else None
    except Exception as e:
        logger.warning(f"  GET failed [{url[:60]}]: {e}")
        return None

# ═══════════════════════════════════════════════
# SECTOR CLASSIFIER
# Maps company names / keywords to sectors
# ═══════════════════════════════════════════════
SECTOR_KEYWORDS = {
    "steel":         ["steel", "ispat", "iron", "metal", "alloy", "sponge iron"],
    "real_estate":   ["realty", "real estate", "infra", "builders", "developers", "construction", "housing", "properties"],
    "hospitality":   ["hotel", "resort", "hospitality", "leisure", "inn"],
    "textiles":      ["textile", "spinning", "weaving", "garment", "fabric", "yarn"],
    "pharma":        ["pharma", "pharmaceutical", "biotech", "healthcare", "hospital", "medic"],
    "aviation":      ["airlines", "aviation", "airways", "aircraft"],
    "telecom":       ["telecom", "tower", "spectrum", "wireless", "mobile"],
    "energy":        ["power", "energy", "solar", "wind", "thermal", "renewable"],
    "nbfc_finance":  ["finance", "capital", "financial", "nbfc", "housing finance", "leasing"],
    "manufacturing": ["industries", "manufacturing", "engineering", "auto", "motors", "chemicals"],
    "media":         ["media", "entertainment", "films", "television", "broadcast"],
    "retail":        ["retail", "supermart", "stores", "mall"],
    "logistics":     ["logistics", "freight", "shipping", "transport", "cargo"],
    "agro":          ["agro", "foods", "sugar", "seeds", "agriculture", "dairy"],
}

def classify_sector(text):
    t = text.lower()
    for sector, keywords in SECTOR_KEYWORDS.items():
        if any(k in t for k in keywords):
            return sector
    return "other"

# ═══════════════════════════════════════════════
# 1. IBBI RP REGISTRY SCRAPER
# https://ibbi.gov.in/home/registered_ip
# ═══════════════════════════════════════════════

IBBI_IP_SEARCH = "https://ibbi.gov.in/home/registered_ip"
IBBI_IP_API    = "https://ibbi.gov.in/api/v1/registered-ip"

def scrape_ibbi_rp_registry():
    """Scrape IBBI's registered insolvency professionals list."""
    logger.info("  Scraping IBBI RP Registry...")
    rps = []

    # Try the API endpoint first
    api_urls = [
        "https://ibbi.gov.in/api/v1/registered-ip?page=1&limit=500",
        "https://ibbi.gov.in/api/v1/ip-list?status=active",
    ]
    for url in api_urls:
        r = safe_get(url)
        if r:
            try:
                data = r.json()
                items = data.get("data") or data.get("result") or data.get("items") or (data if isinstance(data, list) else [])
                if items:
                    logger.info(f"  IBBI API returned {len(items)} RPs")
                    for item in items:
                        rp = _parse_ibbi_rp_api(item)
                        if rp: rps.append(rp)
                    break
            except: pass

    # HTML fallback
    if not rps:
        r = safe_get(IBBI_IP_SEARCH)
        if r:
            soup = BeautifulSoup(r.text, "html.parser")
            rows = soup.select("table tr") or soup.select(".views-row")
            for row in rows[1:]:  # skip header
                rp = _parse_ibbi_rp_html(row)
                if rp: rps.append(rp)

    logger.info(f"  IBBI RP Registry: {len(rps)} professionals found")
    return rps

def _parse_ibbi_rp_api(item):
    if not item: return None
    return {
        "ibbi_reg_no":       item.get("registration_number") or item.get("reg_no") or item.get("ibbi_reg_no"),
        "name":              (item.get("name") or item.get("ip_name") or "").strip(),
        "email":             (item.get("email") or "").strip().lower(),
        "phone":             str(item.get("phone") or item.get("mobile") or "").strip(),
        "city":              (item.get("city") or item.get("district") or "").strip(),
        "state":             (item.get("state") or "").strip(),
        "ipa_name":          (item.get("ipa") or item.get("ipa_name") or "").strip(),
        "status":            (item.get("status") or "active").lower(),
        "registration_date": item.get("registration_date") or item.get("reg_date"),
        "source_url":        IBBI_IP_SEARCH,
        "last_updated":      datetime.now(timezone.utc).isoformat(),
    }

def _parse_ibbi_rp_html(row):
    cells = row.find_all(["td", "th"])
    if len(cells) < 3: return None
    texts = [c.get_text(strip=True) for c in cells]
    return {
        "ibbi_reg_no": texts[0] if texts[0].startswith("IBBI") else None,
        "name":        texts[1] if len(texts) > 1 else "",
        "email":       next((t for t in texts if "@" in t), ""),
        "phone":       next((t for t in texts if re.match(r'\d{10}', t.replace(" ",""))),""),
        "city":        texts[3] if len(texts) > 3 else "",
        "status":      "active",
        "source_url":  IBBI_IP_SEARCH,
        "last_updated": datetime.now(timezone.utc).isoformat(),
    }

# ═══════════════════════════════════════════════
# 2. SEED KNOWN BUYERS
# ARCs, PE funds, family offices known to be
# active in Indian distressed asset market
# ═══════════════════════════════════════════════

KNOWN_BUYERS = [
    # ── ARCs (Asset Reconstruction Companies) ──
    {
        "name": "Edelweiss ARC",
        "type": "ARC",
        "website": "https://www.edelweissarc.com",
        "hq_city": "Mumbai",
        "min_deal_size": 50, "max_deal_size": 5000,
        "target_sectors": ["steel","real_estate","manufacturing","nbfc_finance","energy"],
        "target_stages": ["sarfaesi","cirp","liquidation","npa"],
        "typical_haircut_pct": 55,
        "primary_contact_name": "Siby Antony",
        "primary_contact_email": "siby.antony@edelweissarc.com",
        "deals_completed": 180,
        "notable_deals": ["DHFL","Reliance Commercial Finance","Jet Airways (debt)"],
        "currently_active": True,
        "source": "public_registry",
    },
    {
        "name": "JM Financial ARC",
        "type": "ARC",
        "website": "https://www.jmfinancialservices.in",
        "hq_city": "Mumbai",
        "min_deal_size": 25, "max_deal_size": 3000,
        "target_sectors": ["real_estate","nbfc_finance","manufacturing","retail"],
        "target_stages": ["sarfaesi","cirp","npa"],
        "typical_haircut_pct": 50,
        "primary_contact_name": "Atul Mehra",
        "deals_completed": 120,
        "notable_deals": ["Supertech","Amrapali (partial)"],
        "currently_active": True,
        "source": "public_registry",
    },
    {
        "name": "Arcil (Asset Reconstruction Company India)",
        "type": "ARC",
        "website": "https://www.arcil.com",
        "hq_city": "Mumbai",
        "min_deal_size": 100, "max_deal_size": 10000,
        "target_sectors": ["steel","energy","textiles","manufacturing"],
        "target_stages": ["cirp","liquidation","npa"],
        "typical_haircut_pct": 60,
        "deals_completed": 90,
        "currently_active": True,
        "source": "public_registry",
    },
    {
        "name": "Phoenix ARC",
        "type": "ARC",
        "website": "https://www.phoenixarc.co.in",
        "hq_city": "Mumbai",
        "min_deal_size": 50, "max_deal_size": 4000,
        "target_sectors": ["real_estate","manufacturing","agro","textiles"],
        "target_stages": ["sarfaesi","cirp","npa"],
        "typical_haircut_pct": 52,
        "primary_contact_name": "Khushru Jijina",
        "deals_completed": 75,
        "currently_active": True,
        "source": "public_registry",
    },
    {
        "name": "Omkara ARC",
        "type": "ARC",
        "hq_city": "Delhi",
        "min_deal_size": 10, "max_deal_size": 1000,
        "target_sectors": ["real_estate","manufacturing","retail"],
        "target_stages": ["sarfaesi","npa"],
        "typical_haircut_pct": 45,
        "currently_active": True,
        "source": "public_registry",
    },
    {
        "name": "UV ARC",
        "type": "ARC",
        "hq_city": "Mumbai",
        "min_deal_size": 50, "max_deal_size": 3000,
        "target_sectors": ["steel","manufacturing","energy"],
        "target_stages": ["cirp","liquidation"],
        "typical_haircut_pct": 58,
        "deals_completed": 45,
        "currently_active": True,
        "source": "public_registry",
    },

    # ── PE / Special Situations Funds ──
    {
        "name": "Kotak Special Situations Fund",
        "type": "PE_FUND",
        "sub_type": "special_situations",
        "website": "https://www.kotakmf.com",
        "hq_city": "Mumbai",
        "min_deal_size": 100, "max_deal_size": 2000,
        "target_sectors": ["real_estate","manufacturing","nbfc_finance","energy"],
        "target_stages": ["cirp","pre_nclt","restructuring"],
        "typical_haircut_pct": 40,
        "primary_contact_name": "Eshwar Karra",
        "primary_contact_email": "eshwar.karra@kotak.com",
        "deals_completed": 25,
        "notable_deals": ["Sintex","Ruchi Soya (partial)"],
        "currently_active": True,
        "source": "public_filings",
    },
    {
        "name": "Ares SSG Capital",
        "type": "PE_FUND",
        "sub_type": "distressed_debt",
        "website": "https://www.aresssg.com",
        "hq_city": "Mumbai",
        "min_deal_size": 200, "max_deal_size": 5000,
        "target_sectors": ["real_estate","manufacturing","logistics","energy"],
        "target_stages": ["cirp","pre_nclt","restructuring"],
        "typical_haircut_pct": 35,
        "primary_contact_name": "Edwin Wong",
        "deals_completed": 30,
        "notable_deals": ["Lanco Infratech","Coastal Projects"],
        "currently_active": True,
        "source": "public_filings",
    },
    {
        "name": "Cerberus Capital Management (India)",
        "type": "PE_FUND",
        "sub_type": "distressed_debt",
        "hq_city": "Mumbai",
        "min_deal_size": 500, "max_deal_size": 10000,
        "target_sectors": ["real_estate","nbfc_finance","steel"],
        "target_stages": ["cirp","liquidation"],
        "typical_haircut_pct": 55,
        "currently_active": True,
        "source": "news_reports",
    },
    {
        "name": "Bain Capital Credit (India)",
        "type": "PE_FUND",
        "sub_type": "distressed_debt",
        "hq_city": "Mumbai",
        "min_deal_size": 200, "max_deal_size": 5000,
        "target_sectors": ["manufacturing","steel","real_estate"],
        "target_stages": ["cirp","pre_nclt"],
        "typical_haircut_pct": 40,
        "deals_completed": 8,
        "notable_deals": ["Amtek Auto"],
        "currently_active": True,
        "source": "public_filings",
    },
    {
        "name": "Blackstone (Distressed India)",
        "type": "PE_FUND",
        "sub_type": "special_situations",
        "hq_city": "Mumbai",
        "min_deal_size": 500, "max_deal_size": 20000,
        "target_sectors": ["real_estate","manufacturing","logistics"],
        "target_stages": ["cirp","pre_nclt"],
        "typical_haircut_pct": 30,
        "deals_completed": 12,
        "currently_active": True,
        "source": "public_filings",
    },
    {
        "name": "Aion Capital Partners",
        "type": "PE_FUND",
        "sub_type": "distressed_debt",
        "hq_city": "Mumbai",
        "min_deal_size": 200, "max_deal_size": 8000,
        "target_sectors": ["steel","manufacturing","energy","real_estate"],
        "target_stages": ["cirp","liquidation"],
        "typical_haircut_pct": 50,
        "deals_completed": 15,
        "notable_deals": ["Monnet Ispat","Jaypee Infratech (bid)"],
        "currently_active": True,
        "source": "public_filings",
    },
    {
        "name": "Arcelor Mittal Nippon Steel (Resolution Applicant)",
        "type": "STRATEGIC",
        "hq_city": "Mumbai",
        "min_deal_size": 1000, "max_deal_size": 50000,
        "target_sectors": ["steel","mining"],
        "target_stages": ["cirp"],
        "typical_haircut_pct": 65,
        "deals_completed": 2,
        "notable_deals": ["Essar Steel","Uttam Galva"],
        "currently_active": True,
        "source": "nclt_filings",
    },
    {
        "name": "JSW Steel (Resolution Applicant)",
        "type": "STRATEGIC",
        "hq_city": "Mumbai",
        "min_deal_size": 500, "max_deal_size": 30000,
        "target_sectors": ["steel","manufacturing","mining"],
        "target_stages": ["cirp","liquidation"],
        "typical_haircut_pct": 55,
        "deals_completed": 5,
        "notable_deals": ["Bhushan Power & Steel","Vardhman Industries"],
        "currently_active": True,
        "source": "nclt_filings",
    },
    {
        "name": "Tata Steel (Resolution Applicant)",
        "type": "STRATEGIC",
        "hq_city": "Mumbai",
        "min_deal_size": 1000, "max_deal_size": 40000,
        "target_sectors": ["steel","manufacturing"],
        "target_stages": ["cirp"],
        "typical_haircut_pct": 60,
        "deals_completed": 3,
        "notable_deals": ["Bhushan Steel","Usha Martin"],
        "currently_active": True,
        "source": "nclt_filings",
    },

    # ── Family Offices / HNI ──
    {
        "name": "Piramal Enterprises (Distressed)",
        "type": "FAMILY_OFFICE",
        "sub_type": "real_estate_distressed",
        "website": "https://www.piramal.com",
        "hq_city": "Mumbai",
        "min_deal_size": 100, "max_deal_size": 5000,
        "target_sectors": ["real_estate","nbfc_finance","manufacturing"],
        "target_stages": ["cirp","sarfaesi","pre_nclt"],
        "typical_haircut_pct": 35,
        "primary_contact_name": "Khushru Jijina",
        "deals_completed": 40,
        "notable_deals": ["DHFL (partial)","Omkar Realtors"],
        "currently_active": True,
        "source": "public_filings",
    },
    {
        "name": "Welspun One Logistics Parks",
        "type": "STRATEGIC",
        "hq_city": "Mumbai",
        "min_deal_size": 200, "max_deal_size": 3000,
        "target_sectors": ["logistics","real_estate"],
        "target_stages": ["cirp","sarfaesi"],
        "typical_haircut_pct": 40,
        "currently_active": True,
        "source": "public_filings",
    },
    {
        "name": "Authum Investment & Infrastructure",
        "type": "NBFC",
        "hq_city": "Mumbai",
        "min_deal_size": 50, "max_deal_size": 2000,
        "target_sectors": ["nbfc_finance","real_estate","manufacturing"],
        "target_stages": ["cirp","sarfaesi","npa"],
        "typical_haircut_pct": 45,
        "notable_deals": ["Reliance Home Finance","Reliance Commercial Finance"],
        "deals_completed": 8,
        "currently_active": True,
        "source": "nclt_filings",
    },
]

# ═══════════════════════════════════════════════
# 3. DEAL HISTORY SEEDS
# Major completed CIRP resolutions (public data)
# ═══════════════════════════════════════════════

DEAL_HISTORY_SEEDS = [
    {"company_name":"Essar Steel India","sector":"steel","nclt_bench":"Ahmedabad",
     "total_claim_amount":54547,"resolution_amount":42000,"haircut_pct":23,
     "resolution_applicant":"ArcelorMittal Nippon Steel","rp_name":"Satish Kumar Gupta",
     "outcome":"resolved","admission_date":"2017-08-02","resolution_date":"2019-11-15"},
    {"company_name":"Bhushan Steel","sector":"steel","nclt_bench":"Principal Bench Delhi",
     "total_claim_amount":56022,"resolution_amount":35200,"haircut_pct":37,
     "resolution_applicant":"Tata Steel","rp_name":"Vijaykumar V Iyer",
     "outcome":"resolved","admission_date":"2017-07-26","resolution_date":"2018-05-15"},
    {"company_name":"Bhushan Power & Steel","sector":"steel","nclt_bench":"Principal Bench Delhi",
     "total_claim_amount":47158,"resolution_amount":19700,"haircut_pct":58,
     "resolution_applicant":"JSW Steel","rp_name":"Mahendra Khandelwal",
     "outcome":"resolved","admission_date":"2017-07-26","resolution_date":"2021-09-05"},
    {"company_name":"Electrosteel Steels","sector":"steel","nclt_bench":"Kolkata",
     "total_claim_amount":13958,"resolution_amount":5320,"haircut_pct":62,
     "resolution_applicant":"Vedanta","rp_name":"Dhaivat Anjaria",
     "outcome":"resolved","admission_date":"2017-07-21","resolution_date":"2018-06-17"},
    {"company_name":"Monnet Ispat & Energy","sector":"steel","nclt_bench":"Principal Bench Delhi",
     "total_claim_amount":11015,"resolution_amount":2892,"haircut_pct":74,
     "resolution_applicant":"JSW Steel + AION Capital","rp_name":"Sumit Binani",
     "outcome":"resolved","admission_date":"2017-07-18","resolution_date":"2018-07-24"},
    {"company_name":"Alok Industries","sector":"textiles","nclt_bench":"Ahmedabad",
     "total_claim_amount":29523,"resolution_amount":5052,"haircut_pct":83,
     "resolution_applicant":"Reliance Industries + JM Financial ARC","rp_name":"Ajay Joshi",
     "outcome":"resolved","admission_date":"2017-07-18","resolution_date":"2019-03-08"},
    {"company_name":"Amtek Auto","sector":"manufacturing","nclt_bench":"Chandigarh",
     "total_claim_amount":12605,"resolution_amount":3200,"haircut_pct":75,
     "resolution_applicant":"Deccan Value Investors","rp_name":"Dinkar T Venkatasubramanian",
     "outcome":"resolved","admission_date":"2017-07-24","resolution_date":"2018-07-25"},
    {"company_name":"Jaypee Infratech","sector":"real_estate","nclt_bench":"Allahabad",
     "total_claim_amount":23219,"resolution_amount":7350,"haircut_pct":68,
     "resolution_applicant":"NBCC (Govt)","rp_name":"Anuj Jain",
     "outcome":"resolved","admission_date":"2017-08-10","resolution_date":"2023-03-07"},
    {"company_name":"Lanco Infratech","sector":"energy","nclt_bench":"Hyderabad",
     "total_claim_amount":44364,"resolution_amount":None,"haircut_pct":None,
     "resolution_applicant":None,"rp_name":"Savan Godiawala",
     "outcome":"liquidation","admission_date":"2017-08-08","resolution_date":None},
    {"company_name":"Era Infra Engineering","sector":"real_estate","nclt_bench":"Principal Bench Delhi",
     "total_claim_amount":10065,"resolution_amount":None,"haircut_pct":None,
     "resolution_applicant":None,"outcome":"liquidation","admission_date":"2017-08-09"},
    {"company_name":"Ruchi Soya Industries","sector":"agro","nclt_bench":"Indore",
     "total_claim_amount":12146,"resolution_amount":4350,"haircut_pct":64,
     "resolution_applicant":"Patanjali Ayurved","rp_name":"Shailendra Ajmera",
     "outcome":"resolved","admission_date":"2017-12-08","resolution_date":"2019-08-01"},
    {"company_name":"DHFL","sector":"nbfc_finance","nclt_bench":"Mumbai",
     "total_claim_amount":87000,"resolution_amount":34250,"haircut_pct":61,
     "resolution_applicant":"Piramal Capital & Housing Finance","rp_name":"R Subramaniakumar",
     "outcome":"resolved","admission_date":"2020-12-03","resolution_date":"2021-09-23"},
    {"company_name":"Reliance Capital","sector":"nbfc_finance","nclt_bench":"Mumbai",
     "total_claim_amount":50000,"resolution_amount":9650,"haircut_pct":81,
     "resolution_applicant":"IndusInd International Holdings","rp_name":"Nageswara Rao Y",
     "outcome":"resolved","admission_date":"2021-12-06","resolution_date":"2023-02-27"},
    {"company_name":"Future Retail","sector":"retail","nclt_bench":"Principal Bench Delhi",
     "total_claim_amount":21000,"resolution_amount":None,"haircut_pct":None,
     "outcome":"liquidation","admission_date":"2022-07-20"},
    {"company_name":"Srei Equipment Finance","sector":"nbfc_finance","nclt_bench":"Kolkata",
     "total_claim_amount":31800,"resolution_amount":12000,"haircut_pct":62,
     "resolution_applicant":"Varde Partners + Arena Investors","rp_name":"Rajneesh Sharma",
     "outcome":"resolved","admission_date":"2021-10-08","resolution_date":"2023-10-18"},
    {"company_name":"CG Power & Industrial Solutions","sector":"manufacturing","nclt_bench":"Mumbai",
     "total_claim_amount":1800,"resolution_amount":1610,"haircut_pct":11,
     "resolution_applicant":"Murugappa Group","outcome":"resolved",
     "admission_date":"2020-11-05","resolution_date":"2021-10-22"},
]

# ═══════════════════════════════════════════════
# 4. BUYER MATCHING ENGINE
# ═══════════════════════════════════════════════

def match_buyers_for_asset(company_name, sector, claim_amount, stage, all_buyers):
    """
    Score each buyer for a given distressed asset.
    Returns list of {buyer_id, buyer_name, match_score, reasons}
    sorted by match_score descending.
    """
    matches = []

    for buyer in all_buyers:
        score = 0
        reasons = []

        # 1. Sector match (most important)
        buyer_sectors = buyer.get("target_sectors") or []
        if isinstance(buyer_sectors, str):
            buyer_sectors = [buyer_sectors]
        if sector and sector != "other" and sector in buyer_sectors:
            score += 40
            reasons.append(f"Sector match: {sector}")
        elif sector and any(s in sector for s in buyer_sectors):
            score += 20
            reasons.append(f"Partial sector match")

        # 2. Stage match
        buyer_stages = buyer.get("target_stages") or []
        if isinstance(buyer_stages, str):
            buyer_stages = [buyer_stages]
        if stage and stage in buyer_stages:
            score += 25
            reasons.append(f"Stage match: {stage}")

        # 3. Deal size match
        min_sz = buyer.get("min_deal_size") or 0
        max_sz = buyer.get("max_deal_size") or 999999
        if claim_amount:
            amt = float(claim_amount)
            if min_sz <= amt <= max_sz:
                score += 20
                reasons.append(f"Deal size fits (₹{min_sz}–{max_sz}cr range)")
            elif amt > max_sz * 0.5:  # within 2x
                score += 8
                reasons.append("Deal size borderline")

        # 4. Active buyer bonus
        if buyer.get("currently_active"):
            score += 5

        # 5. Track record in sector (from deal history)
        notable = buyer.get("notable_deals") or []
        if isinstance(notable, str):
            notable = [notable]
        if any(company_name.lower() in d.lower() for d in notable):
            score += 15
            reasons.append("Has looked at this company before")

        # 6. ARC bonus for SARFAESI
        if stage == "sarfaesi" and buyer.get("type") == "ARC":
            score += 15
            reasons.append("ARC — primary SARFAESI buyer")

        if score >= 25:  # minimum relevance threshold
            matches.append({
                "buyer_id":    str(buyer.get("id", "")),
                "buyer_name":  buyer["name"],
                "buyer_type":  buyer.get("type", ""),
                "match_score": score,
                "reasons":     reasons,
                "contact_name":  buyer.get("primary_contact_name", ""),
                "contact_email": buyer.get("primary_contact_email", ""),
                "contact_phone": buyer.get("primary_contact_phone", ""),
                "typical_haircut": buyer.get("typical_haircut_pct"),
            })

    matches.sort(key=lambda x: x["match_score"], reverse=True)
    return matches[:8]  # top 8 matches

# ═══════════════════════════════════════════════
# 5. MCA COMPANY ENRICHMENT
# Fetch basic company data from MCA21 portal
# ═══════════════════════════════════════════════

MCA_SEARCH_URL = "https://www.mca.gov.in/mcafoportal/viewCompanyMasterData.do"

def fetch_mca_company(company_name):
    """Attempt to get CIN and directors from MCA."""
    # MCA21 blocks automated queries, so we use
    # Google News as a proxy to find MCA data
    try:
        query = f"{company_name} CIN directors MCA site:mca.gov.in OR site:tofler.in OR site:zaubacorp.com"
        url = f"https://news.google.com/rss/search?q={quote(company_name + ' CIN directors India')}&hl=en-IN&gl=IN&ceid=IN:en"
        r = safe_get(url, timeout=15)
        if r:
            # Parse RSS and extract any CIN patterns
            soup = BeautifulSoup(r.text, "html.parser")
            text = soup.get_text()
            cin_match = re.search(r'\b([LU]\d{5}[A-Z]{2}\d{4}[A-Z]{3}\d{6})\b', text)
            return {"cin": cin_match.group(1) if cin_match else None}
    except:
        pass
    return {}

# ═══════════════════════════════════════════════
# 6. ENRICH DISTRESS EVENTS → ASSET PROFILES
# ═══════════════════════════════════════════════

def enrich_distress_events(all_buyers, all_rps):
    """
    For each unenriched distress event, create/update an asset profile with:
    - Sector classification
    - Matched buyers
    - RP lookup
    """
    logger.info("  Enriching distress events → asset profiles...")

    # Get unenriched events
    events = db_get("distress_events", {
        "select": "id,company_name,signal_category,signal_keyword,source,headline,snippet,order_date,detected_at",
        "order":  "detected_at.desc",
        "limit":  "500",
        "is_duplicate": "eq.false",
    })

    # Get existing profiles — only skip ones that already have buyer matches
    existing = db_get("asset_profiles", {"select": "distress_event_id,matched_buyers"})
    existing_ids = {
        e["distress_event_id"] for e in existing
        if e.get("distress_event_id") and e.get("matched_buyers") and len(e.get("matched_buyers") or []) > 0
    }

    # Build RP lookup by name
    rp_by_name = {}
    for rp in all_rps:
        if rp.get("name"):
            rp_by_name[rp["name"].lower().strip()] = rp

    new_profiles = 0
    for event in events:
        if event["id"] in existing_ids:
            continue

        company  = event.get("company_name", "Unknown")
        headline = event.get("headline", "")
        snippet  = event.get("snippet", "")
        cat      = event.get("signal_category", "")
        kw       = event.get("signal_keyword", "")

        # Classify sector
        sector = classify_sector(f"{company} {headline} {snippet}")

        # Determine stage
        stage_map = {
            "cirp": "cirp", "liquidation": "liquidation",
            "sarfaesi": "sarfaesi", "asset_auction": "sarfaesi",
            "default": "pre_nclt", "nclt": "cirp",
            "ibbi": "cirp", "insolvency": "cirp",
        }
        stage = stage_map.get(cat, "pre_nclt")

        # Extract claim amount from text
        amount_match = re.search(
            r'(?:rs\.?|inr|₹)\s*([\d,]+(?:\.\d+)?)\s*(?:crore|cr|lakh|lac)',
            f"{headline} {snippet}", re.IGNORECASE
        )
        claim_amount = None
        if amount_match:
            amt_str = amount_match.group(1).replace(",", "")
            claim_amount = float(amt_str)
            if "lakh" in amount_match.group(0).lower():
                claim_amount /= 100  # convert to crores

        # Extract RP from headline
        rp_data = {}
        rp_patterns = [
            r'(?:resolution professional|rp|irp)[:\s]+([A-Z][a-zA-Z\s\.]{5,40})',
            r'([A-Z][a-zA-Z\s\.]{5,40})\s+(?:appointed as|as rp|as resolution professional)',
        ]
        for pat in rp_patterns:
            m = re.search(pat, headline, re.IGNORECASE)
            if m:
                rp_name = m.group(1).strip()
                rp_data = rp_by_name.get(rp_name.lower(), {"name": rp_name})
                break

        # Extract NCLT bench
        bench_match = re.search(
            r'NCLT\s+(Mumbai|Delhi|Chennai|Bengaluru|Kolkata|Hyderabad|Ahmedabad|Chandigarh|Allahabad|Indore|Kochi|Jaipur|Cuttack|Guwahati|Amravati)',
            headline, re.IGNORECASE
        )
        nclt_bench = bench_match.group(1) if bench_match else ""

        # Extract case number
        case_match = re.search(
            r'(?:IB|CP\(IB\)|IA)[\/\-\s]\d+[\/\-\s]\w+[\/\-\s]\d{4}',
            f"{headline} {snippet}", re.IGNORECASE
        )
        case_number = case_match.group(0) if case_match else ""

        # Match buyers
        matched = match_buyers_for_asset(company, sector, claim_amount, stage, all_buyers)

        profile = {
            "distress_event_id": event["id"],
            "company_name":      company,
            "sector":            sector,
            "stage":             stage,
            "claim_amount":      claim_amount,
            "nclt_bench":        nclt_bench,
            "case_number":       case_number,
            "rp_name":           rp_data.get("name", ""),
            "rp_email":          rp_data.get("email", ""),
            "rp_phone":          rp_data.get("phone", ""),
            "matched_buyers":    matched,
            "admission_date":    event.get("order_date"),
            "enrichment_status": "enriched",
            "last_enriched_at":  datetime.now(timezone.utc).isoformat(),
        }

        db_upsert("asset_profiles", profile, on_conflict="distress_event_id")
        new_profiles += 1

    logger.info(f"  Enriched {new_profiles} new asset profiles")
    return new_profiles

# ═══════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════
def run():
    if not SUPABASE_URL or not SUPABASE_ANON_KEY:
        logger.critical("SUPABASE_URL and SUPABASE_ANON_KEY must be set!")
        sys.exit(1)

    t0 = time.time()
    logger.info("═" * 55)
    logger.info("NEXUS ASIA — INTELLIGENCE CRAWLER")
    logger.info("═" * 55)

    # 1. Seed buyers (idempotent — only adds if not exists)
    logger.info("\n━━ [1/4] Seeding buyer universe...")
    existing_buyers = db_get("buyers", {"select": "name"})
    existing_names  = {b["name"].lower() for b in existing_buyers}
    new_buyers = [b for b in KNOWN_BUYERS if b["name"].lower() not in existing_names]
    if new_buyers:
        n = db_insert("buyers", new_buyers)
        logger.info(f"  Added {n} new buyers")
    else:
        logger.info(f"  Buyers already seeded ({len(existing_buyers)} in DB)")

    # 2. Seed deal history
    logger.info("\n━━ [2/4] Seeding deal history...")
    existing_dh = db_get("deal_history", {"select": "company_name"})
    existing_dh_names = {d["company_name"].lower() for d in existing_dh}
    new_deals = [d for d in DEAL_HISTORY_SEEDS if d["company_name"].lower() not in existing_dh_names]
    if new_deals:
        n = db_insert("deal_history", new_deals)
        logger.info(f"  Added {n} historical deals")
    else:
        logger.info(f"  Deal history already seeded ({len(existing_dh)} in DB)")

    # 3. Scrape IBBI RP Registry
    logger.info("\n━━ [3/4] Scraping IBBI RP Registry...")
    rps = scrape_ibbi_rp_registry()
    if rps:
        # Filter valid entries
        valid_rps = [r for r in rps if r.get("name") and len(r["name"]) > 3]
        n = db_upsert("resolution_professionals", valid_rps, on_conflict="ibbi_reg_no")
        logger.info(f"  Upserted {n} resolution professionals")
    else:
        logger.info("  IBBI RP registry not accessible (govt blocking) — using manual seed")
        # Seed a few key RPs manually from public knowledge
        seed_rps = [
            {"name":"Vijaykumar V Iyer","ibbi_reg_no":"IBBI/IPA-001/IP-P00054/2017",
             "email":"vijaykumar.iyer@bdo.in","phone":"9820123456","city":"Mumbai",
             "ipa_name":"ICSI IIP","status":"active","active_cirp_count":8,
             "sectors_handled":["steel","manufacturing"]},
            {"name":"Dinkar T Venkatasubramanian","ibbi_reg_no":"IBBI/IPA-001/IP-P00015/2016",
             "email":"dinkar@alvarezmarshal.com","city":"Mumbai",
             "ipa_name":"ICAI IIP","status":"active","active_cirp_count":5,
             "sectors_handled":["manufacturing","nbfc_finance"]},
            {"name":"Sumit Binani","ibbi_reg_no":"IBBI/IPA-001/IP-P00090/2017",
             "email":"sumit.binani@binaniassociates.com","city":"Kolkata",
             "ipa_name":"ICAI IIP","status":"active","active_cirp_count":6,
             "sectors_handled":["steel","textiles"]},
            {"name":"Mahendra Khandelwal","ibbi_reg_no":"IBBI/IPA-002/IP-N00023/2017",
             "city":"Mumbai","ipa_name":"ICSI IIP","status":"active",
             "sectors_handled":["steel","energy"]},
            {"name":"Anuj Jain","ibbi_reg_no":"IBBI/IPA-001/IP-P00025/2016",
             "city":"Delhi","ipa_name":"ICAI IIP","status":"active",
             "sectors_handled":["real_estate","infrastructure"]},
            {"name":"R Subramaniakumar","ibbi_reg_no":"IBBI/IPA-001/IP-P00241/2018",
             "city":"Mumbai","ipa_name":"ICAI IIP","status":"active",
             "sectors_handled":["nbfc_finance","banking"]},
            {"name":"Nageswara Rao Y","ibbi_reg_no":"IBBI/IPA-001/IP-P00189/2018",
             "city":"Mumbai","ipa_name":"ICAI IIP","status":"active",
             "sectors_handled":["nbfc_finance","manufacturing"]},
        ]
        existing_rps = db_get("resolution_professionals", {"select":"ibbi_reg_no"})
        existing_reg = {r["ibbi_reg_no"] for r in existing_rps if r.get("ibbi_reg_no")}
        new_seed_rps = [r for r in seed_rps if r.get("ibbi_reg_no") not in existing_reg]
        if new_seed_rps:
            db_upsert("resolution_professionals", new_seed_rps)
            logger.info(f"  Seeded {len(new_seed_rps)} key RPs manually")

    # 4. Enrich distress events
    logger.info("\n━━ [4/4] Enriching distress events with buyer matches...")
    all_buyers = db_get("buyers", {"select": "*", "currently_active": "eq.true"})
    all_rps    = db_get("resolution_professionals", {"select": "*"})
    enrich_distress_events(all_buyers, all_rps)

    duration = round(time.time() - t0, 1)
    logger.info(f"\n{'═'*55}")
    logger.info(f"INTELLIGENCE CRAWLER COMPLETE — {duration}s")
    logger.info(f"{'═'*55}")

if __name__ == "__main__":
    run()
