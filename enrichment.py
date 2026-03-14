"""
enrichment.py — Nexus Asia Deal Enrichment Engine
═══════════════════════════════════════════════════════════════════════════
Runs after every crawl batch to enrich raw distress_events with:
  - deal_score (0–100): priority ranking for the deal team
  - channel: how this asset was sourced
  - is_mmr: Mumbai Metropolitan Region flag
  - asset_class: commercial / residential / land / industrial
  - price_crore: extracted from headline/snippet/metadata
  - location: best-guess micro-market
  - pre_leased_asset promotion: writes matched events to pre_leased_assets

Called automatically by crawler.py after every batch.
Can also be run standalone: python enrichment.py --backfill

Scoring model (meeting-derived weights):
  +40  commercial asset class
  +30  MMR location confirmed
  +20  price in 10–500 crore range (sweet spot)
  +15  bank auction / SARFAESI source (motivated seller)
  +15  DRT / legal pre-auction signal
  +15  ARC / NARCL motivated seller
  +10  blue-chip tenant (pre-leased)
  +10  Grade A asset class
  −10  residential (lower priority)
  −20  outside MMR (unless very large deal)
  +10  CRITICAL severity
  +5   HIGH severity
═══════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import os
import re
import sys
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("nexus.enrichment")

SUPABASE_URL      = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY", "")


# ─── Supabase helpers ────────────────────────────────────────────────────

def _h(write: bool = False) -> dict:
    key = SUPABASE_ANON_KEY
    return {
        "apikey": key, "Authorization": f"Bearer {key}",
        "Content-Type": "application/json", "Prefer": "return=minimal",
    }


def _get(table: str, params: dict) -> list:
    try:
        r = requests.get(f"{SUPABASE_URL}/rest/v1/{table}",
                         headers=_h(), params=params, timeout=15)
        return r.json() if r.status_code == 200 else []
    except Exception as e:
        logger.warning(f"GET {table}: {e}")
        return []


def _patch(table: str, data: dict, match: dict) -> bool:
    try:
        params = {k: f"eq.{v}" for k, v in match.items()}
        r = requests.patch(f"{SUPABASE_URL}/rest/v1/{table}",
                           headers=_h(write=True), params=params,
                           json=data, timeout=15)
        return r.status_code in (200, 204)
    except Exception as e:
        logger.warning(f"PATCH {table}: {e}")
        return False


def _post(table: str, data: list | dict) -> bool:
    payload = data if isinstance(data, list) else [data]
    try:
        r = requests.post(f"{SUPABASE_URL}/rest/v1/{table}",
                          headers={**_h(write=True), "Prefer": "resolution=merge-duplicates,return=minimal"},
                          json=payload, timeout=15)
        return r.status_code in (200, 201)
    except Exception as e:
        logger.warning(f"POST {table}: {e}")
        return False


# ─── Extraction helpers ──────────────────────────────────────────────────

PRICE_RE = re.compile(
    r'(?:rs\.?|₹|inr)?\s*([\d,]+(?:\.\d+)?)\s*(crore|cr\.?|lakh|lac|lakhs)',
    re.IGNORECASE
)

MMR_LOCATIONS = [
    'mumbai', 'thane', 'navi mumbai', 'kalyan', 'dombivli', 'panvel',
    'vasai', 'virar', 'mira road', 'bhiwandi', 'kurla', 'bandra',
    'andheri', 'malad', 'goregaon', 'borivali', 'kandivali', 'dahisar',
    'mulund', 'ghatkopar', 'chembur', 'wadala', 'worli', 'lower parel',
    'bkc', 'dharavi', 'sion', 'matunga', 'dadar', 'prabhadevi',
    'powai', 'vikhroli', 'kanjurmarg', 'bhandup', 'airoli', 'vashi',
    'belapur', 'kharghar', 'ulwe',
]

COMMERCIAL_KW = [
    'office', 'commercial', 'it park', 'it/ites', 'bpo', 'business park',
    'grade a', 'grade b', 'corporate', 'showroom', 'shop', 'retail',
    'warehouse', 'godown', 'factory', 'industrial', 'plot',
]

RESIDENTIAL_KW = [
    'flat', 'apartment', 'bungalow', 'villa', 'residential', 'row house',
    'tenement', '2bhk', '3bhk', 'bedroom',
]

LAND_KW = ['land', 'open plot', 'na plot', 'agricultural land', 'non-agricultural']

INDUSTRIAL_KW = ['factory', 'plant', 'manufacturing unit', 'sez', 'midc', 'industrial estate']

HOSPITALITY_KW = ['hotel', 'resort', 'service apartment', 'hospitality', 'inn']

# Channel classifiers by source name patterns
CHANNEL_MAP = {
    'bank_auction':       ['bank of baroda', 'pnb', 'canara bank', 'union bank',
                           'bank of maharashtra', 'central bank', 'iob', 'sbi', 'ibapi',
                           'bankauction', 'bob auction', 'sarfaesi notice'],
    'sarfaesi':           ['sarfaesi', 'ibapi', 'possession notice'],
    'drt':                ['drt portal', 'debt recovery tribunal'],
    'legal_intelligence': ['npa legal', 'bar & bench', 'livelaw', 'scc online',
                           'indiacorplaw', 'trilegal', 'khaitan', 'mondaq'],
    'pre_leased_cre':     ['pre-leased cre', 'grade a office vacancy', '99acres',
                           'magicbricks', 'squareyards', 'jll', 'colliers', 'anarock', 'cbre'],
    'arc_portfolio':      ['narcl', 'arc portfolio', 'arcil', 'edelweiss arc',
                           'phoenix arc', 'jm financial arc'],
    'pe_activity':        ['pe fund', 'vccircle', 'dealstreetasia', 'sebi aif'],
    'market_distress':    ['stock market distress', 'moneycontrol markets', 'et markets'],
    'regulatory':         ['ibbi', 'nclt', 'mca', 'rbi'],
    'media':              ['economic times', 'business standard', 'mint', 'reuters',
                           'financial express', 'businessline', 'ndtv profit'],
}


def extract_price(text: str) -> Optional[float]:
    m = PRICE_RE.search(text)
    if not m:
        return None
    try:
        val = float(m.group(1).replace(',', ''))
        unit = m.group(2).lower()
        return round(val if 'cr' in unit else val / 100, 2)
    except (ValueError, AttributeError):
        return None


def detect_mmr_location(text: str) -> Optional[str]:
    t = text.lower()
    for loc in MMR_LOCATIONS:
        if loc in t:
            return loc.title()
    return None


def detect_asset_class(text: str) -> str:
    t = text.lower()
    if any(k in t for k in HOSPITALITY_KW):    return 'hospitality'
    if any(k in t for k in INDUSTRIAL_KW):     return 'industrial'
    if any(k in t for k in COMMERCIAL_KW):     return 'commercial'
    if any(k in t for k in LAND_KW):           return 'land'
    if any(k in t for k in RESIDENTIAL_KW):    return 'residential'
    return 'other'


def detect_channel(source: str, category: str, metadata: dict) -> str:
    src_lower = source.lower()
    for channel, patterns in CHANNEL_MAP.items():
        if any(p in src_lower for p in patterns):
            return channel
    if category in ('sarfaesi', 'asset_auction'):  return 'bank_auction'
    if category in ('cirp', 'liquidation', 'nclt'): return 'regulatory'
    if category in ('creditor_action',):            return 'drt'
    return metadata.get('channel', 'other')


def score_event(event: dict) -> int:
    """Compute deal_score 0–100 for a distress event."""
    score  = 0
    meta   = event.get('metadata') or {}
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except (ValueError, TypeError):
            meta = {}

    full_text = f"{event.get('headline', '')} {event.get('snippet', '')}".lower()

    asset_class = event.get('asset_class') or detect_asset_class(full_text)
    is_mmr      = event.get('is_mmr') or (detect_mmr_location(full_text) is not None)
    price       = event.get('price_crore') or extract_price(full_text)
    channel     = event.get('channel', '')
    severity    = event.get('severity', 'medium')
    category    = event.get('signal_category', '')

    # Asset class weight
    if asset_class == 'commercial':   score += 40
    elif asset_class == 'industrial': score += 25
    elif asset_class == 'land':       score += 20
    elif asset_class == 'hospitality':score += 20
    elif asset_class == 'residential':score -= 10

    # Location
    if is_mmr: score += 30

    # Deal size sweet spot (10–500 crore)
    if price:
        if 10 <= price <= 500:    score += 20
        elif 5 <= price < 10:     score += 10
        elif price > 500:         score += 15

    # Source channel
    if channel in ('bank_auction', 'sarfaesi'):   score += 15
    if channel in ('drt', 'legal_intelligence'):  score += 15
    if channel == 'arc_portfolio':                score += 15

    # Severity
    if severity == 'critical': score += 10
    elif severity == 'high':   score += 5

    # Tenant quality from metadata
    tenant_score = meta.get('tenant_score', 0)
    if tenant_score >= 90:   score += 10
    elif tenant_score >= 70: score += 5

    # Cap rate flag
    if meta.get('meets_investor_threshold'):  score += 15

    # Motivated seller flag
    if meta.get('motivated_seller'):          score += 10

    # Pre-auction signal
    if meta.get('pre_auction_signal'):        score += 10

    return max(0, min(100, score))


# ─── Pre-leased asset promoter ────────────────────────────────────────────

def should_promote_to_pre_leased(event: dict, meta: dict) -> bool:
    """Decide if this event should also be written to pre_leased_assets."""
    cat   = event.get('signal_category', '')
    score = event.get('deal_score', 0)
    return (
        cat in ('pre_leased_asset', 'other') and
        score >= 40 and
        meta.get('asset_type', '').startswith('pre_leased')
    )


def build_pre_leased_row(event: dict, meta: dict) -> dict:
    cap = meta.get('cap_rate_data') or {}
    return {
        'name': event.get('company_name', ''),
        'address': event.get('location') or meta.get('location', ''),
        'micro_market': meta.get('location', ''),
        'asset_class': meta.get('asset_type', 'grade_b_office').replace('pre_leased_', ''),
        'total_area_sqft': meta.get('area_sqft'),
        'leased_area_sqft': meta.get('area_sqft'),
        'occupancy_pct': 100.0,
        'rent_per_sqft': meta.get('rent_psf'),
        'asking_price_crore': meta.get('price_crore'),
        'cap_rate_pct': cap.get('cap_rate_pct'),
        'noi_annual_cr': cap.get('noi_annual_cr'),
        'yield_on_cost_10yr_pct': cap.get('yield_on_cost_10yr_pct'),
        'irr_estimate_pct': cap.get('irr_estimate_pct'),
        'meets_investor_threshold': cap.get('meets_investor_threshold', False),
        'tenant_score': meta.get('tenant_score', 0),
        'tenant_category': meta.get('tenant_category', 'unknown'),
        'deal_score': event.get('deal_score', 0),
        'source': event.get('source', ''),
        'source_url': event.get('url', ''),
        'status': 'identified',
        'notes': event.get('headline', '')[:400],
    }


def should_promote_to_drt(event: dict, meta: dict) -> bool:
    """Promote events with case numbers to drt_cases."""
    return (
        event.get('channel') == 'drt' and
        bool(meta.get('case_number', '').strip())
    )


def build_drt_row(event: dict, meta: dict) -> dict:
    return {
        'case_number': meta.get('case_number', ''),
        'case_type': meta.get('case_type', 'OA'),
        'drt_bench': meta.get('drt_bench', 'DRT'),
        'borrower_name': event.get('company_name', ''),
        'bank_name': meta.get('bank', ''),
        'collateral_description': event.get('snippet', '')[:400],
        'collateral_location': event.get('location', ''),
        'is_mmr': event.get('is_mmr', False),
        'case_status': 'active',
        'source_url': event.get('url', ''),
    }


def should_promote_to_arc(event: dict, meta: dict) -> bool:
    return (
        event.get('channel') == 'arc_portfolio' or
        bool(meta.get('arc_entity', ''))
    )


def build_arc_row(event: dict, meta: dict) -> dict:
    return {
        'arc_entity': meta.get('arc_entity', 'Other'),
        'borrower_name': event.get('company_name', ''),
        'asset_description': event.get('snippet', '')[:400],
        'asset_location': event.get('location', ''),
        'asset_type': event.get('asset_class') or meta.get('asset_class', 'other'),
        'is_mmr': event.get('is_mmr', False),
        'total_exposure_crore': meta.get('price_crore'),
        'government_guarantee': meta.get('government_guarantee', False),
        'resolution_status': 'under_resolution',
        'source_url': event.get('url', ''),
    }


# ─── Main enrichment batch ────────────────────────────────────────────────

def enrich_batch(events: list[dict]) -> list[dict]:
    """
    Enrich a list of raw event dicts in-memory.
    Returns the same list with added fields (does NOT write to DB).
    Call this before db_insert.
    """
    for event in events:
        full_text = f"{event.get('headline', '')} {event.get('snippet', '')}".lower()
        meta = event.get('metadata') or {}
        if isinstance(meta, str):
            try: meta = json.loads(meta)
            except (ValueError, TypeError): meta = {}

        # Extract missing fields
        if not event.get('price_crore'):
            p = extract_price(full_text)
            if p: event['price_crore'] = p

        if not event.get('location'):
            loc = detect_mmr_location(full_text) or meta.get('location')
            if loc: event['location'] = loc

        if not event.get('asset_class'):
            event['asset_class'] = detect_asset_class(full_text)

        is_mmr = detect_mmr_location(full_text) is not None or meta.get('is_mmr', False)
        event['is_mmr'] = is_mmr

        if not event.get('channel'):
            event['channel'] = detect_channel(
                event.get('source', ''),
                event.get('signal_category', ''),
                meta,
            )

        # Score AFTER all fields populated
        event['deal_score'] = score_event(event)

    return events


def run_enrichment_on_db(hours_back: int = 2):
    """
    Fetch recent unenriched events from DB, enrich them, patch back.
    Run after every crawl batch to keep the DB current.
    """
    if not SUPABASE_URL:
        logger.warning("SUPABASE_URL not set — skipping DB enrichment")
        return

    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours_back)).isoformat()

    # Fetch events with deal_score=0 (unenriched) in the window
    events = _get('distress_events', {
        'detected_at': f'gte.{cutoff}',
        'deal_score': 'eq.0',
        'select': 'id,company_name,signal_keyword,signal_category,source,'
                  'url,headline,snippet,severity,metadata,is_duplicate',
        'limit': '500',
    })

    if not events:
        logger.info(f"No unenriched events in last {hours_back}h")
        return

    logger.info(f"Enriching {len(events)} events...")
    enriched = enrich_batch([dict(e) for e in events])

    promoted_pre_leased = 0
    promoted_drt        = 0
    promoted_arc        = 0
    patched             = 0

    for ev in enriched:
        if ev.get('is_duplicate'):
            continue

        ev_id = ev['id']
        meta  = ev.get('metadata') or {}

        # Patch enriched fields back to distress_events
        ok = _patch('distress_events', {
            'deal_score':    ev.get('deal_score', 0),
            'channel':       ev.get('channel', 'other'),
            'is_mmr':        ev.get('is_mmr', False),
            'asset_class':   ev.get('asset_class', 'other'),
            'price_crore':   ev.get('price_crore'),
            'location':      ev.get('location'),
        }, {'id': ev_id})
        if ok:
            patched += 1

        # Promote to pre_leased_assets
        if should_promote_to_pre_leased(ev, meta):
            row = build_pre_leased_row(ev, meta)
            if _post('pre_leased_assets', row):
                promoted_pre_leased += 1

        # Promote to drt_cases
        if should_promote_to_drt(ev, meta):
            row = build_drt_row(ev, meta)
            if _post('drt_cases', row):
                promoted_drt += 1

        # Promote to arc_portfolio
        if should_promote_to_arc(ev, meta):
            row = build_arc_row(ev, meta)
            if _post('arc_portfolio', row):
                promoted_arc += 1

    logger.info(
        f"Enrichment done: {patched} patched | "
        f"{promoted_pre_leased} → pre_leased | "
        f"{promoted_drt} → drt_cases | "
        f"{promoted_arc} → arc_portfolio"
    )


if __name__ == '__main__':
    import argparse
    p = argparse.ArgumentParser(description='Nexus Asia Enrichment Engine')
    p.add_argument('--hours-back', type=int, default=2,
                   help='How many hours back to enrich (default: 2)')
    p.add_argument('--backfill', action='store_true',
                   help='Backfill all unenriched events (sets hours-back=720)')
    args = p.parse_args()

    if args.backfill:
        logger.info("Backfill mode — enriching up to 30 days back")
        run_enrichment_on_db(hours_back=720)
    else:
        run_enrichment_on_db(hours_back=args.hours_back)
