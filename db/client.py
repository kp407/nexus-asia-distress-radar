"""
db/client.py — Nexus Asia Supabase Client v2
All tables: distress_events, companies, assets, pre_leased_assets,
drt_cases, arc_portfolio, deal_pipeline, investor_mandates,
cap_rate_snapshots, crawler_runs.
"""
from __future__ import annotations
import os, logging, requests
from datetime import datetime, timezone, timedelta, date
from typing import Optional, Any
logger = logging.getLogger(__name__)

class SupabaseClient:
    def __init__(self, url='', anon_key='', service_key=''):
        self.url      = (url or os.environ.get('SUPABASE_URL','')).rstrip('/')
        self.anon_key = anon_key or os.environ.get('SUPABASE_ANON_KEY','')
        self.svc_key  = service_key or os.environ.get('SUPABASE_SERVICE_ROLE_KEY', self.anon_key)
        if not self.url or not self.anon_key:
            raise EnvironmentError("SUPABASE_URL and SUPABASE_ANON_KEY must be set.")

    def _h(self, write=False, prefer='return=minimal'):
        key = self.svc_key if write else self.anon_key
        return {'apikey': key, 'Authorization': f'Bearer {key}',
                'Content-Type': 'application/json', 'Prefer': prefer}

    def _ep(self, table): return f'{self.url}/rest/v1/{table}'

    def select(self, table, params=None):
        try:
            r = requests.get(self._ep(table), headers=self._h(prefer='return=representation'),
                             params=params or {}, timeout=15)
            return r.json() if r.status_code == 200 else []
        except Exception as e:
            logger.error(f'SELECT {table}: {e}'); return []

    def insert(self, table, data):
        payload = data if isinstance(data, list) else [data]
        try:
            r = requests.post(self._ep(table), headers=self._h(write=True),
                              json=payload, timeout=15)
            if r.status_code in (200,201): return True
            logger.error(f'INSERT {table} {r.status_code}: {r.text[:120]}'); return False
        except Exception as e:
            logger.error(f'INSERT {table}: {e}'); return False

    def upsert(self, table, data, on_conflict=''):
        payload = data if isinstance(data, list) else [data]
        params  = {'on_conflict': on_conflict} if on_conflict else {}
        try:
            r = requests.post(self._ep(table),
                              headers=self._h(write=True, prefer='resolution=merge-duplicates,return=minimal'),
                              json=payload, params=params, timeout=15)
            if r.status_code in (200,201): return True
            logger.error(f'UPSERT {table} {r.status_code}: {r.text[:120]}'); return False
        except Exception as e:
            logger.error(f'UPSERT {table}: {e}'); return False

    def update(self, table, data, match):
        params = {k: f'eq.{v}' for k,v in match.items()}
        try:
            r = requests.patch(self._ep(table), headers=self._h(write=True),
                               params=params, json=data, timeout=15)
            if r.status_code in (200,204): return True
            logger.error(f'UPDATE {table} {r.status_code}: {r.text[:120]}'); return False
        except Exception as e:
            logger.error(f'UPDATE {table}: {e}'); return False

    def delete(self, table, match):
        params = {k: f'eq.{v}' for k,v in match.items()}
        try:
            r = requests.delete(self._ep(table), headers=self._h(write=True),
                                params=params, timeout=15)
            return r.status_code in (200,204)
        except Exception as e:
            logger.error(f'DELETE {table}: {e}'); return False

    # ── Distress events ──────────────────────────────────────────────────

    def is_duplicate_event(self, company, keyword, source):
        today = datetime.now(timezone.utc).strftime('%Y-%m-%dT00:00:00Z')
        rows  = self.select('distress_events', {
            'company_name': f'ilike.{company}', 'signal_keyword': f'eq.{keyword}',
            'source': f'eq.{source}', 'detected_at': f'gte.{today}',
            'select': 'id', 'limit': '1'})
        return len(rows) > 0

    def insert_event(self, event):
        if self.is_duplicate_event(event.get('company_name',''),
                                   event.get('signal_keyword',''),
                                   event.get('source','')):
            return False
        return self.insert('distress_events', event)

    def insert_events_batch(self, events):
        ins = sk = fa = 0
        for ev in events:
            if self.is_duplicate_event(ev.get('company_name',''),
                                       ev.get('signal_keyword',''),
                                       ev.get('source','')):
                sk += 1; continue
            if self.insert('distress_events', ev): ins += 1
            else: fa += 1
        return ins, sk, fa

    def get_hot_mmr_commercial(self, min_score=70, hours_back=24):
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours_back)).isoformat()
        return self.select('distress_events', {
            'detected_at': f'gte.{cutoff}', 'deal_score': f'gte.{min_score}',
            'is_mmr': 'eq.true', 'is_duplicate': 'eq.false',
            'order': 'deal_score.desc', 'limit': '50'})

    # ── Company ──────────────────────────────────────────────────────────

    def ensure_company(self, name, sector=''):
        if not name or name in ('Unknown',''): return None
        now = datetime.now(timezone.utc).isoformat()
        try:
            rows = self.select('companies', {'name': f'eq.{name}', 'select': 'id,signal_count', 'limit': '1'})
            if rows:
                cid = rows[0]['id']; cnt = (rows[0].get('signal_count') or 0) + 1
                self.update('companies', {'last_signal_at': now, 'signal_count': cnt, 'updated_at': now}, {'id': cid})
                return cid
            r = requests.post(self._ep('companies'),
                              headers=self._h(write=True, prefer='return=representation'),
                              json={'name': name, 'sector': sector or None,
                                    'first_signal_at': now, 'last_signal_at': now, 'signal_count': 1},
                              timeout=10)
            if r.status_code in (200,201):
                data = r.json()
                return (data[0] if isinstance(data, list) else data).get('id')
        except Exception as e:
            logger.error(f'ensure_company {name}: {e}')
        return None

    # ── Pre-leased assets ────────────────────────────────────────────────

    def upsert_pre_leased(self, asset):
        return self.upsert('pre_leased_assets', asset, on_conflict='source_url')

    def get_investor_ready_assets(self, min_cap_rate=8.5):
        return self.select('pre_leased_assets', {
            'cap_rate_pct': f'gte.{min_cap_rate}',
            'status': 'in.(identified,under_review,in_discussion)',
            'order': 'deal_score.desc', 'limit': '50'})

    # ── Deal pipeline ────────────────────────────────────────────────────

    def get_pipeline_due_today(self):
        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        return self.select('deal_pipeline', {
            'next_action_date': f'lte.{today}',
            'stage': 'not.in.(closed,dropped)',
            'order': 'next_action_date.asc'})

    def advance_pipeline_stage(self, deal_id, new_stage, notes=''):
        data: dict[str,Any] = {'stage': new_stage, 'updated_at': datetime.now(timezone.utc).isoformat()}
        if notes: data['notes'] = notes
        return self.update('deal_pipeline', data, {'id': deal_id})

    # ── DRT cases ────────────────────────────────────────────────────────

    def upsert_drt_case(self, case):
        return self.upsert('drt_cases', case, on_conflict='case_number,drt_bench')

    def get_active_drt_cases(self, mmr_only=True):
        params: dict[str,str] = {'case_status': 'in.(filed,active,rc_issued)',
                                  'order': 'filing_date.desc', 'limit': '100'}
        if mmr_only: params['is_mmr'] = 'eq.true'
        return self.select('drt_cases', params)

    # ── ARC portfolio ────────────────────────────────────────────────────

    def upsert_arc_asset(self, asset):
        return self.upsert('arc_portfolio', asset, on_conflict='arc_entity,borrower_name')

    def get_arc_motivated_sellers(self):
        return self.select('arc_portfolio', {
            'resolution_status': 'in.(under_resolution,sale_process_initiated)',
            'asset_type': 'in.(commercial,industrial,mixed_use)',
            'order': 'total_exposure_crore.desc', 'limit': '50'})

    # ── Crawler run log ──────────────────────────────────────────────────

    def start_run(self, run_id, source_name):
        r = requests.post(self._ep('crawler_runs'),
                          headers=self._h(write=True, prefer='return=representation'),
                          json={'run_id': run_id, 'source_name': source_name,
                                'status': 'started',
                                'started_at': datetime.now(timezone.utc).isoformat()},
                          timeout=10)
        if r.status_code in (200,201):
            data = r.json()
            return (data[0] if isinstance(data, list) else data).get('id')
        return None

    def complete_run(self, row_id, events_found, events_inserted, status='completed', error='', duration=0.0):
        return self.update('crawler_runs', {
            'status': status, 'events_found': events_found,
            'events_inserted': events_inserted,
            'error_message': error[:500] if error else None,
            'duration_seconds': round(duration, 2),
            'completed_at': datetime.now(timezone.utc).isoformat()
        }, {'id': row_id})

    def update_source_crawled(self, source_name):
        return self.update('sources',
                           {'last_crawled_at': datetime.now(timezone.utc).isoformat()},
                           {'name': source_name})

    # ── Cap rate snapshots ───────────────────────────────────────────────

    def add_cap_rate_snapshot(self, micro_market, asset_class, cap_rate_pct,
                               avg_rent_psf=None, source='crawler'):
        return self.insert('cap_rate_snapshots', {
            'micro_market': micro_market, 'asset_class': asset_class,
            'cap_rate_pct': cap_rate_pct, 'avg_rent_psf': avg_rent_psf,
            'snapshot_date': datetime.now(timezone.utc).strftime('%Y-%m-%d'),
            'source': source})

    # ── Convenience queries ──────────────────────────────────────────────

    def get_upcoming_auctions(self, days_ahead=30):
        today  = date.today().isoformat()
        future = (date.today() + timedelta(days=days_ahead)).isoformat()
        return self.select('assets', {
            'auction_date': f'gte.{today}',
            'status': 'in.(upcoming,open)', 'order': 'auction_date.asc', 'limit': '50'})

    def get_company_risk_summary(self, status=''):
        params: dict[str,str] = {
            'select': 'id,name,sector,status,risk_score,signal_count,last_signal_at',
            'order': 'risk_score.desc', 'limit': '100'}
        if status: params['status'] = f'eq.{status}'
        return self.select('companies', params)
