"""
notifier.py — Nexus Asia Deal Alert System
═══════════════════════════════════════════════════════════════════════════
Sends alerts when high-priority deals hit the radar.

Channels:
  1. Slack webhook  — real-time deal alerts (deal_score >= 70)
  2. Email digest   — daily summary of all new deals (requires SMTP env)
  3. Telegram bot   — optional mobile push (TELEGRAM_BOT_TOKEN + CHAT_ID)

Alert triggers (from meeting intel):
  - Commercial MMR asset, deal_score >= 70 → immediate Slack ping
  - Bank auction notice, price 10–150 Cr   → Slack with auction date
  - DRT pre-auction signal                  → Slack (early warning)
  - ARC portfolio asset, commercial + MMR   → Slack
  - Daily digest: all new deals + deal pipeline update

Environment variables:
  SLACK_WEBHOOK_URL       — Slack incoming webhook URL
  ALERT_EMAIL_TO          — Recipient email (comma-separated for multiple)
  SMTP_HOST               — SMTP server (e.g. smtp.gmail.com)
  SMTP_PORT               — SMTP port (default 587)
  SMTP_USER               — SMTP username
  SMTP_PASSWORD           — SMTP password
  TELEGRAM_BOT_TOKEN      — optional
  TELEGRAM_CHAT_ID        — optional
  SUPABASE_URL / ANON_KEY — to fetch recent events
═══════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import os
import sys
import json
import smtplib
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime, timezone, timedelta
from typing import Optional

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("nexus.notifier")

SUPABASE_URL      = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY", "")
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")
ALERT_EMAIL_TO    = os.environ.get("ALERT_EMAIL_TO", "")
SMTP_HOST         = os.environ.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT         = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER         = os.environ.get("SMTP_USER", "")
SMTP_PASS         = os.environ.get("SMTP_PASSWORD", "")
TG_TOKEN          = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TG_CHAT           = os.environ.get("TELEGRAM_CHAT_ID", "")

DASHBOARD_URL = os.environ.get("DASHBOARD_URL", "https://YOUR_USERNAME.github.io/nexus-asia-distress-radar/")

# ─── DB helpers ──────────────────────────────────────────────────────────

def _h() -> dict:
    return {
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
    }


def db_get(table: str, params: dict) -> list:
    try:
        r = requests.get(f"{SUPABASE_URL}/rest/v1/{table}",
                         headers=_h(), params=params, timeout=15)
        return r.json() if r.status_code == 200 else []
    except Exception as e:
        logger.warning(f"db_get {table}: {e}")
        return []


# ─── Event fetch ─────────────────────────────────────────────────────────

def fetch_hot_deals(hours_back: int = 2, min_score: int = 70) -> list[dict]:
    """Fetch high-score MMR commercial events from last N hours."""
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours_back)).isoformat()
    return db_get('distress_events', {
        'detected_at':   f'gte.{cutoff}',
        'deal_score':    f'gte.{min_score}',
        'is_mmr':        'eq.true',
        'is_duplicate':  'eq.false',
        'select':        'id,company_name,signal_category,source,url,headline,'
                         'deal_score,price_crore,location,asset_class,channel,severity,metadata',
        'order':         'deal_score.desc',
        'limit':         '50',
    })


def fetch_new_auctions(hours_back: int = 8) -> list[dict]:
    """Fetch new bank auction listings."""
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours_back)).isoformat()
    return db_get('distress_events', {
        'detected_at':      f'gte.{cutoff}',
        'channel':          'eq.bank_auction',
        'is_duplicate':     'eq.false',
        'select':           'id,company_name,source,url,headline,price_crore,location,metadata',
        'order':            'deal_score.desc',
        'limit':            '30',
    })


def fetch_drt_signals(hours_back: int = 24) -> list[dict]:
    """Fetch new DRT / pre-auction signals."""
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours_back)).isoformat()
    return db_get('distress_events', {
        'detected_at':  f'gte.{cutoff}',
        'channel':      'eq.drt',
        'is_duplicate': 'eq.false',
        'select':       'id,company_name,source,url,headline,price_crore,location,metadata',
        'limit':        '20',
    })


def fetch_deal_pipeline() -> list[dict]:
    """Fetch active deals needing action."""
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    return db_get('deal_pipeline', {
        'next_action_date': f'lte.{today}',
        'stage':            'not.in.(closed,dropped)',
        'select':           'deal_name,stage,priority,next_action,next_action_date,assigned_to,deal_size_crore',
        'order':            'next_action_date.asc',
        'limit':            '20',
    })


# ─── Slack alerts ────────────────────────────────────────────────────────

def _severity_emoji(severity: str, score: int) -> str:
    if score >= 80 or severity == 'critical': return '🚨'
    if score >= 65 or severity == 'high':     return '⚠️'
    return '📍'


def _channel_emoji(channel: str) -> str:
    m = {
        'bank_auction': '🏦', 'sarfaesi': '⚖️', 'drt': '🏛️',
        'arc_portfolio': '🏗️', 'pre_leased_cre': '🏢',
        'legal_intelligence': '📋', 'pe_activity': '💼',
        'market_distress': '📉', 'regulatory': '📜', 'media': '📰',
    }
    return m.get(channel, '📌')


def slack_hot_deal(event: dict) -> bool:
    """Send a Slack alert for a single hot deal."""
    if not SLACK_WEBHOOK_URL:
        return False

    meta = event.get('metadata') or {}
    if isinstance(meta, str):
        try: meta = json.loads(meta)
        except: meta = {}

    score    = event.get('deal_score', 0)
    price    = event.get('price_crore')
    loc      = event.get('location', 'MMR')
    channel  = event.get('channel', 'other')
    severity = event.get('severity', 'medium')
    bank     = meta.get('bank', '')
    auction_date = meta.get('auction_date', '')

    price_str = f"₹{price:.1f} Cr" if price else "Price TBD"
    bank_str  = f" | {bank}" if bank else ""
    auction_str = f"\n📅 *Auction:* {auction_date}" if auction_date else ""

    text = (
        f"{_severity_emoji(severity, score)} *NEW DEAL SIGNAL* {_channel_emoji(channel)}\n"
        f"*{event.get('company_name', 'Unknown')}*\n"
        f"📍 {loc} | 💰 {price_str}{bank_str} | Score: {score}/100\n"
        f"📂 {channel.replace('_', ' ').upper()}{auction_str}\n"
        f"_{event.get('headline', '')[:140]}_\n"
        f"🔗 <{event.get('url', DASHBOARD_URL)}|View Source> | "
        f"<{DASHBOARD_URL}|Dashboard>"
    )

    try:
        r = requests.post(SLACK_WEBHOOK_URL, json={"text": text}, timeout=10)
        return r.status_code == 200
    except Exception as e:
        logger.warning(f"Slack send failed: {e}")
        return False


def slack_daily_digest(
    hot_deals: list[dict],
    auctions:  list[dict],
    pipeline:  list[dict],
) -> bool:
    """Send daily digest summary to Slack."""
    if not SLACK_WEBHOOK_URL:
        return False

    today = datetime.now(timezone.utc).strftime('%d %b %Y')

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"📊 Nexus Asia Daily Digest — {today}"},
        },
        {"type": "divider"},
    ]

    # Hot deals summary
    if hot_deals:
        top5 = hot_deals[:5]
        lines = []
        for ev in top5:
            p = ev.get('price_crore')
            p_str = f"₹{p:.0f}Cr" if p else "—"
            lines.append(
                f"• *{ev.get('company_name','?')}* | "
                f"{ev.get('location', ev.get('channel','?'))} | "
                f"{p_str} | Score {ev.get('deal_score',0)}"
            )
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*🔥 Hot Deals ({len(hot_deals)} new)*\n" + "\n".join(lines),
            },
        })
        blocks.append({"type": "divider"})

    # Upcoming bank auctions
    if auctions:
        top3 = auctions[:3]
        lines = []
        for ev in top3:
            meta = ev.get('metadata') or {}
            if isinstance(meta, str):
                try: meta = json.loads(meta)
                except: meta = {}
            p = ev.get('price_crore')
            dt = meta.get('auction_date', '—')
            lines.append(
                f"• {ev.get('source','Bank')} | "
                f"{ev.get('location','?')} | "
                f"₹{p:.0f}Cr | {dt}" if p else
                f"• {ev.get('source','Bank')} | {ev.get('location','?')} | {dt}"
            )
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*🏦 Bank Auctions ({len(auctions)} new)*\n" + "\n".join(lines),
            },
        })
        blocks.append({"type": "divider"})

    # Deal pipeline actions due
    if pipeline:
        lines = []
        for dp in pipeline:
            pri = '🔴' if dp.get('priority') == 'urgent' else '🟡' if dp.get('priority') == 'high' else '⚪'
            lines.append(
                f"{pri} *{dp.get('deal_name','?')}* | "
                f"{dp.get('stage','?').upper()} | "
                f"{dp.get('assigned_to','?')} — {dp.get('next_action','?')[:60]}"
            )
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*📋 Pipeline Actions Due ({len(pipeline)})*\n" + "\n".join(lines),
            },
        })
        blocks.append({"type": "divider"})

    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": f"<{DASHBOARD_URL}|🖥️ Open Full Dashboard>"},
    })

    try:
        r = requests.post(SLACK_WEBHOOK_URL, json={"blocks": blocks}, timeout=10)
        return r.status_code == 200
    except Exception as e:
        logger.warning(f"Slack digest failed: {e}")
        return False


# ─── Email digest ─────────────────────────────────────────────────────────

def send_email_digest(
    hot_deals: list[dict],
    auctions:  list[dict],
    drt_sigs:  list[dict],
    pipeline:  list[dict],
) -> bool:
    """Send HTML email daily digest."""
    if not all([ALERT_EMAIL_TO, SMTP_USER, SMTP_PASS]):
        logger.info("Email not configured — skipping")
        return False

    today = datetime.now(timezone.utc).strftime('%d %b %Y')
    recipients = [e.strip() for e in ALERT_EMAIL_TO.split(',') if e.strip()]

    def deal_rows(events: list[dict], max_rows: int = 10) -> str:
        rows = ""
        for ev in events[:max_rows]:
            p    = ev.get('price_crore')
            p_str = f"₹{p:.1f} Cr" if p else "—"
            meta = ev.get('metadata') or {}
            if isinstance(meta, str):
                try: meta = json.loads(meta)
                except: meta = {}
            bank = meta.get('bank', '')
            score = ev.get('deal_score', 0)
            score_color = '#e8253a' if score >= 75 else '#f5a623' if score >= 55 else '#888'
            rows += f"""
            <tr>
              <td style="padding:8px 10px;border-bottom:1px solid #222;">
                <a href="{ev.get('url','#')}" style="color:#4d8fff;text-decoration:none;">
                  {ev.get('company_name','?')[:50]}
                </a>
              </td>
              <td style="padding:8px 10px;border-bottom:1px solid #222;color:#aaa;">
                {ev.get('location') or ev.get('channel','?')}
              </td>
              <td style="padding:8px 10px;border-bottom:1px solid #222;">{p_str}</td>
              <td style="padding:8px 10px;border-bottom:1px solid #222;color:#aaa;">{bank or ev.get('source','')[:20]}</td>
              <td style="padding:8px 10px;border-bottom:1px solid #222;">
                <span style="color:{score_color};font-weight:bold;">{score}</span>
              </td>
            </tr>"""
        return rows

    def pipeline_rows(items: list[dict]) -> str:
        rows = ""
        for dp in items[:10]:
            pri_color = '#e8253a' if dp.get('priority') == 'urgent' else '#f5a623'
            rows += f"""
            <tr>
              <td style="padding:8px 10px;border-bottom:1px solid #222;">
                <strong>{dp.get('deal_name','?')}</strong>
              </td>
              <td style="padding:8px 10px;border-bottom:1px solid #222;color:#aaa;">
                {dp.get('stage','?').upper()}
              </td>
              <td style="padding:8px 10px;border-bottom:1px solid #222;">
                <span style="color:{pri_color};">{dp.get('priority','normal').upper()}</span>
              </td>
              <td style="padding:8px 10px;border-bottom:1px solid #222;color:#aaa;">
                {dp.get('assigned_to','?')}
              </td>
              <td style="padding:8px 10px;border-bottom:1px solid #222;font-size:12px;">
                {dp.get('next_action','')[:60]}
              </td>
            </tr>"""
        return rows

    table_style = (
        "width:100%;border-collapse:collapse;background:#111;"
        "color:#e8e8f0;font-family:'Courier New',monospace;font-size:13px;"
    )
    th_style = (
        "padding:8px 10px;background:#1a1a2e;text-align:left;"
        "color:#888;font-size:11px;letter-spacing:0.08em;text-transform:uppercase;"
    )

    html = f"""
    <!DOCTYPE html>
    <html>
    <head><meta charset="UTF-8"/></head>
    <body style="background:#0a0a0b;color:#e8e8f0;font-family:Arial,sans-serif;padding:0;margin:0;">
    <div style="max-width:820px;margin:0 auto;padding:20px;">

      <!-- Header -->
      <div style="background:#e8253a;padding:12px 20px;margin-bottom:24px;">
        <span style="font-family:'Courier New',monospace;font-weight:bold;
                     letter-spacing:0.15em;font-size:15px;color:#fff;">
          ◈ NEXUS ASIA DISTRESS RADAR — {today}
        </span>
      </div>

      <!-- Stats bar -->
      <div style="display:flex;gap:12px;margin-bottom:24px;flex-wrap:wrap;">
        {_stat_badge('Hot Deals', len(hot_deals), '#e8253a')}
        {_stat_badge('Bank Auctions', len(auctions), '#f5a623')}
        {_stat_badge('DRT Pre-Signals', len(drt_sigs), '#4d8fff')}
        {_stat_badge('Pipeline Actions Due', len(pipeline), '#00c896')}
      </div>

      <!-- Hot Deals -->
      {'<h2 style="color:#e8253a;font-size:13px;letter-spacing:0.1em;text-transform:uppercase;margin-bottom:12px;">🔥 HOT DEALS (Score ≥70, MMR Commercial)</h2><table style="' + table_style + '"><tr><th style="' + th_style + '">Company</th><th style="' + th_style + '">Location</th><th style="' + th_style + '">Price</th><th style="' + th_style + '">Source</th><th style="' + th_style + '">Score</th></tr>' + deal_rows(hot_deals) + '</table><br/>' if hot_deals else ''}

      <!-- Bank Auctions -->
      {'<h2 style="color:#f5a623;font-size:13px;letter-spacing:0.1em;text-transform:uppercase;margin-bottom:12px;">🏦 NEW BANK AUCTIONS</h2><table style="' + table_style + '"><tr><th style="' + th_style + '">Property</th><th style="' + th_style + '">Location</th><th style="' + th_style + '">Reserve Price</th><th style="' + th_style + '">Bank</th><th style="' + th_style + '">Score</th></tr>' + deal_rows(auctions) + '</table><br/>' if auctions else ''}

      <!-- DRT Signals -->
      {'<h2 style="color:#4d8fff;font-size:13px;letter-spacing:0.1em;text-transform:uppercase;margin-bottom:12px;">⚖️ DRT PRE-AUCTION SIGNALS (Early Warning)</h2><table style="' + table_style + '"><tr><th style="' + th_style + '">Company</th><th style="' + th_style + '">Location</th><th style="' + th_style + '">Est. Value</th><th style="' + th_style + '">Source</th><th style="' + th_style + '">Score</th></tr>' + deal_rows(drt_sigs) + '</table><br/>' if drt_sigs else ''}

      <!-- Pipeline -->
      {'<h2 style="color:#00c896;font-size:13px;letter-spacing:0.1em;text-transform:uppercase;margin-bottom:12px;">📋 PIPELINE ACTIONS DUE</h2><table style="' + table_style + '"><tr><th style="' + th_style + '">Deal</th><th style="' + th_style + '">Stage</th><th style="' + th_style + '">Priority</th><th style="' + th_style + '">Owner</th><th style="' + th_style + '">Next Action</th></tr>' + pipeline_rows(pipeline) + '</table><br/>' if pipeline else ''}

      <!-- Footer -->
      <div style="margin-top:24px;padding:12px 0;border-top:1px solid #222;
                  font-family:'Courier New',monospace;font-size:11px;color:#555;">
        <a href="{DASHBOARD_URL}" style="color:#4d8fff;">Open Dashboard ↗</a> &nbsp;|&nbsp;
        Nexus Asia Distress Radar &nbsp;|&nbsp; {today}
      </div>
    </div>
    </body>
    </html>
    """

    msg = MIMEMultipart('alternative')
    msg['Subject'] = f"[Nexus Asia] {len(hot_deals)} hot deals | {len(auctions)} auctions | {today}"
    msg['From']    = SMTP_USER
    msg['To']      = ', '.join(recipients)
    msg.attach(MIMEText(html, 'html'))

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as smtp:
            smtp.starttls()
            smtp.login(SMTP_USER, SMTP_PASS)
            smtp.sendmail(SMTP_USER, recipients, msg.as_string())
        logger.info(f"Email sent to {recipients}")
        return True
    except Exception as e:
        logger.error(f"Email send failed: {e}")
        return False


def _stat_badge(label: str, count: int, color: str) -> str:
    return (
        f'<div style="background:#111;border:1px solid #333;padding:10px 16px;">'
        f'<div style="font-size:22px;font-weight:bold;color:{color};">{count}</div>'
        f'<div style="font-size:11px;color:#666;letter-spacing:0.08em;text-transform:uppercase;">'
        f'{label}</div></div>'
    )


# ─── Telegram ─────────────────────────────────────────────────────────────

def telegram_alert(text: str) -> bool:
    """Send a Telegram message."""
    if not TG_TOKEN or not TG_CHAT:
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT, "text": text, "parse_mode": "Markdown"},
            timeout=10,
        )
        return r.status_code == 200
    except Exception as e:
        logger.warning(f"Telegram failed: {e}")
        return False


# ─── Main ─────────────────────────────────────────────────────────────────

def run(mode: str = 'digest'):
    """
    mode='digest'  — daily digest (Slack blocks + email)
    mode='realtime' — immediate Slack alert for deals found last 2h
    """
    logger.info(f"Notifier — mode={mode}")

    hot_deals = fetch_hot_deals(hours_back=2 if mode == 'realtime' else 24)
    auctions  = fetch_new_auctions(hours_back=8 if mode == 'realtime' else 24)
    drt_sigs  = fetch_drt_signals(hours_back=24)
    pipeline  = fetch_deal_pipeline()

    logger.info(f"Found: {len(hot_deals)} hot | {len(auctions)} auctions | "
                f"{len(drt_sigs)} DRT | {len(pipeline)} pipeline")

    if mode == 'realtime':
        # Send individual Slack pings for each hot deal
        sent = 0
        for ev in hot_deals[:10]:
            if slack_hot_deal(ev):
                sent += 1
        for ev in auctions[:5]:
            if slack_hot_deal(ev):
                sent += 1
        logger.info(f"Realtime: {sent} Slack alerts sent")

        # Telegram ping for top deal
        if hot_deals:
            top = hot_deals[0]
            p   = top.get('price_crore')
            telegram_alert(
                f"🚨 *HOT DEAL* Score {top.get('deal_score',0)}/100\n"
                f"{top.get('company_name','?')}\n"
                f"📍 {top.get('location','MMR')} | "
                f"{'₹'+str(round(p,1))+'Cr' if p else 'TBD'}\n"
                f"{top.get('url','')}"
            )
    else:
        # Daily digest
        slack_daily_digest(hot_deals, auctions, pipeline)
        send_email_digest(hot_deals, auctions, drt_sigs, pipeline)


if __name__ == '__main__':
    import argparse
    p = argparse.ArgumentParser(description='Nexus Asia Notifier')
    p.add_argument('--mode', choices=['digest', 'realtime'], default='digest')
    args = p.parse_args()
    run(mode=args.mode)
