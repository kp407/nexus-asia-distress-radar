-- ============================================================
-- NEXUS ASIA DISTRESS RADAR — Supabase Schema
-- Run this in your Supabase SQL Editor
-- ============================================================

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================
-- SOURCES TABLE
-- Registry of all crawled sources
-- ============================================================
CREATE TABLE IF NOT EXISTS sources (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name TEXT NOT NULL,
    url TEXT NOT NULL,
    category TEXT NOT NULL CHECK (category IN ('regulatory', 'financial_media', 'auction', 'legal', 'other')),
    country TEXT NOT NULL DEFAULT 'IN',
    is_active BOOLEAN DEFAULT TRUE,
    last_crawled_at TIMESTAMPTZ,
    crawl_interval_minutes INTEGER DEFAULT 30,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- COMPANIES TABLE
-- Normalized company registry
-- ============================================================
CREATE TABLE IF NOT EXISTS companies (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name TEXT NOT NULL,
    cin TEXT,                          -- Corporate Identification Number (India)
    sector TEXT,
    country TEXT DEFAULT 'IN',
    first_signal_at TIMESTAMPTZ,
    last_signal_at TIMESTAMPTZ,
    signal_count INTEGER DEFAULT 0,
    risk_score INTEGER DEFAULT 0 CHECK (risk_score BETWEEN 0 AND 100),
    status TEXT DEFAULT 'monitoring' CHECK (status IN ('monitoring', 'active_cirp', 'liquidation', 'resolved', 'watch')),
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(name)
);

-- ============================================================
-- DISTRESS EVENTS TABLE
-- Core signal log — every detected distress event
-- ============================================================
CREATE TABLE IF NOT EXISTS distress_events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    company_name TEXT NOT NULL,
    company_id UUID REFERENCES companies(id) ON DELETE SET NULL,
    signal_keyword TEXT NOT NULL,
    signal_category TEXT DEFAULT 'general' CHECK (
        signal_category IN ('insolvency', 'auction', 'restructuring', 'default', 'legal', 'regulatory', 'general')
    ),
    source TEXT NOT NULL,
    source_id UUID REFERENCES sources(id) ON DELETE SET NULL,
    url TEXT,
    headline TEXT,
    snippet TEXT,
    detected_at TIMESTAMPTZ DEFAULT NOW(),
    published_at TIMESTAMPTZ,
    severity TEXT DEFAULT 'medium' CHECK (severity IN ('low', 'medium', 'high', 'critical')),
    is_verified BOOLEAN DEFAULT FALSE,
    is_duplicate BOOLEAN DEFAULT FALSE,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- ASSETS TABLE
-- Distressed assets identified for auction/sale
-- ============================================================
CREATE TABLE IF NOT EXISTS assets (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    company_id UUID REFERENCES companies(id) ON DELETE SET NULL,
    company_name TEXT NOT NULL,
    asset_type TEXT CHECK (asset_type IN ('real_estate', 'plant_machinery', 'financial_asset', 'ip', 'business_unit', 'other')),
    description TEXT,
    reserve_price NUMERIC(20, 2),
    currency TEXT DEFAULT 'INR',
    auction_date DATE,
    auction_url TEXT,
    source TEXT,
    source_event_id UUID REFERENCES distress_events(id) ON DELETE SET NULL,
    status TEXT DEFAULT 'upcoming' CHECK (status IN ('upcoming', 'open', 'sold', 'cancelled', 'relisted')),
    location TEXT,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- CRAWLER RUNS TABLE
-- Audit log of every crawl execution
-- ============================================================
CREATE TABLE IF NOT EXISTS crawler_runs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    run_id TEXT NOT NULL,
    source_name TEXT,
    status TEXT CHECK (status IN ('started', 'completed', 'failed', 'partial')),
    events_found INTEGER DEFAULT 0,
    events_inserted INTEGER DEFAULT 0,
    error_message TEXT,
    duration_seconds NUMERIC(10, 2),
    started_at TIMESTAMPTZ DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

-- ============================================================
-- INDEXES FOR PERFORMANCE
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_distress_events_company ON distress_events(company_name);
CREATE INDEX IF NOT EXISTS idx_distress_events_detected_at ON distress_events(detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_distress_events_signal_keyword ON distress_events(signal_keyword);
CREATE INDEX IF NOT EXISTS idx_distress_events_source ON distress_events(source);
CREATE INDEX IF NOT EXISTS idx_distress_events_severity ON distress_events(severity);
CREATE INDEX IF NOT EXISTS idx_companies_name ON companies(name);
CREATE INDEX IF NOT EXISTS idx_companies_status ON companies(status);
CREATE INDEX IF NOT EXISTS idx_assets_auction_date ON assets(auction_date);

-- ============================================================
-- ROW LEVEL SECURITY (RLS) — public read, service-key write
-- ============================================================
ALTER TABLE distress_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE companies ENABLE ROW LEVEL SECURITY;
ALTER TABLE assets ENABLE ROW LEVEL SECURITY;
ALTER TABLE sources ENABLE ROW LEVEL SECURITY;
ALTER TABLE crawler_runs ENABLE ROW LEVEL SECURITY;

-- Allow public (anon) SELECT on main tables (dashboard uses anon key)
CREATE POLICY "Public read distress_events" ON distress_events FOR SELECT USING (true);
CREATE POLICY "Public read companies" ON companies FOR SELECT USING (true);
CREATE POLICY "Public read assets" ON assets FOR SELECT USING (true);
CREATE POLICY "Public read sources" ON sources FOR SELECT USING (true);
CREATE POLICY "Public read crawler_runs" ON crawler_runs FOR SELECT USING (true);

-- Allow anon INSERT (crawler runs with anon key via env var)
CREATE POLICY "Anon insert distress_events" ON distress_events FOR INSERT WITH CHECK (true);
CREATE POLICY "Anon insert companies" ON companies FOR INSERT WITH CHECK (true);
CREATE POLICY "Anon upsert companies" ON companies FOR UPDATE USING (true);
CREATE POLICY "Anon insert assets" ON assets FOR INSERT WITH CHECK (true);
CREATE POLICY "Anon insert crawler_runs" ON crawler_runs FOR INSERT WITH CHECK (true);
CREATE POLICY "Anon update crawler_runs" ON crawler_runs FOR UPDATE USING (true);
CREATE POLICY "Anon update sources" ON sources FOR UPDATE USING (true);

-- ============================================================
-- SEED DATA — Source Registry
-- ============================================================
INSERT INTO sources (name, url, category, country) VALUES
    ('NCLT', 'https://nclt.gov.in', 'regulatory', 'IN'),
    ('IBBI', 'https://ibbi.gov.in', 'regulatory', 'IN'),
    ('MCA', 'https://www.mca.gov.in', 'regulatory', 'IN'),
    ('Economic Times Markets', 'https://economictimes.indiatimes.com/markets', 'financial_media', 'IN'),
    ('Business Standard', 'https://www.business-standard.com/finance', 'financial_media', 'IN'),
    ('Mint', 'https://www.livemint.com/companies', 'financial_media', 'IN'),
    ('Reuters India', 'https://www.reuters.com/world/india', 'financial_media', 'GLOBAL'),
    ('Bloomberg Markets', 'https://www.bloomberg.com/markets', 'financial_media', 'GLOBAL'),
    ('Bank Auction India', 'https://www.bankauction.in', 'auction', 'IN'),
    ('SARFAESI Auctions', 'https://ibapi.in', 'auction', 'IN')
ON CONFLICT DO NOTHING;

-- ============================================================
-- USEFUL VIEWS
-- ============================================================

-- Recent signals (last 7 days)
CREATE OR REPLACE VIEW recent_signals AS
SELECT
    de.id,
    de.company_name,
    de.signal_keyword,
    de.signal_category,
    de.source,
    de.url,
    de.headline,
    de.snippet,
    de.detected_at,
    de.severity,
    de.is_verified
FROM distress_events de
WHERE de.detected_at > NOW() - INTERVAL '7 days'
  AND de.is_duplicate = FALSE
ORDER BY de.detected_at DESC;

-- Company risk summary
CREATE OR REPLACE VIEW company_risk_summary AS
SELECT
    c.id,
    c.name,
    c.sector,
    c.status,
    c.risk_score,
    c.signal_count,
    c.first_signal_at,
    c.last_signal_at,
    COUNT(de.id) FILTER (WHERE de.severity = 'critical') AS critical_signals,
    COUNT(de.id) FILTER (WHERE de.severity = 'high') AS high_signals
FROM companies c
LEFT JOIN distress_events de ON de.company_name = c.name
GROUP BY c.id, c.name, c.sector, c.status, c.risk_score, c.signal_count, c.first_signal_at, c.last_signal_at
ORDER BY c.risk_score DESC;

-- Upcoming auctions
CREATE OR REPLACE VIEW upcoming_auctions AS
SELECT
    a.id,
    a.company_name,
    a.asset_type,
    a.description,
    a.reserve_price,
    a.currency,
    a.auction_date,
    a.auction_url,
    a.source,
    a.status,
    a.location
FROM assets a
WHERE a.auction_date >= CURRENT_DATE
  AND a.status IN ('upcoming', 'open')
ORDER BY a.auction_date ASC;
