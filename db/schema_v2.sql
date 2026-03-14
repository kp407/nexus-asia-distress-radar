-- ============================================================
-- NEXUS ASIA DISTRESS RADAR — Schema v2
-- Run this in your Supabase SQL Editor AFTER the base schema
-- Adds: pre_leased_assets, deal_pipeline, cap_rate_snapshots,
--       arc_portfolio, drt_cases, investor_mandates
-- ============================================================

-- ============================================================
-- EXTEND distress_events — add order_date and new categories
-- ============================================================
ALTER TABLE distress_events
    ADD COLUMN IF NOT EXISTS order_date DATE,
    ADD COLUMN IF NOT EXISTS deal_score INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS asset_class TEXT CHECK (
        asset_class IN ('commercial', 'residential', 'land', 'industrial', 'hospitality', 'other')
    ),
    ADD COLUMN IF NOT EXISTS price_crore NUMERIC(12, 2),
    ADD COLUMN IF NOT EXISTS location TEXT,
    ADD COLUMN IF NOT EXISTS is_mmr BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS channel TEXT CHECK (
        channel IN (
            'bank_auction', 'sarfaesi', 'drt', 'legal_intelligence',
            'pre_leased_cre', 'arc_portfolio', 'pe_activity',
            'market_distress', 'media', 'regulatory', 'other'
        )
    );

-- Extend signal_category to include new types
ALTER TABLE distress_events
    DROP CONSTRAINT IF EXISTS distress_events_signal_category_check;
ALTER TABLE distress_events
    ADD CONSTRAINT distress_events_signal_category_check CHECK (
        signal_category IN (
            'insolvency', 'auction', 'restructuring', 'default',
            'legal', 'regulatory', 'general',
            'sarfaesi', 'creditor_action', 'rbi_action',
            'distressed_asset', 'cirp', 'liquidation',
            'pre_leased_asset', 'cre_vacancy', 'arc_portfolio',
            'pe_activity', 'market_stress', 'other'
        )
    );

-- ============================================================
-- PRE_LEASED_ASSETS TABLE
-- Grade A and B commercial properties with active leases
-- Core product for international investor pitches
-- ============================================================
CREATE TABLE IF NOT EXISTS pre_leased_assets (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Property identity
    name TEXT,
    address TEXT,
    micro_market TEXT,                    -- BKC, Lower Parel, Malad, etc.
    city TEXT DEFAULT 'Mumbai',
    is_mmr BOOLEAN DEFAULT TRUE,

    -- Physical specs
    asset_class TEXT CHECK (asset_class IN (
        'grade_a_office', 'grade_b_office', 'it_park',
        'retail_mall', 'retail_highstreet', 'industrial',
        'hospitality', 'mixed_use'
    )),
    total_area_sqft NUMERIC(14, 2),
    leased_area_sqft NUMERIC(14, 2),
    occupancy_pct NUMERIC(5, 2),

    -- Lease details
    tenant_name TEXT,
    tenant_category TEXT CHECK (tenant_category IN (
        'blue_chip', 'institutional', 'government', 'listed_company',
        'mnc', 'startup', 'unknown'
    )),
    tenant_score INTEGER DEFAULT 0 CHECK (tenant_score BETWEEN 0 AND 100),
    lease_start_date DATE,
    lease_expiry_date DATE,
    lock_in_months INTEGER,
    rent_per_sqft NUMERIC(10, 2),
    rent_escalation_pct NUMERIC(5, 2) DEFAULT 15,
    escalation_frequency_years INTEGER DEFAULT 3,

    -- Financials
    asking_price_crore NUMERIC(12, 2),
    gross_rent_annual_cr NUMERIC(10, 3),
    noi_annual_cr NUMERIC(10, 3),
    cap_rate_pct NUMERIC(5, 2),
    yield_on_cost_10yr_pct NUMERIC(6, 2),
    irr_estimate_pct NUMERIC(5, 2),
    meets_investor_threshold BOOLEAN DEFAULT FALSE,  -- 8.5%+ cap rate

    -- Deal context
    seller_type TEXT CHECK (seller_type IN (
        'bank_npa', 'arc', 'narcl', 'pe_exit', 'developer',
        'promoter_distress', 'family_office', 'other'
    )),
    urgency_level TEXT DEFAULT 'normal' CHECK (urgency_level IN (
        'normal', 'motivated', 'distressed', 'desperate'
    )),
    strata_complications BOOLEAN DEFAULT FALSE,
    oc_received BOOLEAN DEFAULT TRUE,
    title_clear BOOLEAN DEFAULT TRUE,

    -- Source
    source_event_id UUID REFERENCES distress_events(id) ON DELETE SET NULL,
    source TEXT,
    source_url TEXT,
    deal_score INTEGER DEFAULT 0 CHECK (deal_score BETWEEN 0 AND 100),

    -- Status tracking
    status TEXT DEFAULT 'identified' CHECK (status IN (
        'identified', 'under_review', 'in_discussion',
        'loi_signed', 'due_diligence', 'closed', 'dropped'
    )),
    notes TEXT,

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- CAP RATE SNAPSHOTS — track cap rate over time by micro-market
-- For building investor pitch data on escalation story
-- ============================================================
CREATE TABLE IF NOT EXISTS cap_rate_snapshots (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    micro_market TEXT NOT NULL,
    asset_class TEXT NOT NULL,
    cap_rate_pct NUMERIC(5, 2) NOT NULL,
    avg_rent_psf NUMERIC(8, 2),
    avg_price_psf NUMERIC(10, 2),
    sample_size INTEGER DEFAULT 1,
    snapshot_date DATE DEFAULT CURRENT_DATE,
    source TEXT,
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Seed with current Mumbai benchmarks (from deal intel)
INSERT INTO cap_rate_snapshots (micro_market, asset_class, cap_rate_pct, avg_rent_psf, snapshot_date, source, notes) VALUES
    ('BKC',           'grade_a_office', 5.8,  350, '2025-01-01', 'Market Intel', 'Core BKC Grade A'),
    ('Lower Parel',   'grade_a_office', 6.0,  280, '2025-01-01', 'Market Intel', 'Lower Parel office corridor'),
    ('Andheri',       'grade_a_office', 6.8,  150, '2025-01-01', 'Market Intel', 'Andheri East IT zone'),
    ('Powai',         'grade_a_office', 7.0,  130, '2025-01-01', 'Market Intel', 'Powai IT/tech campus'),
    ('Malad',         'grade_a_office', 7.5,  115, '2025-01-01', 'Market Intel', 'Malad suburban office'),
    ('Goregaon',      'grade_a_office', 7.3,  120, '2025-01-01', 'Market Intel', 'Goregaon SBD'),
    ('Kurla',         'grade_b_office', 7.8,  110, '2025-01-01', 'Market Intel', 'Kurla commercial'),
    ('Thane',         'grade_a_office', 8.2,  80,  '2025-01-01', 'Market Intel', 'Thane suburban'),
    ('Navi Mumbai',   'grade_a_office', 8.5,  75,  '2025-01-01', 'Market Intel', 'Navi Mumbai IT parks'),
    ('Airoli/Belapur','grade_b_office', 9.2,  65,  '2025-01-01', 'Market Intel', 'Belapur Node - meets threshold')
ON CONFLICT DO NOTHING;

-- ============================================================
-- DRT_CASES — track Debt Recovery Tribunal filings
-- Pre-auction intelligence: DRT filing → SARFAESI → auction
-- ============================================================
CREATE TABLE IF NOT EXISTS drt_cases (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    case_number TEXT,
    case_type TEXT CHECK (case_type IN ('OA', 'IA', 'RC', 'MA', 'other')),
    drt_bench TEXT,                        -- Mumbai-I, Mumbai-II, Pune, etc.
    borrower_name TEXT NOT NULL,
    borrower_sector TEXT,
    bank_name TEXT,
    loan_amount_crore NUMERIC(12, 2),
    collateral_description TEXT,
    collateral_location TEXT,
    collateral_type TEXT CHECK (collateral_type IN (
        'commercial', 'residential', 'land', 'plant_machinery', 'other'
    )),
    is_mmr BOOLEAN DEFAULT FALSE,
    filing_date DATE,
    last_hearing_date DATE,
    next_hearing_date DATE,
    case_status TEXT DEFAULT 'active' CHECK (case_status IN (
        'filed', 'active', 'rc_issued', 'possession_taken',
        'auction_scheduled', 'sold', 'settled', 'closed'
    )),
    source_event_id UUID REFERENCES distress_events(id) ON DELETE SET NULL,
    source_url TEXT,
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(case_number, drt_bench)
);

-- ============================================================
-- ARC_PORTFOLIO — NARCL, ARCIL, Edelweiss ARC, Phoenix ARC etc.
-- These are motivated sellers with distressed CRE assets
-- ============================================================
CREATE TABLE IF NOT EXISTS arc_portfolio (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    arc_entity TEXT NOT NULL CHECK (arc_entity IN (
        'NARCL', 'ARCIL', 'Edelweiss ARC', 'Phoenix ARC',
        'JM Financial ARC', 'Kotak ARC', 'CFM ARC', 'Omkara ARC', 'Other'
    )),
    borrower_name TEXT NOT NULL,
    sector TEXT,
    total_exposure_crore NUMERIC(14, 2),
    acquisition_price_crore NUMERIC(14, 2),
    sr_outstanding_crore NUMERIC(14, 2),         -- Security Receipts outstanding
    government_guarantee BOOLEAN DEFAULT FALSE,   -- NARCL 85% GoI guarantee
    asset_description TEXT,
    asset_location TEXT,
    asset_type TEXT CHECK (asset_type IN (
        'commercial', 'residential', 'land', 'industrial',
        'hospitality', 'mixed_use', 'financial', 'other'
    )),
    is_mmr BOOLEAN DEFAULT FALSE,
    acquisition_date DATE,
    sr_maturity_date DATE,
    resolution_status TEXT DEFAULT 'under_resolution' CHECK (
        resolution_status IN (
            'acquired', 'under_resolution', 'resolution_plan_approved',
            'sale_process_initiated', 'sold', 'written_off'
        )
    ),
    estimated_realisation_crore NUMERIC(14, 2),
    source_event_id UUID REFERENCES distress_events(id) ON DELETE SET NULL,
    source_url TEXT,
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- INVESTOR_MANDATES — track PE / family office buy criteria
-- Match incoming deal flow against investor mandates automatically
-- ============================================================
CREATE TABLE IF NOT EXISTS investor_mandates (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    investor_name TEXT NOT NULL,
    investor_type TEXT CHECK (investor_type IN (
        'pe_fund', 'family_office', 'reit', 'sovereign_wealth',
        'hni', 'international_fund', 'domestic_fund', 'other'
    )),
    domicile TEXT,                          -- India, London, Singapore, Dubai etc.

    -- Investment criteria
    target_asset_class TEXT[],              -- ['grade_a_office', 'grade_b_office']
    target_micro_markets TEXT[],            -- ['BKC', 'Lower Parel', 'Powai']
    min_deal_size_crore NUMERIC(10, 2),
    max_deal_size_crore NUMERIC(10, 2),
    min_cap_rate_pct NUMERIC(5, 2),
    target_irr_pct NUMERIC(5, 2),
    preferred_hold_years INTEGER DEFAULT 10,
    requires_blue_chip_tenant BOOLEAN DEFAULT FALSE,
    accepts_strata BOOLEAN DEFAULT FALSE,
    accepts_npa_asset BOOLEAN DEFAULT TRUE,

    -- Contact
    primary_contact TEXT,
    contact_email TEXT,
    contact_phone TEXT,
    last_contacted DATE,
    relationship_status TEXT DEFAULT 'cold' CHECK (relationship_status IN (
        'cold', 'warm', 'active', 'loi_stage', 'closed'
    )),

    -- Notes
    notes TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Seed: the London investor group from meeting
INSERT INTO investor_mandates (
    investor_name, investor_type, domicile,
    target_asset_class, target_micro_markets,
    min_deal_size_crore, max_deal_size_crore,
    min_cap_rate_pct, target_irr_pct,
    preferred_hold_years,
    requires_blue_chip_tenant, accepts_strata,
    notes, relationship_status
) VALUES (
    'London Investor Group (Portfolio)',
    'international_fund', 'London',
    ARRAY['grade_a_office', 'grade_b_office'],
    ARRAY['BKC', 'Lower Parel', 'Andheri', 'Powai', 'Malad', 'Goregaon'],
    100, 1000,
    8.5, 14.0,
    10,
    FALSE, FALSE,
    'Seeking portfolio of Grade A/B office properties. Cap rate target 8.5–9%. Willing to consider 3-escalation story at 15% per 3 years over 10 years. Introduced via meeting 14-Mar-2026.',
    'warm'
) ON CONFLICT DO NOTHING;

-- ============================================================
-- DEAL_PIPELINE — track all active deals being pursued
-- From identified → closed
-- ============================================================
CREATE TABLE IF NOT EXISTS deal_pipeline (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    deal_name TEXT NOT NULL,
    property_name TEXT,
    property_address TEXT,
    micro_market TEXT,

    -- Deal financials
    deal_size_crore NUMERIC(12, 2),
    asking_price_crore NUMERIC(12, 2),
    negotiated_price_crore NUMERIC(12, 2),
    area_sqft NUMERIC(14, 2),
    price_psf NUMERIC(10, 2),
    cap_rate_pct NUMERIC(5, 2),

    -- Asset details
    asset_class TEXT,
    tenant_name TEXT,
    lease_expiry DATE,
    occupancy_pct NUMERIC(5, 2),

    -- Parties
    seller_name TEXT,
    seller_type TEXT,
    buyer_name TEXT,
    buyer_type TEXT,

    -- Pipeline status
    stage TEXT DEFAULT 'identified' CHECK (stage IN (
        'identified',       -- sourced, not yet approached
        'approached',       -- initial conversation done
        'term_sheet',       -- non-binding term sheet exchanged
        'loi_signed',       -- LOI executed
        'due_diligence',    -- DD in progress
        'negotiation',      -- final price negotiation
        'documentation',    -- legal docs being drafted
        'closed',           -- transaction completed
        'dropped'           -- deal fell through
    )),

    -- Action tracking
    next_action TEXT,
    next_action_date DATE,
    assigned_to TEXT,

    -- Source intel
    source_channel TEXT,                    -- how deal was sourced
    source_event_id UUID REFERENCES distress_events(id) ON DELETE SET NULL,
    mandate_id UUID REFERENCES investor_mandates(id) ON DELETE SET NULL,

    -- Compliance (from meeting)
    cash_component BOOLEAN DEFAULT FALSE,
    team_is_deal_maker_only BOOLEAN DEFAULT TRUE,  -- not handling cash
    pml_compliant BOOLEAN DEFAULT TRUE,

    priority TEXT DEFAULT 'normal' CHECK (priority IN ('low', 'normal', 'high', 'urgent')),
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Seed active deals from meeting
INSERT INTO deal_pipeline (
    deal_name, property_name, micro_market,
    deal_size_crore, asset_class, tenant_name,
    seller_type, stage, priority,
    next_action, next_action_date, assigned_to,
    notes, source_channel
) VALUES
(
    'Lotus Building — Strata Sale',
    'Lotus Building', 'Worli/Prabhadevi',
    150, 'grade_b_office', 'Vacant / Strata',
    'developer', 'approached', 'high',
    'Connect interested investors to Lotus building for 150Cr transaction',
    '2026-03-21', 'Pranav',
    'Seven floors. Strata complications. Two additional floors possible. Vacant for long time. Need right person to manage strata issues. Leasing easier than sale once resolved. Mandate: London Investor Group.',
    'direct'
),
(
    'Kurla Commercial — 40,000 sqft',
    'Kurla Property', 'Kurla',
    NULL, 'grade_b_office', 'TBD',
    'unknown', 'identified', 'high',
    'Share basic details: area, photos, location. Monday site inspection.',
    '2026-03-16', 'Pranav',
    '40,000 sqft Kurla. Connect with DTDC client (Rish). Site inspection Monday 16-Mar-2026.',
    'referral'
),
(
    'Malad Pre-Leased — Clean Deal',
    'Malad Office Property', 'Malad',
    NULL, 'grade_a_office', 'Reputed Institution',
    'unknown', 'identified', 'normal',
    'Explore Malad property — clean and easy to execute',
    '2026-03-21', 'Pranav',
    'Pre-leased to reputed institution. Cleaner than Lotus. No strata complications. Grade A suburban — cap rate likely 7.5%+.',
    'direct'
)
ON CONFLICT DO NOTHING;

-- ============================================================
-- NEW INDEXES
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_pre_leased_micro_market ON pre_leased_assets(micro_market);
CREATE INDEX IF NOT EXISTS idx_pre_leased_cap_rate ON pre_leased_assets(cap_rate_pct DESC);
CREATE INDEX IF NOT EXISTS idx_pre_leased_meets_threshold ON pre_leased_assets(meets_investor_threshold);
CREATE INDEX IF NOT EXISTS idx_pre_leased_deal_score ON pre_leased_assets(deal_score DESC);
CREATE INDEX IF NOT EXISTS idx_drt_bench ON drt_cases(drt_bench);
CREATE INDEX IF NOT EXISTS idx_drt_status ON drt_cases(case_status);
CREATE INDEX IF NOT EXISTS idx_drt_mmr ON drt_cases(is_mmr);
CREATE INDEX IF NOT EXISTS idx_arc_entity ON arc_portfolio(arc_entity);
CREATE INDEX IF NOT EXISTS idx_arc_status ON arc_portfolio(resolution_status);
CREATE INDEX IF NOT EXISTS idx_pipeline_stage ON deal_pipeline(stage);
CREATE INDEX IF NOT EXISTS idx_pipeline_priority ON deal_pipeline(priority);
CREATE INDEX IF NOT EXISTS idx_distress_channel ON distress_events(channel);
CREATE INDEX IF NOT EXISTS idx_distress_is_mmr ON distress_events(is_mmr);
CREATE INDEX IF NOT EXISTS idx_distress_deal_score ON distress_events(deal_score DESC);

-- ============================================================
-- RLS for new tables
-- ============================================================
ALTER TABLE pre_leased_assets ENABLE ROW LEVEL SECURITY;
ALTER TABLE cap_rate_snapshots ENABLE ROW LEVEL SECURITY;
ALTER TABLE drt_cases ENABLE ROW LEVEL SECURITY;
ALTER TABLE arc_portfolio ENABLE ROW LEVEL SECURITY;
ALTER TABLE investor_mandates ENABLE ROW LEVEL SECURITY;
ALTER TABLE deal_pipeline ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Public read pre_leased_assets" ON pre_leased_assets FOR SELECT USING (true);
CREATE POLICY "Public read cap_rate_snapshots" ON cap_rate_snapshots FOR SELECT USING (true);
CREATE POLICY "Public read drt_cases" ON drt_cases FOR SELECT USING (true);
CREATE POLICY "Public read arc_portfolio" ON arc_portfolio FOR SELECT USING (true);
CREATE POLICY "Public read investor_mandates" ON investor_mandates FOR SELECT USING (true);
CREATE POLICY "Public read deal_pipeline" ON deal_pipeline FOR SELECT USING (true);

CREATE POLICY "Anon insert pre_leased_assets" ON pre_leased_assets FOR INSERT WITH CHECK (true);
CREATE POLICY "Anon insert drt_cases" ON drt_cases FOR INSERT WITH CHECK (true);
CREATE POLICY "Anon insert arc_portfolio" ON arc_portfolio FOR INSERT WITH CHECK (true);
CREATE POLICY "Anon insert deal_pipeline" ON deal_pipeline FOR INSERT WITH CHECK (true);
CREATE POLICY "Anon upsert pre_leased_assets" ON pre_leased_assets FOR UPDATE USING (true);
CREATE POLICY "Anon upsert drt_cases" ON drt_cases FOR UPDATE USING (true);
CREATE POLICY "Anon upsert deal_pipeline" ON deal_pipeline FOR UPDATE USING (true);
CREATE POLICY "Anon insert cap_rate_snapshots" ON cap_rate_snapshots FOR INSERT WITH CHECK (true);

-- ============================================================
-- UPGRADED VIEWS
-- ============================================================

-- High-priority deal signals (MMR commercial, deal_score > 50)
CREATE OR REPLACE VIEW hot_deal_signals AS
SELECT
    de.id,
    de.company_name,
    de.signal_keyword,
    de.signal_category,
    de.source,
    de.url,
    de.headline,
    de.detected_at,
    de.severity,
    de.deal_score,
    de.price_crore,
    de.location,
    de.asset_class,
    de.channel,
    de.metadata
FROM distress_events de
WHERE de.is_mmr = TRUE
  AND de.asset_class = 'commercial'
  AND de.deal_score >= 50
  AND de.is_duplicate = FALSE
  AND de.detected_at > NOW() - INTERVAL '30 days'
ORDER BY de.deal_score DESC, de.detected_at DESC;

-- Pre-leased assets meeting investor threshold
CREATE OR REPLACE VIEW investor_ready_assets AS
SELECT
    pla.*,
    cr.cap_rate_pct AS market_cap_rate,
    cr.avg_rent_psf AS benchmark_rent
FROM pre_leased_assets pla
LEFT JOIN cap_rate_snapshots cr ON (
    cr.micro_market = pla.micro_market AND
    cr.asset_class = pla.asset_class
)
WHERE pla.status IN ('identified', 'under_review', 'in_discussion')
ORDER BY pla.deal_score DESC, pla.cap_rate_pct DESC;

-- Active deal pipeline with investor mandate match
CREATE OR REPLACE VIEW pipeline_with_mandates AS
SELECT
    dp.*,
    im.investor_name,
    im.investor_type,
    im.min_cap_rate_pct AS investor_min_cap_rate,
    im.target_irr_pct
FROM deal_pipeline dp
LEFT JOIN investor_mandates im ON dp.mandate_id = im.id
WHERE dp.stage NOT IN ('closed', 'dropped')
ORDER BY
    CASE dp.priority
        WHEN 'urgent' THEN 1 WHEN 'high' THEN 2
        WHEN 'normal' THEN 3 ELSE 4
    END,
    dp.next_action_date ASC NULLS LAST;

-- Bank auction MMR commercial only (most actionable)
CREATE OR REPLACE VIEW mmr_commercial_auctions AS
SELECT
    de.id,
    de.company_name,
    de.headline,
    de.url,
    de.detected_at,
    de.price_crore,
    de.location,
    (de.metadata->>'bank') AS bank,
    (de.metadata->>'auction_date') AS auction_date,
    (de.metadata->>'deal_score')::INTEGER AS deal_score
FROM distress_events de
WHERE de.channel = 'bank_auction'
  AND de.is_mmr = TRUE
  AND de.asset_class = 'commercial'
  AND de.is_duplicate = FALSE
  AND de.detected_at > NOW() - INTERVAL '60 days'
ORDER BY
    (de.metadata->>'deal_score')::INTEGER DESC NULLS LAST,
    de.detected_at DESC;
