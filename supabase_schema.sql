-- ============================================================
-- NABCA Control States Schema for Supabase
-- ============================================================
--
-- Run this in the Supabase SQL Editor BEFORE the first pipeline run.
--
-- To use a different schema, find-replace 'nabca' with your schema name.
-- Then set SUPABASE_SCHEMA=<your_schema> when running the pipeline.
-- ============================================================

-- Create schema (safe to run if it already exists)
CREATE SCHEMA IF NOT EXISTS nabca;

-- ============================================================
-- Table 1: sales_fact
-- State-level sales data for spirits and wine
-- ============================================================
CREATE TABLE IF NOT EXISTS nabca.sales_fact (
    id              BIGSERIAL PRIMARY KEY,
    year            INTEGER NOT NULL,
    month           INTEGER NOT NULL,
    report_date     DATE NOT NULL,
    report_type     TEXT NOT NULL CHECK (report_type IN ('monthly', 'rolling_12')),
    table_source    TEXT NOT NULL,       -- 'spirits_markets', 'spirits_on_premise', 'wine'
    channel         TEXT NOT NULL,       -- 'total', 'on_premise'
    product_type    TEXT NOT NULL,       -- 'spirits', 'wine'
    geography_type  TEXT NOT NULL,       -- 'state', 'total_control'
    state_name      TEXT,                -- Full state name (NULL for total_control)
    volume_9l       NUMERIC,
    volume_pct_change NUMERIC,
    dollar_sales    NUMERIC,
    dollar_pct_change NUMERIC,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ============================================================
-- Table 2: brand_category_data
-- Category-level spirit breakdown (Vodka, Whiskey, etc.)
-- ============================================================
CREATE TABLE IF NOT EXISTS nabca.brand_category_data (
    id              BIGSERIAL PRIMARY KEY,
    year            INTEGER NOT NULL,
    month           INTEGER NOT NULL,
    report_date     DATE NOT NULL,
    report_type     TEXT NOT NULL CHECK (report_type IN ('monthly', 'rolling_12')),
    table_source    TEXT NOT NULL,       -- Always 'spirits_categories'
    channel         TEXT NOT NULL,       -- Always 'total'
    product_type    TEXT NOT NULL,       -- Always 'spirits'
    geography_type  TEXT NOT NULL,       -- Always 'total_control'
    category        TEXT NOT NULL,       -- VODKA, WHISKEY, TEQUILA, RUM, GIN, etc.
    volume_9l       NUMERIC,
    volume_pct_change NUMERIC,
    dollar_sales    NUMERIC,
    dollar_pct_change NUMERIC,
    price_mix       NUMERIC,            -- Rolling 12 only (NULL for monthly)
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ============================================================
-- Table 3: commentary
-- Narrative text extracted from each monthly report
-- ============================================================
CREATE TABLE IF NOT EXISTS nabca.commentary (
    id              BIGSERIAL PRIMARY KEY,
    commentary_id   TEXT UNIQUE NOT NULL, -- Format: NABCA-{year}-{month:02d}-001
    year            INTEGER NOT NULL,
    month           INTEGER NOT NULL,
    report_date     DATE NOT NULL,
    section         TEXT NOT NULL,        -- Always 'full_report'
    content         TEXT NOT NULL,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ============================================================
-- Indexes
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_sales_fact_date ON nabca.sales_fact(year, month, report_type);
CREATE INDEX IF NOT EXISTS idx_sales_fact_state ON nabca.sales_fact(state_name);
CREATE INDEX IF NOT EXISTS idx_sales_fact_source ON nabca.sales_fact(table_source, channel);

CREATE INDEX IF NOT EXISTS idx_brand_category_date ON nabca.brand_category_data(year, month, report_type);
CREATE INDEX IF NOT EXISTS idx_brand_category_category ON nabca.brand_category_data(category);

CREATE INDEX IF NOT EXISTS idx_commentary_date ON nabca.commentary(year, month);
CREATE INDEX IF NOT EXISTS idx_commentary_id ON nabca.commentary(commentary_id);

-- ============================================================
-- Permissions
-- ============================================================
GRANT USAGE ON SCHEMA nabca TO anon, authenticated;
GRANT ALL ON ALL TABLES IN SCHEMA nabca TO anon, authenticated;
GRANT ALL ON ALL SEQUENCES IN SCHEMA nabca TO anon, authenticated;

-- ============================================================
-- Optional: Row Level Security (uncomment if needed)
-- ============================================================
-- ALTER TABLE nabca.sales_fact ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE nabca.brand_category_data ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE nabca.commentary ENABLE ROW LEVEL SECURITY;
-- CREATE POLICY "Allow public read" ON nabca.sales_fact FOR SELECT USING (true);
-- CREATE POLICY "Allow public read" ON nabca.brand_category_data FOR SELECT USING (true);
-- CREATE POLICY "Allow public read" ON nabca.commentary FOR SELECT USING (true);
