-- NABCA Data Schema for Supabase
-- Custom schema: nabca (already exists)
-- Run this in Supabase SQL Editor

-- Table 1: Sales Fact Table
CREATE TABLE IF NOT EXISTS nabca.sales_fact (
    id BIGSERIAL PRIMARY KEY,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    report_date DATE NOT NULL,
    report_type TEXT NOT NULL CHECK (report_type IN ('monthly', 'rolling_12')),
    table_source TEXT NOT NULL,
    channel TEXT NOT NULL,
    product_type TEXT NOT NULL,
    geography_type TEXT NOT NULL,
    state_name TEXT,
    volume_9l NUMERIC,
    volume_pct_change NUMERIC,
    dollar_sales NUMERIC,
    dollar_pct_change NUMERIC,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Table 2: Brand Category Data
CREATE TABLE IF NOT EXISTS nabca.brand_category_data (
    id BIGSERIAL PRIMARY KEY,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    report_date DATE NOT NULL,
    report_type TEXT NOT NULL CHECK (report_type IN ('monthly', 'rolling_12')),
    table_source TEXT NOT NULL,
    channel TEXT NOT NULL,
    product_type TEXT NOT NULL,
    geography_type TEXT NOT NULL,
    category TEXT NOT NULL,
    volume_9l NUMERIC,
    volume_pct_change NUMERIC,
    dollar_sales NUMERIC,
    dollar_pct_change NUMERIC,
    price_mix NUMERIC,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Table 3: Commentary
CREATE TABLE IF NOT EXISTS nabca.commentary (
    id BIGSERIAL PRIMARY KEY,
    commentary_id TEXT UNIQUE NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    report_date DATE NOT NULL,
    section TEXT NOT NULL,
    content TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_sales_fact_date ON nabca.sales_fact(year, month, report_type);
CREATE INDEX IF NOT EXISTS idx_sales_fact_state ON nabca.sales_fact(state_name);
CREATE INDEX IF NOT EXISTS idx_sales_fact_source ON nabca.sales_fact(table_source, channel);

CREATE INDEX IF NOT EXISTS idx_brand_category_date ON nabca.brand_category_data(year, month, report_type);
CREATE INDEX IF NOT EXISTS idx_brand_category_category ON nabca.brand_category_data(category);

CREATE INDEX IF NOT EXISTS idx_commentary_date ON nabca.commentary(year, month);
CREATE INDEX IF NOT EXISTS idx_commentary_id ON nabca.commentary(commentary_id);

-- Grant permissions to authenticated users (adjust as needed)
GRANT USAGE ON SCHEMA nabca TO anon, authenticated;
GRANT ALL ON ALL TABLES IN SCHEMA nabca TO anon, authenticated;
GRANT ALL ON ALL SEQUENCES IN SCHEMA nabca TO anon, authenticated;

-- Optional: Enable Row Level Security (RLS)
-- ALTER TABLE nabca.sales_fact ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE nabca.brand_category_data ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE nabca.commentary ENABLE ROW LEVEL SECURITY;

-- Example policies (uncomment if using RLS)
-- CREATE POLICY "Allow public read" ON nabca.sales_fact FOR SELECT USING (true);
-- CREATE POLICY "Allow public read" ON nabca.brand_category_data FOR SELECT USING (true);
-- CREATE POLICY "Allow public read" ON nabca.commentary FOR SELECT USING (true);
