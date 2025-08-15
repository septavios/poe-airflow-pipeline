-- Path of Exile Market Data Database Schema
-- This schema is designed for time-series analysis of market data

-- Create database (if using PostgreSQL)
-- CREATE DATABASE poe_market_data;

-- Currency data table for tracking exchange rates over time
CREATE TABLE IF NOT EXISTS currency_data (
    id SERIAL PRIMARY KEY,
    currency_name VARCHAR(100) NOT NULL,
    currency_type VARCHAR(50),
    chaos_value DECIMAL(15,4),
    exalted_value DECIMAL(15,4),
    divine_value DECIMAL(15,4),
    count INTEGER,
    details_id VARCHAR(50),
    icon_url TEXT,
    trade_info JSONB,
    sparkline JSONB,
    low_confidence BOOLEAN DEFAULT FALSE,
    listing_count INTEGER,
    data_hash VARCHAR(32),
    extracted_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Skill gems data table
CREATE TABLE IF NOT EXISTS skill_gems_data (
    id SERIAL PRIMARY KEY,
    gem_name VARCHAR(200) NOT NULL,
    gem_type VARCHAR(50),
    gem_level INTEGER,
    gem_quality INTEGER,
    variant VARCHAR(100),
    chaos_value DECIMAL(15,4),
    exalted_value DECIMAL(15,4),
    divine_value DECIMAL(15,4),
    count INTEGER,
    details_id VARCHAR(50),
    icon_url TEXT,
    trade_info JSONB,
    sparkline JSONB,
    low_confidence BOOLEAN DEFAULT FALSE,
    listing_count INTEGER,
    corrupted BOOLEAN DEFAULT FALSE,
    data_hash VARCHAR(32),
    extracted_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Divination cards data table
CREATE TABLE IF NOT EXISTS divination_cards_data (
    id SERIAL PRIMARY KEY,
    card_name VARCHAR(200) NOT NULL,
    chaos_value DECIMAL(15,4),
    exalted_value DECIMAL(15,4),
    divine_value DECIMAL(15,4),
    count INTEGER,
    details_id VARCHAR(50),
    icon_url TEXT,
    trade_info JSONB,
    sparkline JSONB,
    low_confidence BOOLEAN DEFAULT FALSE,
    listing_count INTEGER,
    stack_size INTEGER,
    data_hash VARCHAR(32),
    extracted_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Unique items data table
CREATE TABLE IF NOT EXISTS unique_items_data (
    id SERIAL PRIMARY KEY,
    item_name VARCHAR(200) NOT NULL,
    item_type VARCHAR(100),
    base_type VARCHAR(100),
    variant VARCHAR(100),
    chaos_value DECIMAL(15,4),
    exalted_value DECIMAL(15,4),
    divine_value DECIMAL(15,4),
    count INTEGER,
    details_id VARCHAR(50),
    icon_url TEXT,
    trade_info JSONB,
    sparkline JSONB,
    low_confidence BOOLEAN DEFAULT FALSE,
    listing_count INTEGER,
    links INTEGER,
    item_level INTEGER,
    corrupted BOOLEAN DEFAULT FALSE,
    data_hash VARCHAR(32),
    extracted_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Market summary table for aggregated analysis
CREATE TABLE IF NOT EXISTS market_summary (
    id SERIAL PRIMARY KEY,
    summary_date DATE NOT NULL,
    total_currencies INTEGER,
    total_gems INTEGER,
    total_cards INTEGER,
    total_unique_items INTEGER,
    avg_currency_chaos_value DECIMAL(15,4),
    avg_gem_chaos_value DECIMAL(15,4),
    avg_card_chaos_value DECIMAL(15,4),
    avg_unique_chaos_value DECIMAL(15,4),
    high_value_currencies INTEGER,
    high_value_gems INTEGER,
    high_value_cards INTEGER,
    most_liquid_currency VARCHAR(100),
    most_liquid_gem VARCHAR(200),
    most_liquid_card VARCHAR(200),
    market_volatility DECIMAL(10,4),
    summary_data JSONB,
    extracted_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(summary_date, extracted_at)
);

-- Profit opportunities table
CREATE TABLE IF NOT EXISTS profit_opportunities (
    id SERIAL PRIMARY KEY,
    opportunity_type VARCHAR(50) NOT NULL, -- 'currency', 'gem', 'card', 'unique'
    item_name VARCHAR(200) NOT NULL,
    item_variant VARCHAR(100),
    current_chaos_value DECIMAL(15,4),
    historical_avg_chaos_value DECIMAL(15,4),
    profit_percentage DECIMAL(10,4),
    confidence_score DECIMAL(5,4),
    listing_count INTEGER,
    recommendation VARCHAR(20), -- 'BUY', 'SELL', 'HOLD'
    risk_level VARCHAR(20), -- 'LOW', 'MEDIUM', 'HIGH'
    analysis_data JSONB,
    extracted_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Data extraction log table
CREATE TABLE IF NOT EXISTS extraction_log (
    id SERIAL PRIMARY KEY,
    extraction_type VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL, -- 'SUCCESS', 'FAILED', 'PARTIAL'
    records_processed INTEGER,
    error_message TEXT,
    execution_time_seconds DECIMAL(10,3),
    extracted_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_currency_data_extracted_at ON currency_data(extracted_at);
CREATE INDEX IF NOT EXISTS idx_currency_data_name ON currency_data(currency_name);
CREATE INDEX IF NOT EXISTS idx_currency_data_chaos_value ON currency_data(chaos_value);

CREATE INDEX IF NOT EXISTS idx_skill_gems_extracted_at ON skill_gems_data(extracted_at);
CREATE INDEX IF NOT EXISTS idx_skill_gems_name ON skill_gems_data(gem_name);
CREATE INDEX IF NOT EXISTS idx_skill_gems_chaos_value ON skill_gems_data(chaos_value);

CREATE INDEX IF NOT EXISTS idx_divination_cards_extracted_at ON divination_cards_data(extracted_at);
CREATE INDEX IF NOT EXISTS idx_divination_cards_name ON divination_cards_data(card_name);
CREATE INDEX IF NOT EXISTS idx_divination_cards_chaos_value ON divination_cards_data(chaos_value);

CREATE INDEX IF NOT EXISTS idx_unique_items_extracted_at ON unique_items_data(extracted_at);
CREATE INDEX IF NOT EXISTS idx_unique_items_name ON unique_items_data(item_name);
CREATE INDEX IF NOT EXISTS idx_unique_items_chaos_value ON unique_items_data(chaos_value);

CREATE INDEX IF NOT EXISTS idx_market_summary_date ON market_summary(summary_date);
CREATE INDEX IF NOT EXISTS idx_profit_opportunities_extracted_at ON profit_opportunities(extracted_at);
CREATE INDEX IF NOT EXISTS idx_profit_opportunities_type ON profit_opportunities(opportunity_type);
CREATE INDEX IF NOT EXISTS idx_extraction_log_extracted_at ON extraction_log(extracted_at);

-- Create views for common queries
CREATE OR REPLACE VIEW latest_currency_prices AS
SELECT DISTINCT ON (currency_name) 
    currency_name,
    chaos_value,
    exalted_value,
    divine_value,
    count,
    listing_count,
    extracted_at
FROM currency_data 
ORDER BY currency_name, extracted_at DESC;

CREATE OR REPLACE VIEW latest_gem_prices AS
SELECT DISTINCT ON (gem_name, gem_level, gem_quality, variant) 
    gem_name,
    gem_level,
    gem_quality,
    variant,
    chaos_value,
    exalted_value,
    divine_value,
    count,
    listing_count,
    extracted_at
FROM skill_gems_data 
ORDER BY gem_name, gem_level, gem_quality, variant, extracted_at DESC;

CREATE OR REPLACE VIEW price_trends_24h AS
SELECT 
    'currency' as item_type,
    currency_name as item_name,
    AVG(chaos_value) as avg_chaos_value,
    MIN(chaos_value) as min_chaos_value,
    MAX(chaos_value) as max_chaos_value,
    COUNT(*) as data_points
FROM currency_data 
WHERE extracted_at >= NOW() - INTERVAL '24 hours'
GROUP BY currency_name
UNION ALL
SELECT 
    'gem' as item_type,
    gem_name as item_name,
    AVG(chaos_value) as avg_chaos_value,
    MIN(chaos_value) as min_chaos_value,
    MAX(chaos_value) as max_chaos_value,
    COUNT(*) as data_points
FROM skill_gems_data 
WHERE extracted_at >= NOW() - INTERVAL '24 hours'
GROUP BY gem_name;

-- Grant permissions (adjust as needed)
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow_user;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO airflow_user;