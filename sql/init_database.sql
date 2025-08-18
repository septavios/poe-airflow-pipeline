-- Path of Exile Market Data Database Schema
-- This schema is designed for time-series analysis of market data

-- Create database (if using PostgreSQL)
-- CREATE DATABASE poe_market_data;

-- Leagues table to store available Path of Exile leagues
CREATE TABLE IF NOT EXISTS poe_leagues (
    id SERIAL PRIMARY KEY,
    league_name VARCHAR(100) NOT NULL UNIQUE,
    display_name VARCHAR(100) NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    start_date DATE,
    end_date DATE,
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Insert default leagues
INSERT INTO poe_leagues (league_name, display_name, is_active, description) VALUES
('Necropolis', 'Necropolis', true, 'Necropolis Challenge League'),
('Crucible', 'Crucible', false, 'Crucible Challenge League'),
('Sanctum', 'Sanctum', false, 'Sanctum Challenge League'),
('Kalandra', 'Kalandra', false, 'Lake of Kalandra Challenge League'),
('mercenaries', 'Mercenaries', true, 'Mercenaries Challenge League')
ON CONFLICT (league_name) DO NOTHING;

-- Currency data table for tracking exchange rates over time
CREATE TABLE IF NOT EXISTS poe_currency_data (
    id SERIAL PRIMARY KEY,
    currency_name VARCHAR(100) NOT NULL,
    currency_type VARCHAR(50),
    chaos_value DECIMAL(15,4),
    exalted_value DECIMAL(15,4),
    divine_value DECIMAL(15,4),
    chaos_equivalent DECIMAL(15,4),
    count INTEGER,
    details_id VARCHAR(50),
    icon_url TEXT,
    trade_info JSONB,
    sparkline JSONB,
    pay_sparkline JSONB,
    receive_sparkline JSONB,
    low_confidence_pay_sparkline JSONB,
    low_confidence_receive_sparkline JSONB,
    low_confidence BOOLEAN DEFAULT FALSE,
    listing_count INTEGER,
    data_point_count INTEGER,
    includes_secondary BOOLEAN DEFAULT FALSE,
    data_hash VARCHAR(32),
    league VARCHAR(50) NOT NULL,
    extracted_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Skill gems data table
CREATE TABLE IF NOT EXISTS poe_skill_gems_data (
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
    league VARCHAR(50) NOT NULL,
    extracted_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Divination cards data table
CREATE TABLE IF NOT EXISTS poe_divination_cards_data (
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
    league VARCHAR(50) NOT NULL,
    extracted_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Unique items data table
CREATE TABLE IF NOT EXISTS poe_unique_items_data (
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
    league VARCHAR(50) NOT NULL,
    extracted_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Market summary table for aggregated analysis
CREATE TABLE IF NOT EXISTS poe_market_summary (
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
CREATE TABLE IF NOT EXISTS poe_profit_opportunities (
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
CREATE TABLE IF NOT EXISTS poe_extraction_log (
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
-- Leagues table indexes
CREATE INDEX IF NOT EXISTS idx_poe_leagues_name ON poe_leagues(league_name);
CREATE INDEX IF NOT EXISTS idx_poe_leagues_active ON poe_leagues(is_active);

CREATE INDEX IF NOT EXISTS idx_poe_currency_data_extracted_at ON poe_currency_data(extracted_at);
CREATE INDEX IF NOT EXISTS idx_poe_currency_data_name ON poe_currency_data(currency_name);
CREATE INDEX IF NOT EXISTS idx_poe_currency_data_chaos_value ON poe_currency_data(chaos_value);

CREATE INDEX IF NOT EXISTS idx_poe_skill_gems_extracted_at ON poe_skill_gems_data(extracted_at);
CREATE INDEX IF NOT EXISTS idx_poe_skill_gems_name ON poe_skill_gems_data(gem_name);
CREATE INDEX IF NOT EXISTS idx_poe_skill_gems_chaos_value ON poe_skill_gems_data(chaos_value);

CREATE INDEX IF NOT EXISTS idx_poe_divination_cards_extracted_at ON poe_divination_cards_data(extracted_at);
CREATE INDEX IF NOT EXISTS idx_poe_divination_cards_name ON poe_divination_cards_data(card_name);
CREATE INDEX IF NOT EXISTS idx_poe_divination_cards_chaos_value ON poe_divination_cards_data(chaos_value);

CREATE INDEX IF NOT EXISTS idx_poe_unique_items_extracted_at ON poe_unique_items_data(extracted_at);
CREATE INDEX IF NOT EXISTS idx_poe_unique_items_name ON poe_unique_items_data(item_name);
CREATE INDEX IF NOT EXISTS idx_poe_unique_items_chaos_value ON poe_unique_items_data(chaos_value);

CREATE INDEX IF NOT EXISTS idx_poe_market_summary_date ON poe_market_summary(summary_date);
CREATE INDEX IF NOT EXISTS idx_poe_profit_opportunities_extracted_at ON poe_profit_opportunities(extracted_at);
CREATE INDEX IF NOT EXISTS idx_poe_profit_opportunities_type ON poe_profit_opportunities(opportunity_type);
CREATE INDEX IF NOT EXISTS idx_poe_extraction_log_extracted_at ON poe_extraction_log(extracted_at);

-- League-specific indexes for filtering data by league
CREATE INDEX IF NOT EXISTS idx_poe_currency_data_league ON poe_currency_data(league);
CREATE INDEX IF NOT EXISTS idx_poe_skill_gems_data_league ON poe_skill_gems_data(league);
CREATE INDEX IF NOT EXISTS idx_poe_divination_cards_data_league ON poe_divination_cards_data(league);
CREATE INDEX IF NOT EXISTS idx_poe_unique_items_data_league ON poe_unique_items_data(league);

-- Composite indexes for league + time-series queries
CREATE INDEX IF NOT EXISTS idx_poe_currency_data_league_time ON poe_currency_data(league, extracted_at);
CREATE INDEX IF NOT EXISTS idx_poe_skill_gems_data_league_time ON poe_skill_gems_data(league, extracted_at);
CREATE INDEX IF NOT EXISTS idx_poe_divination_cards_data_league_time ON poe_divination_cards_data(league, extracted_at);
CREATE INDEX IF NOT EXISTS idx_poe_unique_items_data_league_time ON poe_unique_items_data(league, extracted_at);

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
FROM poe_currency_data 
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
FROM poe_skill_gems_data 
ORDER BY gem_name, gem_level, gem_quality, variant, extracted_at DESC;

CREATE OR REPLACE VIEW price_trends_24h AS
SELECT 
    'currency' as item_type,
    currency_name as item_name,
    AVG(chaos_value) as avg_chaos_value,
    MIN(chaos_value) as min_chaos_value,
    MAX(chaos_value) as max_chaos_value,
    COUNT(*) as data_points
FROM poe_currency_data 
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
FROM poe_skill_gems_data 
WHERE extracted_at >= NOW() - INTERVAL '24 hours'
GROUP BY gem_name;

-- Grant permissions (adjust as needed)
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow_user;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO airflow_user;