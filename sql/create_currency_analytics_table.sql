-- Create optimized currency analytics table for data science analysis
-- This table extracts frequently-used fields from trade_info JSONB into dedicated columns
-- while maintaining a reference to the original data

CREATE TABLE IF NOT EXISTS poe_currency_analytics (
    id SERIAL PRIMARY KEY,
    
    -- Reference to original data
    currency_data_id INTEGER REFERENCES poe_currency_data(id),
    
    -- Basic currency information
    currency_name VARCHAR(100) NOT NULL,
    league VARCHAR(50) NOT NULL,
    extracted_at TIMESTAMP WITH TIME ZONE NOT NULL,
    
    -- Price data (extracted from trade_info)
    buy_price DECIMAL(15,8),
    sell_price DECIMAL(15,8),
    chaos_buy_price DECIMAL(15,8),
    chaos_sell_price DECIMAL(15,8),
    price_gap_absolute DECIMAL(15,8),
    price_gap_percentage DECIMAL(8,4),
    
    -- Volume and liquidity metrics
    pay_count INTEGER,
    receive_count INTEGER,
    pay_listing_count INTEGER,
    receive_listing_count INTEGER,
    total_listing_count INTEGER,
    
    -- Currency IDs for relationship analysis
    pay_currency_id INTEGER,
    receive_currency_id INTEGER,
    currency_id INTEGER,
    trade_id VARCHAR(100),
    
    -- Market confidence and quality indicators
    data_point_count INTEGER,
    includes_secondary BOOLEAN DEFAULT FALSE,
    low_confidence BOOLEAN DEFAULT FALSE,
    
    -- Time-based metrics
    sample_time_utc TIMESTAMP WITH TIME ZONE,
    league_id INTEGER,
    
    -- Change metrics for trend analysis
    total_change_pay DECIMAL(10,4),
    total_change_receive DECIMAL(10,4),
    total_change_low_confidence_pay DECIMAL(10,4),
    total_change_low_confidence_receive DECIMAL(10,4),
    
    -- Calculated metrics for analysis
    liquidity_score DECIMAL(15,4), -- log(listing_count) * price
    market_depth_score DECIMAL(15,4), -- pay_count + receive_count
    volatility_index DECIMAL(10,4), -- price_gap_percentage normalized
    
    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for optimal query performance
CREATE INDEX IF NOT EXISTS idx_currency_analytics_name ON poe_currency_analytics(currency_name);
CREATE INDEX IF NOT EXISTS idx_currency_analytics_league ON poe_currency_analytics(league);
CREATE INDEX IF NOT EXISTS idx_currency_analytics_extracted_at ON poe_currency_analytics(extracted_at);
CREATE INDEX IF NOT EXISTS idx_currency_analytics_buy_price ON poe_currency_analytics(buy_price);
CREATE INDEX IF NOT EXISTS idx_currency_analytics_sell_price ON poe_currency_analytics(sell_price);
CREATE INDEX IF NOT EXISTS idx_currency_analytics_price_gap ON poe_currency_analytics(price_gap_percentage);
CREATE INDEX IF NOT EXISTS idx_currency_analytics_liquidity ON poe_currency_analytics(liquidity_score);
CREATE INDEX IF NOT EXISTS idx_currency_analytics_volume ON poe_currency_analytics(market_depth_score);
CREATE INDEX IF NOT EXISTS idx_currency_analytics_currency_id ON poe_currency_analytics(currency_id);
CREATE INDEX IF NOT EXISTS idx_currency_analytics_pay_currency ON poe_currency_analytics(pay_currency_id);
CREATE INDEX IF NOT EXISTS idx_currency_analytics_receive_currency ON poe_currency_analytics(receive_currency_id);

-- Composite indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_currency_analytics_league_time ON poe_currency_analytics(league, extracted_at);
CREATE INDEX IF NOT EXISTS idx_currency_analytics_name_time ON poe_currency_analytics(currency_name, extracted_at);
CREATE INDEX IF NOT EXISTS idx_currency_analytics_price_volume ON poe_currency_analytics(buy_price, market_depth_score);

-- Create a view for easy data science analysis
CREATE OR REPLACE VIEW currency_market_analysis AS
SELECT 
    currency_name,
    league,
    extracted_at,
    buy_price,
    sell_price,
    price_gap_percentage,
    pay_count + receive_count as total_volume,
    pay_listing_count + receive_listing_count as total_listings,
    liquidity_score,
    market_depth_score,
    volatility_index,
    CASE 
        WHEN low_confidence THEN 'Low'
        WHEN data_point_count < 5 THEN 'Medium'
        ELSE 'High'
    END as confidence_level,
    CASE
        WHEN buy_price > 1000 THEN 'High Value'
        WHEN buy_price > 100 THEN 'Medium Value'
        WHEN buy_price > 10 THEN 'Low Value'
        ELSE 'Micro Value'
    END as price_category
FROM poe_currency_analytics
WHERE extracted_at >= NOW() - INTERVAL '7 days';

-- Create materialized view for hourly aggregations (useful for time-series analysis)
CREATE MATERIALIZED VIEW IF NOT EXISTS currency_hourly_metrics AS
SELECT 
    currency_name,
    league,
    DATE_TRUNC('hour', extracted_at) as hour_bucket,
    AVG(buy_price) as avg_buy_price,
    AVG(sell_price) as avg_sell_price,
    AVG(price_gap_percentage) as avg_spread,
    SUM(pay_count + receive_count) as total_volume,
    AVG(liquidity_score) as avg_liquidity,
    STDDEV(buy_price) as price_volatility,
    COUNT(*) as data_points,
    MIN(extracted_at) as first_update,
    MAX(extracted_at) as last_update
FROM poe_currency_analytics
GROUP BY currency_name, league, DATE_TRUNC('hour', extracted_at)
ORDER BY hour_bucket DESC;

-- Create index on materialized view
CREATE UNIQUE INDEX IF NOT EXISTS idx_currency_hourly_unique 
    ON currency_hourly_metrics(currency_name, league, hour_bucket);
CREATE INDEX IF NOT EXISTS idx_currency_hourly_bucket ON currency_hourly_metrics(hour_bucket);

-- Add comments for documentation
COMMENT ON TABLE poe_currency_analytics IS 'Optimized table for data science analysis with extracted trade_info fields';
COMMENT ON COLUMN poe_currency_analytics.liquidity_score IS 'Calculated as log(total_listing_count) * buy_price';
COMMENT ON COLUMN poe_currency_analytics.market_depth_score IS 'Sum of pay_count and receive_count indicating market activity';
COMMENT ON COLUMN poe_currency_analytics.volatility_index IS 'Normalized price_gap_percentage for volatility analysis';
COMMENT ON VIEW currency_market_analysis IS 'Simplified view for data science analysis with categorized data';
COMMENT ON MATERIALIZED VIEW currency_hourly_metrics IS 'Hourly aggregated metrics for time-series analysis and forecasting';