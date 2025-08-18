-- Add chaos pricing fields to poe_currency_analytics table
-- This script adds the new chaos_buy_price and chaos_sell_price columns
-- to support chaos-normalized pricing analysis

-- Add chaos pricing columns if they don't exist
ALTER TABLE poe_currency_analytics 
ADD COLUMN IF NOT EXISTS chaos_buy_price DECIMAL(15,8);

ALTER TABLE poe_currency_analytics 
ADD COLUMN IF NOT EXISTS chaos_sell_price DECIMAL(15,8);

-- Create indexes for the new chaos pricing columns
CREATE INDEX IF NOT EXISTS idx_currency_analytics_chaos_buy_price 
    ON poe_currency_analytics(chaos_buy_price);

CREATE INDEX IF NOT EXISTS idx_currency_analytics_chaos_sell_price 
    ON poe_currency_analytics(chaos_sell_price);

-- Update the currency_market_analysis view to include chaos pricing
CREATE OR REPLACE VIEW currency_market_analysis AS
SELECT 
    currency_name,
    league,
    extracted_at,
    buy_price,
    sell_price,
    chaos_buy_price,
    chaos_sell_price,
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
        WHEN chaos_buy_price > 1000 THEN 'High Value'
        WHEN chaos_buy_price > 100 THEN 'Medium Value'
        WHEN chaos_buy_price > 10 THEN 'Low Value'
        ELSE 'Micro Value'
    END as price_category
FROM poe_currency_analytics
WHERE extracted_at >= NOW() - INTERVAL '7 days';

-- Update the materialized view to include chaos pricing metrics
DROP MATERIALIZED VIEW IF EXISTS currency_hourly_metrics;

CREATE MATERIALIZED VIEW currency_hourly_metrics AS
SELECT 
    currency_name,
    league,
    DATE_TRUNC('hour', extracted_at) as hour_bucket,
    AVG(buy_price) as avg_buy_price,
    AVG(sell_price) as avg_sell_price,
    AVG(chaos_buy_price) as avg_chaos_buy_price,
    AVG(chaos_sell_price) as avg_chaos_sell_price,
    AVG(price_gap_percentage) as avg_spread,
    SUM(pay_count + receive_count) as total_volume,
    AVG(liquidity_score) as avg_liquidity,
    STDDEV(chaos_buy_price) as chaos_price_volatility,
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

-- Add comments for the new columns
COMMENT ON COLUMN poe_currency_analytics.chaos_buy_price IS 'Buy price normalized to chaos orb equivalent';
COMMENT ON COLUMN poe_currency_analytics.chaos_sell_price IS 'Sell price normalized to chaos orb equivalent';

PRINT 'Successfully added chaos pricing fields to poe_currency_analytics table';