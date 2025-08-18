-- Migration script to add comprehensive currency data fields from POE Ninja API
-- This script adds missing fields to capture all details from the JSON response

-- Add missing fields to poe_currency_data table
ALTER TABLE poe_currency_data 
ADD COLUMN IF NOT EXISTS buy_price DECIMAL(15,8),
ADD COLUMN IF NOT EXISTS sell_price DECIMAL(15,8),
ADD COLUMN IF NOT EXISTS price_gap_absolute DECIMAL(15,8),
ADD COLUMN IF NOT EXISTS price_gap_percentage DECIMAL(10,4),
ADD COLUMN IF NOT EXISTS trade_id VARCHAR(100),
ADD COLUMN IF NOT EXISTS currency_id INTEGER,
ADD COLUMN IF NOT EXISTS league_id INTEGER,
ADD COLUMN IF NOT EXISTS sample_time_utc TIMESTAMP WITH TIME ZONE,
ADD COLUMN IF NOT EXISTS pay_count INTEGER,
ADD COLUMN IF NOT EXISTS receive_count INTEGER,
ADD COLUMN IF NOT EXISTS pay_listing_count INTEGER,
ADD COLUMN IF NOT EXISTS receive_listing_count INTEGER,
ADD COLUMN IF NOT EXISTS total_change_pay DECIMAL(10,4),
ADD COLUMN IF NOT EXISTS total_change_receive DECIMAL(10,4),
ADD COLUMN IF NOT EXISTS total_change_low_confidence_pay DECIMAL(10,4),
ADD COLUMN IF NOT EXISTS total_change_low_confidence_receive DECIMAL(10,4);

-- Add indexes for the new fields to improve query performance
CREATE INDEX IF NOT EXISTS idx_poe_currency_data_buy_price ON poe_currency_data(buy_price);
CREATE INDEX IF NOT EXISTS idx_poe_currency_data_sell_price ON poe_currency_data(sell_price);
CREATE INDEX IF NOT EXISTS idx_poe_currency_data_price_gap_percentage ON poe_currency_data(price_gap_percentage);
CREATE INDEX IF NOT EXISTS idx_poe_currency_data_trade_id ON poe_currency_data(trade_id);
CREATE INDEX IF NOT EXISTS idx_poe_currency_data_currency_id ON poe_currency_data(currency_id);
CREATE INDEX IF NOT EXISTS idx_poe_currency_data_league_id ON poe_currency_data(league_id);
CREATE INDEX IF NOT EXISTS idx_poe_currency_data_sample_time ON poe_currency_data(sample_time_utc);

-- Create a new table to store currency details metadata
CREATE TABLE IF NOT EXISTS poe_currency_details (
    id SERIAL PRIMARY KEY,
    currency_id INTEGER UNIQUE NOT NULL,
    currency_name VARCHAR(100) NOT NULL,
    icon_url TEXT,
    trade_id VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for currency details table
CREATE INDEX IF NOT EXISTS idx_poe_currency_details_currency_id ON poe_currency_details(currency_id);
CREATE INDEX IF NOT EXISTS idx_poe_currency_details_name ON poe_currency_details(currency_name);
CREATE INDEX IF NOT EXISTS idx_poe_currency_details_trade_id ON poe_currency_details(trade_id);

-- Create a view for comprehensive currency data with all details
CREATE OR REPLACE VIEW comprehensive_currency_data AS
SELECT 
    cd.currency_name,
    cd.chaos_value,
    cd.chaos_equivalent,
    cd.buy_price,
    cd.sell_price,
    cd.price_gap_absolute,
    cd.price_gap_percentage,
    cd.count,
    cd.listing_count,
    cd.pay_count,
    cd.receive_count,
    cd.pay_listing_count,
    cd.receive_listing_count,
    cd.icon_url,
    cd.trade_id,
    cd.currency_id,
    cd.league_id,
    cd.league,
    cd.sample_time_utc,
    cd.low_confidence,
    cd.data_point_count,
    cd.includes_secondary,
    cd.total_change_pay,
    cd.total_change_receive,
    cd.total_change_low_confidence_pay,
    cd.total_change_low_confidence_receive,
    cd.trade_info,
    cd.sparkline,
    cd.pay_sparkline,
    cd.receive_sparkline,
    cd.low_confidence_pay_sparkline,
    cd.low_confidence_receive_sparkline,
    cd.extracted_at,
    cd.created_at
FROM poe_currency_data cd
ORDER BY cd.extracted_at DESC, cd.chaos_equivalent DESC;

-- Create a view for latest comprehensive currency prices
CREATE OR REPLACE VIEW latest_comprehensive_currency_prices AS
SELECT DISTINCT ON (currency_name) 
    currency_name,
    chaos_value,
    chaos_equivalent,
    buy_price,
    sell_price,
    price_gap_absolute,
    price_gap_percentage,
    count,
    listing_count,
    pay_count,
    receive_count,
    pay_listing_count,
    receive_listing_count,
    icon_url,
    trade_id,
    currency_id,
    league_id,
    league,
    sample_time_utc,
    low_confidence,
    data_point_count,
    includes_secondary,
    total_change_pay,
    total_change_receive,
    extracted_at
FROM poe_currency_data 
ORDER BY currency_name, extracted_at DESC;

-- Create a view for currency arbitrage opportunities
CREATE OR REPLACE VIEW currency_arbitrage_opportunities AS
SELECT 
    currency_name,
    chaos_equivalent,
    buy_price,
    sell_price,
    price_gap_absolute,
    price_gap_percentage,
    listing_count,
    pay_listing_count,
    receive_listing_count,
    CASE 
        WHEN price_gap_percentage > 10 AND listing_count > 50 THEN 'HIGH'
        WHEN price_gap_percentage > 5 AND listing_count > 20 THEN 'MEDIUM'
        WHEN price_gap_percentage > 2 AND listing_count > 10 THEN 'LOW'
        ELSE 'NONE'
    END as arbitrage_opportunity,
    extracted_at
FROM latest_comprehensive_currency_prices
WHERE price_gap_percentage > 0
ORDER BY price_gap_percentage DESC;

COMMIT;

-- Display success message
SELECT 'Migration completed successfully. Added comprehensive currency data fields.' as status;