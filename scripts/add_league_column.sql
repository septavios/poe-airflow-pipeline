-- Migration script to add league column to existing tables
-- This script adds a league column to track which POE league the data belongs to

-- Add league column to poe_currency_data table
ALTER TABLE poe_currency_data 
ADD COLUMN IF NOT EXISTS league VARCHAR(50) NOT NULL DEFAULT 'Unknown';

-- Add league column to poe_skill_gems_data table
ALTER TABLE poe_skill_gems_data 
ADD COLUMN IF NOT EXISTS league VARCHAR(50) NOT NULL DEFAULT 'Unknown';

-- Add league column to poe_divination_cards_data table
ALTER TABLE poe_divination_cards_data 
ADD COLUMN IF NOT EXISTS league VARCHAR(50) NOT NULL DEFAULT 'Unknown';

-- Add league column to poe_unique_items_data table
ALTER TABLE poe_unique_items_data 
ADD COLUMN IF NOT EXISTS league VARCHAR(50) NOT NULL DEFAULT 'Unknown';

-- Add indexes for league column for better query performance
CREATE INDEX IF NOT EXISTS idx_poe_currency_data_league ON poe_currency_data(league);
CREATE INDEX IF NOT EXISTS idx_poe_skill_gems_data_league ON poe_skill_gems_data(league);
CREATE INDEX IF NOT EXISTS idx_poe_divination_cards_data_league ON poe_divination_cards_data(league);
CREATE INDEX IF NOT EXISTS idx_poe_unique_items_data_league ON poe_unique_items_data(league);

-- Create composite indexes for league + extracted_at for time-series queries
CREATE INDEX IF NOT EXISTS idx_poe_currency_data_league_time ON poe_currency_data(league, extracted_at);
CREATE INDEX IF NOT EXISTS idx_poe_skill_gems_data_league_time ON poe_skill_gems_data(league, extracted_at);
CREATE INDEX IF NOT EXISTS idx_poe_divination_cards_data_league_time ON poe_divination_cards_data(league, extracted_at);
CREATE INDEX IF NOT EXISTS idx_poe_unique_items_data_league_time ON poe_unique_items_data(league, extracted_at);

-- Update existing records to set league based on current POE_LEAGUE environment variable
-- Note: This will set all existing records to the current league
-- In production, you might want to handle this differently based on your data retention needs

PRINT 'League column migration completed successfully!';
PRINT 'All existing records have been set to default league: Unknown';
PRINT 'Future data extractions will include the correct league information.';