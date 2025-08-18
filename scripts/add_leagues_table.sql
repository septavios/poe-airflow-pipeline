-- Migration script to add leagues table to existing database
-- Run this script to add dynamic league management to your POE Market Dashboard

-- Create leagues table
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

-- Create indexes for leagues table
CREATE INDEX IF NOT EXISTS idx_poe_leagues_name ON poe_leagues(league_name);
CREATE INDEX IF NOT EXISTS idx_poe_leagues_active ON poe_leagues(is_active);

-- Insert default leagues
INSERT INTO poe_leagues (league_name, display_name, is_active, description) VALUES
('Necropolis', 'Necropolis', false, 'Necropolis Challenge League'),
('Crucible', 'Crucible', false, 'Crucible Challenge League'),
('Sanctum', 'Sanctum', false, 'Sanctum Challenge League'),
('Kalandra', 'Kalandra', false, 'Lake of Kalandra Challenge League'),
('mercenaries', 'Mercenaries', true, 'Mercenaries Challenge League')
ON CONFLICT (league_name) DO NOTHING;

-- Update trigger function for updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger for automatic updated_at updates
CREATE TRIGGER update_poe_leagues_updated_at 
    BEFORE UPDATE ON poe_leagues 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();

SELECT 'Leagues table migration completed successfully!' as message;