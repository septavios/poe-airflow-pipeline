-- Database Optimization Script for POE Market Dashboard
-- This script adds performance optimizations for the currency dashboard

-- Additional indexes for currency data optimization
-- Index for currency name lookups (Divine Orb specifically)
CREATE INDEX IF NOT EXISTS idx_poe_currency_data_name_league_time 
ON poe_currency_data(currency_name, league, extracted_at DESC);

-- Index for chaos_value ordering (most common sort)
CREATE INDEX IF NOT EXISTS idx_poe_currency_data_chaos_value_desc 
ON poe_currency_data(league, extracted_at, chaos_value DESC) 
WHERE extracted_at >= NOW() - INTERVAL '7 days';

-- Index for low_confidence filtering
CREATE INDEX IF NOT EXISTS idx_poe_currency_data_confidence 
ON poe_currency_data(league, low_confidence, extracted_at) 
WHERE extracted_at >= NOW() - INTERVAL '7 days';

-- Partial index for recent data (last 7 days) - most queries use this filter
CREATE INDEX IF NOT EXISTS idx_poe_currency_data_recent 
ON poe_currency_data(league, chaos_value DESC, extracted_at) 
WHERE extracted_at >= NOW() - INTERVAL '7 days';

-- Index for count column (used in sorting)
CREATE INDEX IF NOT EXISTS idx_poe_currency_data_count 
ON poe_currency_data(league, count DESC, extracted_at) 
WHERE extracted_at >= NOW() - INTERVAL '7 days';

-- Materialized view for Divine Orb values (frequently accessed)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_divine_orb_values AS
SELECT 
    league,
    chaos_value as divine_chaos_value,
    extracted_at,
    ROW_NUMBER() OVER (PARTITION BY league ORDER BY extracted_at DESC) as rn
FROM poe_currency_data 
WHERE currency_name = 'Divine Orb'
AND extracted_at >= NOW() - INTERVAL '7 days';

-- Index on the materialized view
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_divine_orb_values_league_rn 
ON mv_divine_orb_values(league, rn);

-- Refresh function for the materialized view
CREATE OR REPLACE FUNCTION refresh_divine_orb_values()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_divine_orb_values;
END;
$$ LANGUAGE plpgsql;

-- Performance monitoring view
CREATE OR REPLACE VIEW currency_query_performance AS
SELECT 
    schemaname,
    tablename,
    attname,
    n_distinct,
    correlation,
    most_common_vals,
    most_common_freqs
FROM pg_stats 
WHERE tablename = 'poe_currency_data'
AND schemaname = 'public';

-- Table statistics update (run periodically)
-- ANALYZE poe_currency_data;

-- Vacuum settings recommendations (add to postgresql.conf)
/*
Recommended PostgreSQL settings for better performance:

# Memory settings
shared_buffers = 256MB
effective_cache_size = 1GB
work_mem = 4MB
maintenance_work_mem = 64MB

# Checkpoint settings
checkpoint_completion_target = 0.9
wal_buffers = 16MB

# Query planner settings
random_page_cost = 1.1
effective_io_concurrency = 200

# Autovacuum settings
autovacuum = on
autovacuum_max_workers = 3
autovacuum_naptime = 1min
autovacuum_vacuum_threshold = 50
autovacuum_analyze_threshold = 50
autovacuum_vacuum_scale_factor = 0.2
autovacuum_analyze_scale_factor = 0.1
*/

-- Query to check index usage
/*
SELECT 
    schemaname,
    tablename,
    indexname,
    idx_tup_read,
    idx_tup_fetch,
    idx_scan
FROM pg_stat_user_indexes 
WHERE tablename = 'poe_currency_data'
ORDER BY idx_scan DESC;
*/

-- Query to find slow queries (enable pg_stat_statements extension)
/*
SELECT 
    query,
    calls,
    total_time,
    mean_time,
    rows
FROM pg_stat_statements 
WHERE query LIKE '%poe_currency_data%'
ORDER BY mean_time DESC
LIMIT 10;
*/