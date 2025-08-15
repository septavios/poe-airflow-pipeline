#!/usr/bin/env python3
"""
Database initialization script for Path of Exile market data pipeline.
This script creates the database schema and sets up initial configurations.
"""

import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'postgres'),
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'user': os.getenv('POSTGRES_USER', 'airflow'),
    'password': os.getenv('POSTGRES_PASSWORD', 'airflow'),
    'database': os.getenv('POSTGRES_DB', 'airflow')
}

def create_database_connection(database=None):
    """Create a database connection."""
    config = DB_CONFIG.copy()
    if database:
        config['database'] = database
    
    try:
        conn = psycopg2.connect(**config)
        return conn
    except psycopg2.Error as e:
        logger.error(f"Error connecting to database: {e}")
        raise

def execute_sql_file(conn, sql_file_path):
    """Execute SQL commands from a file."""
    try:
        with open(sql_file_path, 'r') as file:
            sql_content = file.read()
        
        cursor = conn.cursor()
        cursor.execute(sql_content)
        conn.commit()
        cursor.close()
        logger.info(f"Successfully executed SQL file: {sql_file_path}")
        
    except Exception as e:
        logger.error(f"Error executing SQL file {sql_file_path}: {e}")
        conn.rollback()
        raise

def check_table_exists(conn, table_name):
    """Check if a table exists in the database."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = %s
        );
    """, (table_name,))
    
    exists = cursor.fetchone()[0]
    cursor.close()
    return exists

def get_table_count(conn, table_name):
    """Get the number of records in a table."""
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
    count = cursor.fetchone()[0]
    cursor.close()
    return count

def initialize_database():
    """Initialize the database with the required schema."""
    logger.info("Starting database initialization...")
    
    # Get the SQL file path
    script_dir = Path(__file__).parent
    sql_file = script_dir.parent / 'sql' / 'init_database.sql'
    
    if not sql_file.exists():
        logger.error(f"SQL file not found: {sql_file}")
        return False
    
    try:
        # Connect to the database
        conn = create_database_connection()
        logger.info(f"Connected to database: {DB_CONFIG['database']}")
        
        # Execute the initialization SQL
        execute_sql_file(conn, sql_file)
        
        # Verify tables were created
        tables_to_check = [
            'currency_data',
            'skill_gems_data', 
            'divination_cards_data',
            'unique_items_data',
            'market_summary',
            'profit_opportunities',
            'extraction_log'
        ]
        
        logger.info("Verifying table creation...")
        for table in tables_to_check:
            if check_table_exists(conn, table):
                logger.info(f"✓ Table '{table}' created successfully")
            else:
                logger.error(f"✗ Table '{table}' was not created")
                return False
        
        # Check views
        cursor = conn.cursor()
        cursor.execute("""
            SELECT viewname FROM pg_views 
            WHERE schemaname = 'public' 
            AND viewname IN ('latest_currency_prices', 'latest_gem_prices', 'price_trends_24h');
        """)
        views = cursor.fetchall()
        cursor.close()
        
        logger.info(f"Created {len(views)} views: {[v[0] for v in views]}")
        
        conn.close()
        logger.info("Database initialization completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        return False

def reset_database():
    """Reset the database by dropping and recreating all tables."""
    logger.warning("Resetting database - this will delete all data!")
    
    try:
        conn = create_database_connection()
        cursor = conn.cursor()
        
        # Drop all tables
        tables_to_drop = [
            'extraction_log',
            'profit_opportunities', 
            'market_summary',
            'unique_items_data',
            'divination_cards_data',
            'skill_gems_data',
            'currency_data'
        ]
        
        for table in tables_to_drop:
            cursor.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
            logger.info(f"Dropped table: {table}")
        
        # Drop views
        views_to_drop = ['latest_currency_prices', 'latest_gem_prices', 'price_trends_24h']
        for view in views_to_drop:
            cursor.execute(f"DROP VIEW IF EXISTS {view} CASCADE;")
            logger.info(f"Dropped view: {view}")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        # Reinitialize
        return initialize_database()
        
    except Exception as e:
        logger.error(f"Database reset failed: {e}")
        return False

def show_database_status():
    """Show the current status of the database."""
    try:
        conn = create_database_connection()
        
        tables = [
            'currency_data',
            'skill_gems_data',
            'divination_cards_data', 
            'unique_items_data',
            'market_summary',
            'profit_opportunities',
            'extraction_log'
        ]
        
        logger.info("Database Status:")
        logger.info("=" * 50)
        
        for table in tables:
            if check_table_exists(conn, table):
                count = get_table_count(conn, table)
                logger.info(f"✓ {table:<25} | {count:>10} records")
            else:
                logger.info(f"✗ {table:<25} | Not found")
        
        conn.close()
        
    except Exception as e:
        logger.error(f"Error checking database status: {e}")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == "init":
            success = initialize_database()
            sys.exit(0 if success else 1)
        elif command == "reset":
            success = reset_database()
            sys.exit(0 if success else 1)
        elif command == "status":
            show_database_status()
            sys.exit(0)
        else:
            print("Usage: python init_db.py [init|reset|status]")
            sys.exit(1)
    else:
        # Default action
        success = initialize_database()
        sys.exit(0 if success else 1)