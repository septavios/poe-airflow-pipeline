from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import requests
import pandas as pd
import json
import os
import numpy as np
from typing import Dict, List, Any
import psycopg2
from psycopg2.extras import RealDictCursor
from decimal import Decimal

# Default arguments for the DAG
default_args = {
    'owner': 'data_scientist',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'poe_unified_pipeline',
    default_args=default_args,
    description='Unified Path of Exile data extraction and transformation pipeline',
    schedule=timedelta(hours=6),  # Run every 6 hours
    catchup=False,
    tags=['poe', 'gaming', 'economics', 'data_science', 'unified'],
)

# Configuration
LEAGUE = 'Settlers'  # Current league - update as needed
BASE_URL = 'https://poe.ninja/api/data'
DATA_DIR = '/opt/airflow/logs/poe_data'  # Store data in logs directory
OUTPUT_DIR = '/opt/airflow/logs/poe_analytics'

# Database configuration
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'postgres'),
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'user': os.getenv('POSTGRES_USER', 'airflow'),
    'password': os.getenv('POSTGRES_PASSWORD', 'airflow'),
    'database': os.getenv('POSTGRES_DB', 'airflow')
}

def get_db_connection():
    """Create a database connection"""
    return psycopg2.connect(**DB_CONFIG)

def log_extraction(extraction_type: str, status: str, record_count: int = 0, error_message: str = None):
    """Log extraction activity to database"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO poe_extraction_log (extraction_type, status, records_processed, error_message, extracted_at)
                    VALUES (%s, %s, %s, %s, %s)
                """, (extraction_type, status, record_count, error_message, datetime.now()))
                conn.commit()
    except Exception as e:
        print(f"Error logging extraction: {str(e)}")

def log_transformation(transformation_type: str, records_processed: int, status: str = 'success', error_message: str = None):
    """Log transformation activity to database"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO poe_extraction_log (extraction_type, status, records_processed, error_message, extracted_at)
                    VALUES (%s, %s, %s, %s, %s)
                """, (f"transform_{transformation_type}", status, records_processed, error_message, datetime.now()))
                conn.commit()
    except Exception as e:
        print(f"Failed to log transformation: {e}")

def create_directories():
    """Create directories for storing extracted and transformed data"""
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"Directories created: {DATA_DIR}, {OUTPUT_DIR}")

# ============================================================================
# EXTRACTION FUNCTIONS
# ============================================================================

def fetch_currency_data(**context) -> Dict[str, Any]:
    """Fetch currency exchange rates from poe.ninja and insert into database"""
    url = f"{BASE_URL}/currencyoverview?league={LEAGUE}&type=Currency"
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Extract relevant currency information
        currency_data = []
        for line in data.get('lines', []):
            currency_info = {
                'currency_name': line.get('currencyTypeName'),
                'chaos_value': line.get('chaosEquivalent', 0),
                'details_id': line.get('detailsId'),
                'count': line.get('count', 0),
                'listing_count': line.get('receive', {}).get('listing_count', 0),
                'icon_url': line.get('icon'),
                'trade_info': line.get('pay', {}),
                'sparkline': line.get('sparkline', {}),
                'low_confidence': line.get('lowConfidence', False),
                'timestamp': datetime.now().isoformat()
            }
            currency_data.append(currency_info)
        
        # Insert into database
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                for item in currency_data:
                    cur.execute("""
                        INSERT INTO poe_currency_data 
                        (currency_name, chaos_value, details_id, count, listing_count, icon_url, trade_info, sparkline, low_confidence, extracted_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        item['currency_name'],
                        item['chaos_value'],
                        item['details_id'],
                        item['count'],
                        item['listing_count'],
                        item['icon_url'],
                        json.dumps(item['trade_info']),
                        json.dumps(item['sparkline']),
                        item['low_confidence'],
                        datetime.now()
                    ))
            conn.commit()
        
        # Also save to file for backup
        filename = f"{DATA_DIR}/currency_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, 'w') as f:
            json.dump(currency_data, f, indent=2)
        
        log_extraction('currency_data', 'success', len(currency_data))
        print(f"Currency data saved: {len(currency_data)} items processed to database and {filename}")
        return {'filename': filename, 'total_count': len(currency_data)}
        
    except Exception as e:
        log_extraction('currency_data', 'error', 0, str(e))
        print(f"Error fetching currency data: {str(e)}")
        raise

def fetch_skill_gems_data(**context) -> Dict[str, Any]:
    """Fetch skill gem prices and information and insert into database"""
    url = f"{BASE_URL}/itemoverview?league={LEAGUE}&type=SkillGem"
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Extract skill gem information
        gems_data = []
        for line in data.get('lines', []):
            gem_info = {
                'gem_name': line.get('name'),
                'gem_type': line.get('itemClass'),
                'chaos_value': line.get('chaosValue', 0),
                'divine_value': line.get('divineValue', 0),
                'gem_level': line.get('gemLevel'),
                'gem_quality': line.get('gemQuality'),
                'corrupted': line.get('corrupted', False),
                'variant': line.get('variant'),
                'listing_count': line.get('listingCount', 0),
                'count': line.get('count', 0),
                'details_id': line.get('detailsId'),
                'icon_url': line.get('icon'),
                'trade_info': line.get('pay', {}),
                'sparkline': line.get('sparkline', {}),
                'low_confidence': line.get('lowConfidence', False),
                'timestamp': datetime.now().isoformat()
            }
            gems_data.append(gem_info)
        
        # Insert into database
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                for item in gems_data:
                    cur.execute("""
                        INSERT INTO poe_skill_gems_data 
                        (gem_name, gem_type, chaos_value, divine_value, gem_level, gem_quality, corrupted, 
                         variant, listing_count, count, details_id, icon_url, trade_info, sparkline, low_confidence, extracted_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        item['gem_name'],
                        item['gem_type'],
                        item['chaos_value'],
                        item['divine_value'],
                        item['gem_level'],
                        item['gem_quality'],
                        item['corrupted'],
                        item['variant'],
                        item['listing_count'],
                        item['count'],
                        item['details_id'],
                        item['icon_url'],
                        json.dumps(item['trade_info']),
                        json.dumps(item['sparkline']),
                        item['low_confidence'],
                        datetime.now()
                    ))
            conn.commit()
        
        # Also save to file for backup
        filename = f"{DATA_DIR}/skill_gems_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, 'w') as f:
            json.dump(gems_data, f, indent=2)
        
        log_extraction('skill_gems_data', 'success', len(gems_data))
        print(f"Skill gems data saved: {len(gems_data)} items processed to database and {filename}")
        return {'filename': filename, 'total_count': len(gems_data)}
        
    except Exception as e:
        log_extraction('skill_gems_data', 'error', 0, str(e))
        print(f"Error fetching skill gems data: {str(e)}")
        raise

def fetch_divination_cards_data(**context) -> Dict[str, Any]:
    """Fetch divination card prices and information and insert into database"""
    url = f"{BASE_URL}/itemoverview?league={LEAGUE}&type=DivinationCard"
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Extract divination card information
        cards_data = []
        for line in data.get('lines', []):
            card_info = {
                'card_name': line.get('name'),
                'chaos_value': line.get('chaosValue', 0),
                'divine_value': line.get('divineValue', 0),
                'stack_size': line.get('stackSize'),
                'listing_count': line.get('listingCount', 0),
                'count': line.get('count', 0),
                'details_id': line.get('detailsId'),
                'icon_url': line.get('icon'),
                'trade_info': line.get('pay', {}),
                'sparkline': line.get('sparkline', {}),
                'low_confidence': line.get('lowConfidence', False),
                'timestamp': datetime.now().isoformat()
            }
            cards_data.append(card_info)
        
        # Insert into database
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                for item in cards_data:
                    cur.execute("""
                        INSERT INTO poe_divination_cards_data 
                        (card_name, chaos_value, divine_value, stack_size, listing_count, count, 
                         details_id, icon_url, trade_info, sparkline, low_confidence, extracted_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        item['card_name'],
                        item['chaos_value'],
                        item['divine_value'],
                        item['stack_size'],
                        item['listing_count'],
                        item['count'],
                        item['details_id'],
                        item['icon_url'],
                        json.dumps(item['trade_info']),
                        json.dumps(item['sparkline']),
                        item['low_confidence'],
                        datetime.now()
                    ))
            conn.commit()
        
        # Also save to file for backup
        filename = f"{DATA_DIR}/divination_cards_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, 'w') as f:
            json.dump(cards_data, f, indent=2)
        
        log_extraction('divination_cards_data', 'success', len(cards_data))
        print(f"Divination cards data saved: {len(cards_data)} items processed to database and {filename}")
        return {'filename': filename, 'total_count': len(cards_data)}
        
    except Exception as e:
        log_extraction('divination_cards_data', 'error', 0, str(e))
        print(f"Error fetching divination cards data: {str(e)}")
        raise

def fetch_unique_items_data(**context) -> Dict[str, Any]:
    """Fetch unique weapons, armours, and accessories data and insert into database"""
    item_types = ['UniqueWeapon', 'UniqueArmour', 'UniqueAccessory']
    all_items_data = []
    
    for item_type in item_types:
        url = f"{BASE_URL}/itemoverview?league={LEAGUE}&type={item_type}"
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            # Extract unique item information
            for line in data.get('lines', []):
                item_info = {
                    'item_name': line.get('name'),
                    'base_type': line.get('baseType'),
                    'item_type': item_type,
                    'chaos_value': line.get('chaosValue', 0),
                    'divine_value': line.get('divineValue', 0),
                    'exalted_value': line.get('exaltedValue', 0),
                    'links': line.get('links'),
                    'item_level': line.get('levelRequired'),
                    'variant': line.get('variant'),
                    'listing_count': line.get('listingCount', 0),
                    'count': line.get('count', 0),
                    'details_id': line.get('detailsId'),
                    'icon_url': line.get('icon'),
                    'trade_info': line.get('pay', {}),
                    'sparkline': line.get('sparkline', {}),
                    'low_confidence': line.get('lowConfidence', False),
                    'corrupted': line.get('corrupted', False),
                    'timestamp': datetime.now().isoformat()
                }
                all_items_data.append(item_info)
                
        except Exception as e:
            print(f"Error fetching {item_type} data: {str(e)}")
            continue
    
    try:
        # Insert into database
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                for item in all_items_data:
                    cur.execute("""
                        INSERT INTO poe_unique_items_data 
                        (item_name, base_type, item_type, chaos_value, divine_value, exalted_value, 
                         links, item_level, variant, listing_count, count, details_id, 
                         icon_url, trade_info, sparkline, low_confidence, corrupted, extracted_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        item['item_name'],
                        item['base_type'],
                        item['item_type'],
                        item['chaos_value'],
                        item['divine_value'],
                        item['exalted_value'],
                        item['links'],
                        item['item_level'],
                        item['variant'],
                        item['listing_count'],
                        item['count'],
                        item['details_id'],
                        item['icon_url'],
                        json.dumps(item['trade_info']),
                        json.dumps(item['sparkline']),
                        item['low_confidence'],
                        item['corrupted'],
                        datetime.now()
                    ))
            conn.commit()
        
        # Also save to file for backup
        filename = f"{DATA_DIR}/unique_items_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, 'w') as f:
            json.dump(all_items_data, f, indent=2)
        
        log_extraction('unique_items_data', 'success', len(all_items_data))
        print(f"Unique items data saved: {len(all_items_data)} items processed to database and {filename}")
        return {'filename': filename, 'total_count': len(all_items_data)}
        
    except Exception as e:
        log_extraction('unique_items_data', 'error', 0, str(e))
        print(f"Error saving unique items data: {str(e)}")
        raise

# ============================================================================
# TRANSFORMATION FUNCTIONS
# ============================================================================

def transform_currency_data(**context) -> Dict[str, Any]:
    """Transform currency data for market analysis"""
    try:
        # Load currency data from database
        with get_db_connection() as conn:
            df = pd.read_sql_query("""
                SELECT currency_name, chaos_value, listing_count, extracted_at
                FROM poe_currency_data 
                WHERE extracted_at >= NOW() - INTERVAL '2 hours'
                ORDER BY extracted_at DESC
            """, conn)
        
        if df.empty:
            print("No recent currency data found in database")
            log_transformation('currency', 0, 'error', 'No recent data found')
            return {'records': 0}
        
        # Clean and transform data
        df['chaos_value'] = pd.to_numeric(df['chaos_value'], errors='coerce')
        df['listing_count'] = pd.to_numeric(df['listing_count'], errors='coerce')
        
        # Remove rows with invalid data
        df = df.dropna(subset=['chaos_value', 'listing_count'])
        df = df[df['chaos_value'] > 0]
        
        # Calculate market metrics
        df['market_cap'] = df['chaos_value'] * df['listing_count']
        df['liquidity_score'] = np.log1p(df['listing_count']) * df['chaos_value']
        
        # Categorize currencies
        def categorize_currency(name):
            if not name:
                return 'Unknown'
            name_lower = name.lower()
            if 'divine' in name_lower:
                return 'Premium'
            elif any(x in name_lower for x in ['exalted', 'ancient', 'eternal']):
                return 'High-Tier'
            elif any(x in name_lower for x in ['chaos', 'alchemy', 'fusing', 'chromatic']):
                return 'Mid-Tier'
            else:
                return 'Low-Tier'
        
        df['currency_tier'] = df['currency_name'].apply(categorize_currency)
        
        # Create summary statistics
        summary_stats = {
            'total_currencies': len(df),
            'total_market_cap': float(df['market_cap'].sum()),
            'avg_chaos_value': float(df['chaos_value'].mean()),
            'median_chaos_value': float(df['chaos_value'].median()),
            'most_valuable': df.loc[df['chaos_value'].idxmax(), 'currency_name'] if not df.empty else None,
            'most_liquid': df.loc[df['listing_count'].idxmax(), 'currency_name'] if not df.empty else None,
            'tier_distribution': df['currency_tier'].value_counts().to_dict(),
            'timestamp': datetime.now().isoformat()
        }
        
        # Insert market summary into database
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO poe_market_summary (
                        summary_date, total_currencies, avg_currency_chaos_value,
                        high_value_currencies, most_liquid_currency, summary_data, extracted_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    datetime.now().date(),
                    summary_stats['total_currencies'],
                    summary_stats['avg_chaos_value'],
                    len(df[df['chaos_value'] > 100]),
                    df.loc[df['listing_count'].idxmax(), 'currency_name'] if not df.empty else None,
                    json.dumps(summary_stats),
                    datetime.now()
                ))
                conn.commit()
        
        # Save backup files
        output_file = f"{OUTPUT_DIR}/currency_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        analysis_data = {
            'summary_stats': summary_stats,
            'top_10_by_value': df.nlargest(10, 'chaos_value')[['currency_name', 'chaos_value', 'listing_count']].to_dict('records'),
            'top_10_by_liquidity': df.nlargest(10, 'listing_count')[['currency_name', 'chaos_value', 'listing_count']].to_dict('records')
        }
        
        with open(output_file, 'w') as f:
            json.dump(analysis_data, f, indent=2)
        
        log_transformation('currency', len(df))
        print(f"Currency analysis saved to database and backup files: {output_file}")
        return {'json_file': output_file, 'records': len(df)}
        
    except Exception as e:
        error_msg = f"Error transforming currency data: {str(e)}"
        print(error_msg)
        log_transformation('currency', 0, 'error', str(e))
        raise

def transform_gems_data(**context) -> Dict[str, Any]:
    """Transform skill gems data for profit analysis"""
    try:
        # Load gems data from database
        with get_db_connection() as conn:
            df = pd.read_sql_query("""
                SELECT gem_name, chaos_value, divine_value, gem_level, gem_quality, 
                       listing_count, corrupted, extracted_at
                FROM poe_skill_gems_data 
                WHERE extracted_at >= NOW() - INTERVAL '2 hours'
                ORDER BY extracted_at DESC
            """, conn)
        
        if df.empty:
            print("No recent gems data found in database")
            log_transformation('gems', 0, 'error', 'No recent data found')
            return {'records': 0}
        
        # Clean and transform data
        df['chaos_value'] = pd.to_numeric(df['chaos_value'], errors='coerce')
        df['divine_value'] = pd.to_numeric(df['divine_value'], errors='coerce')
        df['gem_level'] = pd.to_numeric(df['gem_level'], errors='coerce')
        df['gem_quality'] = pd.to_numeric(df['gem_quality'], errors='coerce')
        df['listing_count'] = pd.to_numeric(df['listing_count'], errors='coerce')
        
        # Calculate profit metrics
        df['is_max_level'] = df['gem_level'] == 20
        df['is_quality'] = df['gem_quality'] > 0
        df['is_corrupted'] = df['corrupted'].fillna(False)
        
        # Estimate leveling profit potential
        def calculate_leveling_profit(row):
            if pd.isna(row['chaos_value']) or row['chaos_value'] <= 0:
                return 0
            
            base_value = row['chaos_value']
            gem_level = row['gem_level'] if not pd.isna(row['gem_level']) else 1
            
            # Simple profit model: higher level gems are worth more
            if gem_level < 20 and not row['is_corrupted']:
                # Estimate profit based on level difference
                level_multiplier = (20 - gem_level) * 0.1  # 10% per level
                return base_value * level_multiplier
            return 0
        
        df['estimated_leveling_profit'] = df.apply(calculate_leveling_profit, axis=1)
        df['profit_margin_percent'] = np.where(df['chaos_value'] > 0, 
                                             (df['estimated_leveling_profit'] / df['chaos_value']) * 100, 0)
        
        # Categorize gems
        def categorize_gem(name):
            if not name:
                return 'Unknown'
            name_lower = name.lower()
            if 'awakened' in name_lower:
                return 'Awakened'
            elif 'support' in name_lower:
                return 'Support'
            elif any(x in name_lower for x in ['aura', 'herald', 'banner']):
                return 'Aura/Herald'
            elif any(x in name_lower for x in ['curse', 'mark']):
                return 'Curse'
            else:
                return 'Active Skill'
        
        df['gem_category'] = df['gem_name'].apply(categorize_gem)
        
        # Create summary statistics
        summary_stats = {
            'total_gems': len(df),
            'avg_chaos_value': float(df['chaos_value'].mean()),
            'profitable_gems': len(df[df['estimated_leveling_profit'] > 0]),
            'high_profit_gems': len(df[df['profit_margin_percent'] > 50]),
            'awakened_gems': len(df[df['gem_category'] == 'Awakened']),
            'corrupted_gems': len(df[df['is_corrupted']]),
            'max_level_gems': len(df[df['is_max_level']]),
            'category_distribution': df['gem_category'].value_counts().to_dict(),
            'timestamp': datetime.now().isoformat()
        }
        
        # Identify top profit opportunities
        profitable_gems = df[df['estimated_leveling_profit'] > 0].copy()
        profitable_gems = profitable_gems.sort_values('profit_margin_percent', ascending=False)
        
        # Insert profit opportunities into database
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Clear old profit opportunities (keep only recent ones)
                cur.execute("DELETE FROM poe_profit_opportunities WHERE extracted_at < NOW() - INTERVAL '1 day'")
                
                # Insert new opportunities
                for _, opportunity in profitable_gems.head(50).iterrows():
                    cur.execute("""
                        INSERT INTO poe_profit_opportunities 
                        (opportunity_type, item_name, item_variant, current_chaos_value, 
                         profit_percentage, confidence_score, analysis_data, extracted_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        'gem',
                        opportunity['gem_name'],
                        opportunity.get('variant'),
                        float(opportunity['chaos_value']),
                        float(opportunity['profit_margin_percent']),
                        min(float(opportunity['listing_count']) / 50.0, 1.0),
                        json.dumps({
                            'gem_level': int(opportunity['gem_level']) if not pd.isna(opportunity['gem_level']) else None,
                            'gem_quality': int(opportunity['gem_quality']) if not pd.isna(opportunity['gem_quality']) else None,
                            'listing_count': int(opportunity['listing_count']) if not pd.isna(opportunity['listing_count']) else None,
                            'strategy': 'gem_leveling'
                        }),
                        datetime.now()
                    ))
                conn.commit()
        
        # Save backup files
        output_file = f"{OUTPUT_DIR}/gems_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        analysis_data = {
            'summary_stats': summary_stats,
            'top_profit_opportunities': profitable_gems.head(20)[[
                'gem_name', 'chaos_value', 'estimated_leveling_profit', 'profit_margin_percent',
                'gem_level', 'gem_quality', 'listing_count', 'gem_category'
            ]].to_dict('records')
        }
        
        with open(output_file, 'w') as f:
            json.dump(analysis_data, f, indent=2)
        
        log_transformation('gems', len(df))
        print(f"Gems analysis saved to database and backup files: {output_file}")
        return {'json_file': output_file, 'records': len(df)}
        
    except Exception as e:
        error_msg = f"Error transforming gems data: {str(e)}"
        print(error_msg)
        log_transformation('gems', 0, 'error', str(e))
        raise

def create_market_summary(**context) -> Dict[str, Any]:
    """Create comprehensive market summary from all data sources"""
    try:
        # Get latest data from database
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Get profit opportunities
                cur.execute("""
                    SELECT item_name, current_chaos_value, profit_percentage, confidence_score
                    FROM poe_profit_opportunities 
                    WHERE extracted_at >= NOW() - INTERVAL '2 hours'
                    ORDER BY profit_percentage DESC
                    LIMIT 10
                """)
                top_opportunities = cur.fetchall()
                
                # Get market summary stats
                cur.execute("""
                    SELECT COUNT(*) as total_records, 
                           AVG(avg_currency_chaos_value) as avg_currency_value,
                           AVG(avg_gem_chaos_value) as avg_gem_value
                    FROM poe_market_summary 
                    WHERE created_at >= NOW() - INTERVAL '2 hours'
                """)
                market_stats = cur.fetchone()
        
        # Convert Decimal objects to float for JSON serialization
        def convert_decimals(data):
            if isinstance(data, dict):
                return {k: convert_decimals(v) for k, v in data.items()}
            elif isinstance(data, list):
                return [convert_decimals(item) for item in data]
            elif isinstance(data, Decimal):
                return float(data)
            return data
        
        market_summary = {
            'timestamp': datetime.now().isoformat(),
            'top_profit_opportunities': convert_decimals([dict(row) for row in top_opportunities]),
            'market_stats': convert_decimals(dict(market_stats) if market_stats else {}),
            'data_freshness': 'last_2_hours'
        }
        
        # Save comprehensive summary
        output_file = f"{OUTPUT_DIR}/market_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(output_file, 'w') as f:
            json.dump(market_summary, f, indent=2)
        
        log_transformation('market_summary', len(top_opportunities))
        print(f"Market summary created: {output_file}")
        return {'summary_file': output_file, 'opportunities_count': len(top_opportunities)}
        
    except Exception as e:
        error_msg = f"Error creating market summary: {str(e)}"
        print(error_msg)
        log_transformation('market_summary', 0, 'error', str(e))
        raise

# ============================================================================
# TASK DEFINITIONS
# ============================================================================

# Setup task
setup_task = PythonOperator(
    task_id='setup_directories',
    python_callable=create_directories,
    dag=dag,
)

# Extraction tasks
extract_currency_task = PythonOperator(
    task_id='extract_currency_data',
    python_callable=fetch_currency_data,
    dag=dag,
)

extract_gems_task = PythonOperator(
    task_id='extract_skill_gems_data',
    python_callable=fetch_skill_gems_data,
    dag=dag,
)

extract_cards_task = PythonOperator(
    task_id='extract_divination_cards_data',
    python_callable=fetch_divination_cards_data,
    dag=dag,
)

extract_items_task = PythonOperator(
    task_id='extract_unique_items_data',
    python_callable=fetch_unique_items_data,
    dag=dag,
)

# Transformation tasks
transform_currency_task = PythonOperator(
    task_id='transform_currency_data',
    python_callable=transform_currency_data,
    dag=dag,
)

transform_gems_task = PythonOperator(
    task_id='transform_gems_data',
    python_callable=transform_gems_data,
    dag=dag,
)

# Final summary task
market_summary_task = PythonOperator(
    task_id='create_market_summary',
    python_callable=create_market_summary,
    dag=dag,
)

# ============================================================================
# TASK DEPENDENCIES
# ============================================================================

# Setup must run first
setup_task >> [extract_currency_task, extract_gems_task, extract_cards_task, extract_items_task]

# Transformation tasks depend on their respective extraction tasks
extract_currency_task >> transform_currency_task
extract_gems_task >> transform_gems_task

# Market summary depends on all transformation tasks
[transform_currency_task, transform_gems_task] >> market_summary_task