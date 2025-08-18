from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import requests
import pandas as pd
import json
import os
import numpy as np
import math
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
LEAGUE = os.getenv('POE_LEAGUE', 'Mercenaries')  # Read from environment variable, fallback to 'Mercenaries'
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
        
        # Create a mapping of currency details for image URLs and trade IDs
        currency_details_map = {}
        for detail in data.get('currencyDetails', []):
            currency_details_map[detail.get('name')] = {
                'icon_url': detail.get('icon'),
                'trade_id': detail.get('tradeId'),
                'currency_id': detail.get('id')
            }
        
        # Extract relevant currency information with enhanced data capture
        currency_data = []
        for line in data.get('lines', []):
            # Extract pay and receive data separately for better analysis
            pay_data = line.get('pay', {})
            receive_data = line.get('receive', {})
            
            # Get currency details (icon, trade_id) from the mapping
            currency_name = line.get('currencyTypeName')
            currency_details = currency_details_map.get(currency_name, {})
            
            # Calculate buy/sell prices and gaps using chaos values when appropriate
            # Currency ID 22 represents Chaos Orb, so we should use those values for chaos calculations
            chaos_buy_price = 0
            chaos_sell_price = 0
            
            # Determine chaos values based on currency IDs
            if pay_data.get('get_currency_id') == 22:  # Getting chaos orbs
                chaos_buy_price = 1 / pay_data.get('value', 1) if pay_data.get('value', 0) > 0 else 0
            elif pay_data.get('pay_currency_id') == 22:  # Paying with chaos orbs
                chaos_buy_price = pay_data.get('value', 0)
            
            if receive_data.get('get_currency_id') == 22:  # Getting chaos orbs
                chaos_sell_price = 1 / receive_data.get('value', 1) if receive_data.get('value', 0) > 0 else 0
            elif receive_data.get('pay_currency_id') == 22:  # Paying with chaos orbs
                chaos_sell_price = receive_data.get('value', 0)
            
            # Use chaos equivalent as fallback if no direct chaos trades
            if chaos_buy_price == 0 and chaos_sell_price == 0:
                chaos_buy_price = line.get('chaosEquivalent', 0)
                chaos_sell_price = line.get('chaosEquivalent', 0)
            
            # Raw buy/sell prices for reference
            buy_price = pay_data.get('value', 0)  # What you pay to buy
            sell_price = receive_data.get('value', 0)  # What you receive when selling
            
            # Calculate price gap using chaos values
            price_gap_absolute = 0
            price_gap_percentage = 0
            
            if chaos_buy_price > 0 and chaos_sell_price > 0:
                price_gap_absolute = abs(chaos_sell_price - chaos_buy_price)
                # Calculate percentage gap relative to the average chaos price
                avg_chaos_price = (chaos_buy_price + chaos_sell_price) / 2
                if avg_chaos_price > 0:
                    price_gap_percentage = (price_gap_absolute / avg_chaos_price) * 100
            
            currency_info = {
                'currency_name': currency_name,
                'chaos_value': line.get('chaosEquivalent', 0),
                'chaos_equivalent': line.get('chaosEquivalent', 0),
                'details_id': line.get('detailsId'),
                'count': line.get('count', 0),
                'icon_url': currency_details.get('icon_url'),
                'trade_id': currency_details.get('trade_id'),
                'low_confidence': line.get('lowConfidence', False),
                
                # Buy/Sell price analysis (chaos-based)
                'buy_price': buy_price,
                'sell_price': sell_price,
                'chaos_buy_price': chaos_buy_price,
                'chaos_sell_price': chaos_sell_price,
                'price_gap_absolute': price_gap_absolute,
                'price_gap_percentage': price_gap_percentage,
                
                # Enhanced pay/receive data capture
                'pay_data': {
                    'id': pay_data.get('id', 0),
                    'league_id': pay_data.get('league_id'),
                    'pay_currency_id': pay_data.get('pay_currency_id'),
                    'get_currency_id': pay_data.get('get_currency_id'),
                    'sample_time_utc': pay_data.get('sample_time_utc'),
                    'count': pay_data.get('count', 0),
                    'value': pay_data.get('value', 0),
                    'data_point_count': pay_data.get('data_point_count', 0),
                    'includes_secondary': pay_data.get('includes_secondary', False),
                    'listing_count': pay_data.get('listing_count', 0)
                },
                'receive_data': {
                    'id': receive_data.get('id', 0),
                    'league_id': receive_data.get('league_id'),
                    'pay_currency_id': receive_data.get('pay_currency_id'),
                    'get_currency_id': receive_data.get('get_currency_id'),
                    'sample_time_utc': receive_data.get('sample_time_utc'),
                    'count': receive_data.get('count', 0),
                    'value': receive_data.get('value', 0),
                    'data_point_count': receive_data.get('data_point_count', 0),
                    'includes_secondary': receive_data.get('includes_secondary', False),
                    'listing_count': receive_data.get('listing_count', 0)
                },
                
                # Enhanced sparkline data capture
                'pay_sparkline': line.get('paySparkLine', {}),
                'receive_sparkline': line.get('receiveSparkLine', {}),
                'low_confidence_pay_sparkline': line.get('lowConfidencePaySparkLine', {}),
                'low_confidence_receive_sparkline': line.get('lowConfidenceReceiveSparkLine', {}),
                
                # Legacy sparkline for backward compatibility
                'sparkline': line.get('sparkline', {}),
                
                # Extract aggregated data point count and includes_secondary from pay/receive data
                'data_point_count': max(
                    pay_data.get('data_point_count', 0),
                    receive_data.get('data_point_count', 0)
                ),
                'includes_secondary': (
                    pay_data.get('includes_secondary', False) or 
                    receive_data.get('includes_secondary', False)
                ),
                
                'timestamp': datetime.now().isoformat()
            }
            currency_data.append(currency_info)
        
        # Insert comprehensive currency details into the currency_details table
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # First, insert/update currency details
                for detail in data.get('currencyDetails', []):
                    cur.execute("""
                        INSERT INTO poe_currency_details (currency_id, currency_name, icon_url, trade_id, updated_at)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (currency_id) DO UPDATE SET
                            currency_name = EXCLUDED.currency_name,
                            icon_url = EXCLUDED.icon_url,
                            trade_id = EXCLUDED.trade_id,
                            updated_at = EXCLUDED.updated_at
                    """, (
                        detail.get('id'),
                        detail.get('name'),
                        detail.get('icon'),
                        detail.get('tradeId'),
                        datetime.now()
                    ))
                
                # Insert comprehensive currency data with all fields
                for item in currency_data:
                    # Calculate total listing count from both pay and receive data
                    total_listing_count = (
                        item['pay_data'].get('listing_count', 0) + 
                        item['receive_data'].get('listing_count', 0)
                    )
                    
                    # Extract sparkline total changes with proper None handling
                    pay_total_change = item['pay_sparkline'].get('totalChange') or 0
                    receive_total_change = item['receive_sparkline'].get('totalChange') or 0
                    low_conf_pay_total_change = item['low_confidence_pay_sparkline'].get('totalChange') or 0
                    low_conf_receive_total_change = item['low_confidence_receive_sparkline'].get('totalChange') or 0

                    # Derived metrics for advanced analytics with proper None handling
                    pay_sparkline_data = item['pay_sparkline'].get('data', [])
                    receive_sparkline_data = item['receive_sparkline'].get('data', [])
                    
                    pay_volatility = (
                        float(np.std(pay_sparkline_data))
                        if pay_sparkline_data and len(pay_sparkline_data) > 0 and all(x is not None for x in pay_sparkline_data) else 0
                    )
                    receive_volatility = (
                        float(np.std(receive_sparkline_data))
                        if receive_sparkline_data and len(receive_sparkline_data) > 0 and all(x is not None for x in receive_sparkline_data) else 0
                    )
                    average_price_change = (pay_total_change + receive_total_change) / 2
                    total_count = (item['pay_data'].get('count') or 0) + (item['receive_data'].get('count') or 0)
                    liquidity_score = (
                        total_listing_count / total_count if total_count > 0 else 0
                    )
                    
                    # Get currency_id from currency_details_map
                    currency_details = currency_details_map.get(item['currency_name'], {})
                    currency_id = currency_details.get('currency_id')
                    
                    # Extract sample time from pay data (use receive if pay is not available)
                    sample_time_utc = None
                    if item['pay_data'].get('sample_time_utc'):
                        sample_time_utc = item['pay_data']['sample_time_utc']
                    elif item['receive_data'].get('sample_time_utc'):
                        sample_time_utc = item['receive_data']['sample_time_utc']
                    
                    # Extract league_id from pay or receive data
                    league_id = item['pay_data'].get('league_id') or item['receive_data'].get('league_id')
                    
                    cur.execute("""
                        INSERT INTO poe_currency_data
                        (currency_name, chaos_value, chaos_equivalent, details_id, count, listing_count,
                         icon_url, trade_info, sparkline, pay_sparkline, receive_sparkline,
                         low_confidence_pay_sparkline, low_confidence_receive_sparkline,
                         low_confidence, league, extracted_at, pay_volatility, receive_volatility,
                         average_price_change, liquidity_score)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        item['currency_name'],
                        item['chaos_value'],
                        item['chaos_equivalent'],
                        item['details_id'],
                        item['count'],
                        total_listing_count,
                        item['icon_url'],
                        json.dumps({
                            'pay_data': item['pay_data'],
                            'receive_data': item['receive_data'],
                            'buy_price': item['buy_price'],
                            'sell_price': item['sell_price'],
                            'chaos_buy_price': item['chaos_buy_price'],
                            'chaos_sell_price': item['chaos_sell_price'],
                            'price_gap_absolute': item['price_gap_absolute'],
                            'price_gap_percentage': item['price_gap_percentage'],
                            'trade_id': item['trade_id'],
                            'currency_id': currency_id,
                            'league_id': league_id,
                            'sample_time_utc': sample_time_utc,
                            'pay_count': item['pay_data'].get('count', 0),
                            'receive_count': item['receive_data'].get('count', 0),
                            'pay_listing_count': item['pay_data'].get('listing_count', 0),
                            'receive_listing_count': item['receive_data'].get('listing_count', 0),
                            'total_change_pay': pay_total_change,
                            'total_change_receive': receive_total_change,
                            'total_change_low_confidence_pay': low_conf_pay_total_change,
                            'total_change_low_confidence_receive': low_conf_receive_total_change,
                            'data_point_count': item['data_point_count'],
                            'includes_secondary': item['includes_secondary'],
                            'pay_volatility': pay_volatility,
                            'receive_volatility': receive_volatility,
                            'average_price_change': average_price_change,
                            'liquidity_score': liquidity_score
                        }),
                        json.dumps(item['sparkline']),  # Keep legacy sparkline for compatibility
                        json.dumps(item['pay_sparkline']),
                        json.dumps(item['receive_sparkline']),
                        json.dumps(item['low_confidence_pay_sparkline']),
                        json.dumps(item['low_confidence_receive_sparkline']),
                        item['low_confidence'],
                        LEAGUE,
                        datetime.now(),
                        pay_volatility,
                        receive_volatility,
                        average_price_change,
                        liquidity_score
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
                         variant, listing_count, count, details_id, icon_url, trade_info, sparkline, low_confidence, league, extracted_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                        LEAGUE,
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
                         details_id, icon_url, trade_info, sparkline, low_confidence, league, extracted_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                        LEAGUE,
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
                         icon_url, trade_info, sparkline, low_confidence, corrupted, league, extracted_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                        LEAGUE,
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
# ARBITRAGE ANALYSIS FUNCTIONS
# ============================================================================

def calculate_confidence_score(pay_listing_count: int, receive_listing_count: int, spread_percentage: float) -> tuple[int, bool]:
    """
    Calculate confidence score and identify low confidence data based on listing counts and spread.
    
    Args:
        pay_listing_count: Number of pay listings
        receive_listing_count: Number of receive listings
        spread_percentage: Arbitrage spread percentage
        
    Returns:
        Tuple of (confidence_score, is_low_confidence)
    """
    # Base confidence is the minimum of the two listing counts
    min_listings = min(pay_listing_count, receive_listing_count)
    total_listings = pay_listing_count + receive_listing_count
    
    # Calculate base confidence score (0-100)
    if min_listings >= 50:
        base_score = 90
    elif min_listings >= 20:
        base_score = 70
    elif min_listings >= 10:
        base_score = 50
    elif min_listings >= 5:
        base_score = 30
    else:
        base_score = 10
    
    # Adjust for listing imbalance (large difference between pay and receive counts)
    listing_ratio = max(pay_listing_count, receive_listing_count) / max(min_listings, 1)
    if listing_ratio > 10:  # Very imbalanced
        base_score -= 20
    elif listing_ratio > 5:  # Moderately imbalanced
        base_score -= 10
    
    # Adjust for suspicious spreads (very high spreads with low listings are often unreliable)
    if spread_percentage > 100 and min_listings < 10:
        base_score -= 30  # Highly suspicious
    elif spread_percentage > 50 and min_listings < 5:
        base_score -= 20  # Moderately suspicious
    
    # Ensure score is within bounds
    confidence_score = max(0, min(100, base_score))
    
    # Determine if this is low confidence data
    is_low_confidence = (
        min_listings < 5 or  # Very few listings on either side
        confidence_score < 25 or  # Low overall confidence
        (spread_percentage > 200 and min_listings < 10)  # Extremely high spread with low listings
    )
    
    return confidence_score, is_low_confidence


def analyze_arbitrage_opportunities(json_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Analyze arbitrage opportunities from POE Ninja currency data.
    
    Args:
        json_data: Dictionary containing 'lines' and 'currencyDetails'
        
    Returns:
        List of dictionaries with arbitrage analysis results
    """
    lines = json_data.get('lines', [])
    currency_details = json_data.get('currencyDetails', [])
    
    # Create currency lookup dictionaries - both by ID and by name
    currency_lookup_by_id = {detail['id']: detail['name'] for detail in currency_details}
    currency_lookup_by_name = {detail['name']: detail['id'] for detail in currency_details}
    
    arbitrage_results = []
    
    for line in lines:
        currency_name = None
        currency_id = None
        
        try:
            # Get currency name and ID - handle both string names and integer IDs
            if 'currencyTypeName' in line:
                currency_name = line['currencyTypeName']
                currency_id = currency_lookup_by_name.get(currency_name)
            elif 'get_currency_id' in line:
                currency_id = line['get_currency_id']
                currency_name = currency_lookup_by_id.get(currency_id)
            elif 'detailsId' in line:
                # Try to find by detailsId
                details_id = line['detailsId']
                for detail in currency_details:
                    if detail.get('tradeId') == details_id or detail.get('name', '').lower().replace(' ', '-') == details_id:
                        currency_name = detail['name']
                        currency_id = detail['id']
                        break
            
            # If we still don't have currency info, skip this line
            if not currency_name or currency_id is None:
                print(f"Warning: Could not identify currency for line: {line.get('currencyTypeName', 'Unknown')}")
                continue
            
            # Extract pay and receive data
            pay_data = line.get('pay', {})
            receive_data = line.get('receive', {})
            
            pay_value = pay_data.get('value', 0)
            receive_value = receive_data.get('value', 0)
            pay_listing_count = pay_data.get('listing_count', 0)
            receive_listing_count = receive_data.get('listing_count', 0)
            
            # Validate data - skip if essential data is missing or invalid
            if not pay_value or not receive_value or pay_value <= 0 or receive_value <= 0:
                print(f"Warning: Invalid pay/receive values for {currency_name}: pay={pay_value}, receive={receive_value}")
                continue
                
            # Calculate prices and spreads with proper validation
            try:
                buy_price = 1 / pay_value
                sell_price = receive_value
                spread = sell_price - buy_price
                
                # Avoid division by zero and handle edge cases
                if buy_price > 0:
                    spread_percentage = (spread / buy_price) * 100
                else:
                    spread_percentage = 0
                    
                listing_difference = pay_listing_count - receive_listing_count
                
                # Enhanced confidence scoring system
                confidence_score, is_low_confidence = calculate_confidence_score(
                    pay_listing_count, receive_listing_count, spread_percentage
                )
                
                # Enhanced arbitrage opportunity detection with confidence filtering
                arbitrage_opportunity = (
                    sell_price > buy_price and 
                    spread_percentage > 0.01 and  # At least 0.01% spread
                    not is_low_confidence  # Filter out low confidence opportunities
                )
                
                arbitrage_results.append({
                    'currency_name': currency_name,
                    'currency_id': int(currency_id),  # Ensure integer type
                    'buy_price_chaos': round(buy_price, 4),
                    'sell_price_chaos': round(sell_price, 4),
                    'spread_chaos': round(spread, 4),
                    'spread_percentage': round(spread_percentage, 2),
                    'listing_count_difference': listing_difference,
                    'arbitrage_opportunity': arbitrage_opportunity,
                    'pay_listing_count': pay_listing_count,
                    'receive_listing_count': receive_listing_count,
                    'confidence_score': confidence_score,
                    'is_low_confidence': is_low_confidence,
                    'pay_value': pay_value,  # Store original values for debugging
                    'receive_value': receive_value
                })
                
            except (ZeroDivisionError, ValueError) as calc_error:
                print(f"Warning: Calculation error for {currency_name}: {str(calc_error)}")
                continue
            
        except Exception as e:
            print(f"Error processing line for currency {currency_name or 'Unknown'}: {str(e)}")
            continue
    
    # Sort by spread percentage (highest first) for best opportunities
    arbitrage_results.sort(key=lambda x: x['spread_percentage'], reverse=True)
    
    return arbitrage_results

def format_arbitrage_table(arbitrage_results: List[Dict[str, Any]]) -> str:
    """
    Format arbitrage results into a readable table string.
    
    Args:
        arbitrage_results: List of arbitrage analysis results
        
    Returns:
        Formatted table string
    """
    if not arbitrage_results:
        return "No arbitrage opportunities found."
    
    # Table headers
    headers = [
        "Currency Name",
        "Buy Price (Chaos)", 
        "Sell Price (Chaos)",
        "Spread (Chaos)",
        "Spread %",
        "Listing Diff",
        "Arbitrage"
    ]
    
    # Calculate column widths
    col_widths = [len(header) for header in headers]
    for result in arbitrage_results:
        col_widths[0] = max(col_widths[0], len(str(result['currency_name'])))
        col_widths[1] = max(col_widths[1], len(str(result['buy_price_chaos'])))
        col_widths[2] = max(col_widths[2], len(str(result['sell_price_chaos'])))
        col_widths[3] = max(col_widths[3], len(str(result['spread_chaos'])))
        col_widths[4] = max(col_widths[4], len(f"{result['spread_percentage']:.2f}%"))
        col_widths[5] = max(col_widths[5], len(str(result['listing_count_difference'])))
        col_widths[6] = max(col_widths[6], len("Yes" if result['arbitrage_opportunity'] else "No"))
    
    # Build table
    table_lines = []
    
    # Header row
    header_row = " | ".join(header.ljust(width) for header, width in zip(headers, col_widths))
    table_lines.append(header_row)
    table_lines.append("-" * len(header_row))
    
    # Data rows
    for result in arbitrage_results:
        row_data = [
            str(result['currency_name']).ljust(col_widths[0]),
            str(result['buy_price_chaos']).ljust(col_widths[1]),
            str(result['sell_price_chaos']).ljust(col_widths[2]),
            str(result['spread_chaos']).ljust(col_widths[3]),
            f"{result['spread_percentage']:.2f}%".ljust(col_widths[4]),
            str(result['listing_count_difference']).ljust(col_widths[5]),
            ("Yes" if result['arbitrage_opportunity'] else "No").ljust(col_widths[6])
        ]
        table_lines.append(" | ".join(row_data))
    
    return "\n".join(table_lines)

def extract_and_analyze_arbitrage(**context) -> Dict[str, Any]:
    """
    Airflow task to extract currency data and analyze arbitrage opportunities.
    """
    try:
        # Fetch currency data from POE Ninja API
        url = f"{BASE_URL}/currencyoverview?league={LEAGUE}&type=Currency"
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        json_data = response.json()
        
        # Analyze arbitrage opportunities
        arbitrage_results = analyze_arbitrage_opportunities(json_data)
        
        # Format and print table
        table_output = format_arbitrage_table(arbitrage_results)
        print("\n" + "="*80)
        print("PATH OF EXILE CURRENCY ARBITRAGE OPPORTUNITIES")
        print("="*80)
        print(table_output)
        print("="*80)
        
        # Save results to database
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Create arbitrage opportunities table if it doesn't exist
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS poe_arbitrage_opportunities (
                    id SERIAL PRIMARY KEY,
                    currency_name VARCHAR(100) NOT NULL,
                    currency_id INTEGER,
                    buy_price_chaos DECIMAL(15,8),
                    sell_price_chaos DECIMAL(15,8),
                    spread_chaos DECIMAL(15,8),
                    spread_percentage DECIMAL(8,4),
                    listing_count_difference INTEGER,
                    arbitrage_opportunity BOOLEAN,
                    pay_listing_count INTEGER,
                    receive_listing_count INTEGER,
                    confidence_score INTEGER,
                    is_low_confidence BOOLEAN DEFAULT FALSE,
                    league VARCHAR(50) NOT NULL,
                    extracted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Clear old data for today to avoid duplicates
            cursor.execute("""
                DELETE FROM poe_arbitrage_opportunities 
                WHERE league = %s AND DATE(extracted_at) = CURRENT_DATE
            """, (LEAGUE,))
            
            # Insert arbitrage results with proper error handling
            successful_inserts = 0
            for result in arbitrage_results:
                try:
                    # Validate currency_id is not None
                    currency_id = result.get('currency_id')
                    if currency_id is None:
                        print(f"Warning: Skipping {result['currency_name']} - no valid currency_id")
                        continue
                    
                    cursor.execute("""
                        INSERT INTO poe_arbitrage_opportunities (
                            currency_name, currency_id, buy_price_chaos, sell_price_chaos,
                            spread_chaos, spread_percentage, listing_count_difference,
                            arbitrage_opportunity, pay_listing_count, receive_listing_count,
                            confidence_score, is_low_confidence, league
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        result['currency_name'], 
                        int(currency_id),  # Ensure integer type
                        Decimal(str(result['buy_price_chaos'])),
                        Decimal(str(result['sell_price_chaos'])),
                        Decimal(str(result['spread_chaos'])),
                        Decimal(str(result['spread_percentage'])),
                        result['listing_count_difference'], 
                        result['arbitrage_opportunity'],
                        result['pay_listing_count'], 
                        result['receive_listing_count'],
                        result['confidence_score'],
                        result['is_low_confidence'],
                        LEAGUE
                    ))
                    successful_inserts += 1
                    
                except Exception as insert_error:
                    print(f"Error inserting {result['currency_name']}: {str(insert_error)}")
                    continue
            
            conn.commit()
            print(f"Successfully inserted {successful_inserts} arbitrage records into database")
        
        # Save to file for analysis
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f"{OUTPUT_DIR}/arbitrage_analysis_{timestamp}.json"
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        with open(output_file, 'w') as f:
            json.dump({
                'analysis_timestamp': timestamp,
                'league': LEAGUE,
                'total_opportunities': len([r for r in arbitrage_results if r['arbitrage_opportunity']]),
                'total_currencies_analyzed': len(arbitrage_results),
                'arbitrage_results': arbitrage_results,
                'table_output': table_output
            }, f, indent=2)
        
        log_extraction('arbitrage_analysis', 'success', len(arbitrage_results))
        
        return {
            'total_opportunities': len([r for r in arbitrage_results if r['arbitrage_opportunity']]),
            'total_analyzed': len(arbitrage_results),
            'output_file': output_file
        }
        
    except Exception as e:
        error_msg = f"Arbitrage analysis failed: {str(e)}"
        print(error_msg)
        log_extraction('arbitrage_analysis', 'error', 0, error_msg)
        raise

# ============================================================================
# TRANSFORMATION FUNCTIONS
# ============================================================================

def transform_currency_to_analytics(**context) -> Dict[str, Any]:
    """Transform currency data from trade_info JSONB to optimized analytics table"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # First, ensure the analytics table exists
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS poe_currency_analytics (
                    id SERIAL PRIMARY KEY,
                    currency_data_id INTEGER REFERENCES poe_currency_data(id),
                    currency_name VARCHAR(100) NOT NULL,
                    league VARCHAR(50) NOT NULL,
                    extracted_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    buy_price DECIMAL(15,8),
                    sell_price DECIMAL(15,8),
                    chaos_buy_price DECIMAL(15,8),
                    chaos_sell_price DECIMAL(15,8),
                    price_gap_absolute DECIMAL(15,8),
                    price_gap_percentage DECIMAL(8,4),
                    pay_count INTEGER,
                    receive_count INTEGER,
                    pay_listing_count INTEGER,
                    receive_listing_count INTEGER,
                    total_listing_count INTEGER,
                    pay_currency_id INTEGER,
                    receive_currency_id INTEGER,
                    currency_id INTEGER,
                    trade_id VARCHAR(100),
                    data_point_count INTEGER,
                    includes_secondary BOOLEAN DEFAULT FALSE,
                    low_confidence BOOLEAN DEFAULT FALSE,
                    sample_time_utc TIMESTAMP WITH TIME ZONE,
                    league_id INTEGER,
                    total_change_pay DECIMAL(10,4),
                    total_change_receive DECIMAL(10,4),
                    total_change_low_confidence_pay DECIMAL(10,4),
                    total_change_low_confidence_receive DECIMAL(10,4),
                    liquidity_score DECIMAL(15,4),
                    market_depth_score DECIMAL(15,4),
                    volatility_index DECIMAL(10,4),
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Get recent currency data that hasn't been transformed yet
            cursor.execute("""
                SELECT cd.id, cd.currency_name, cd.league, cd.extracted_at, cd.trade_info, 
                       cd.listing_count, cd.low_confidence, cd.data_point_count, cd.includes_secondary
                FROM poe_currency_data cd
                LEFT JOIN poe_currency_analytics ca ON cd.id = ca.currency_data_id
                WHERE cd.extracted_at >= NOW() - INTERVAL '2 hours'
                  AND cd.trade_info IS NOT NULL
                  AND ca.id IS NULL
                ORDER BY cd.extracted_at DESC
            """)
            
            rows = cursor.fetchall()
            transformed_count = 0
            
            for row in rows:
                currency_data_id, currency_name, league, extracted_at, trade_info, listing_count, low_confidence, data_point_count, includes_secondary = row
                
                if not trade_info:
                    continue
                    
                # Extract data from trade_info JSONB
                buy_price = trade_info.get('buy_price')
                sell_price = trade_info.get('sell_price')
                chaos_buy_price = trade_info.get('chaos_buy_price')
                chaos_sell_price = trade_info.get('chaos_sell_price')
                price_gap_absolute = trade_info.get('price_gap_absolute')
                price_gap_percentage = trade_info.get('price_gap_percentage')
                pay_count = trade_info.get('pay_count', 0)
                receive_count = trade_info.get('receive_count', 0)
                pay_listing_count = trade_info.get('pay_listing_count', 0)
                receive_listing_count = trade_info.get('receive_listing_count', 0)
                pay_currency_id = trade_info.get('pay_data', {}).get('pay_currency_id')
                receive_currency_id = trade_info.get('receive_data', {}).get('get_currency_id')
                currency_id = trade_info.get('currency_id')
                trade_id = trade_info.get('trade_id')
                sample_time_utc = trade_info.get('sample_time_utc')
                league_id = trade_info.get('league_id')
                total_change_pay = trade_info.get('total_change_pay')
                total_change_receive = trade_info.get('total_change_receive')
                total_change_low_confidence_pay = trade_info.get('total_change_low_confidence_pay')
                total_change_low_confidence_receive = trade_info.get('total_change_low_confidence_receive')
                
                # Calculate derived metrics
                market_depth_score = (pay_count or 0) + (receive_count or 0)
                
                # Calculate liquidity score: log(listing_count) * price
                if listing_count and listing_count > 0 and buy_price and buy_price > 0:
                    liquidity_score = math.log1p(listing_count) * float(buy_price)
                else:
                    liquidity_score = 0
                
                # Calculate volatility index (normalized price gap)
                if price_gap_percentage and price_gap_percentage > 0:
                    volatility_index = min(float(price_gap_percentage) / 100.0, 2.0)  # Cap at 200%
                else:
                    volatility_index = 0
                
                # Parse sample_time_utc if it's a string
                if isinstance(sample_time_utc, str):
                    try:
                        sample_time_utc = datetime.fromisoformat(sample_time_utc.replace('Z', '+00:00'))
                    except Exception:
                        sample_time_utc = None
                
                # Insert into analytics table
                cursor.execute("""
                    INSERT INTO poe_currency_analytics (
                        currency_data_id, currency_name, league, extracted_at,
                        buy_price, sell_price, chaos_buy_price, chaos_sell_price, price_gap_absolute, price_gap_percentage,
                        pay_count, receive_count, pay_listing_count, receive_listing_count,
                        total_listing_count, pay_currency_id, receive_currency_id, currency_id,
                        trade_id, data_point_count, includes_secondary, low_confidence,
                        sample_time_utc, league_id, total_change_pay, total_change_receive,
                        total_change_low_confidence_pay, total_change_low_confidence_receive,
                        liquidity_score, market_depth_score, volatility_index
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """, (
                    currency_data_id, currency_name, league, extracted_at,
                    buy_price, sell_price, chaos_buy_price, chaos_sell_price, price_gap_absolute, price_gap_percentage,
                    pay_count, receive_count, pay_listing_count, receive_listing_count,
                    listing_count, pay_currency_id, receive_currency_id, currency_id,
                    trade_id, data_point_count, includes_secondary, low_confidence,
                    sample_time_utc, league_id, total_change_pay, total_change_receive,
                    total_change_low_confidence_pay, total_change_low_confidence_receive,
                    liquidity_score, market_depth_score, volatility_index
                ))
                
                transformed_count += 1
            
            conn.commit()
            
            # Refresh materialized view if it exists
            try:
                cursor.execute("REFRESH MATERIALIZED VIEW currency_hourly_metrics")
                conn.commit()
            except:
                pass  # View might not exist yet
            
            print(f"Transformed {transformed_count} currency records to analytics table")
            log_transformation('currency_analytics', transformed_count, 'success')
            
            return {'transformed_records': transformed_count}
            
    except Exception as e:
        print(f"Error in currency analytics transformation: {e}")
        log_transformation('currency_analytics', 0, 'error', str(e))
        raise

def transform_currency_data(**context) -> Dict[str, Any]:
    """Transform currency data for market analysis"""
    try:
        # Load currency data from database
        with get_db_connection() as conn:
            df = pd.read_sql_query("""
                SELECT currency_name, chaos_value, listing_count, extracted_at
                FROM poe_currency_data 
                WHERE extracted_at >= NOW() - INTERVAL '2 hours'
                  AND league = %s
                ORDER BY extracted_at DESC
            """, conn, params=[LEAGUE])
        
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
                  AND league = %s
                ORDER BY extracted_at DESC
            """, conn, params=[LEAGUE])
        
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

# Currency analytics transformation (hybrid approach)
transform_currency_analytics_task = PythonOperator(
    task_id='transform_currency_analytics',
    python_callable=transform_currency_to_analytics,
    dag=dag,
)

transform_gems_task = PythonOperator(
    task_id='transform_gems_data',
    python_callable=transform_gems_data,
    dag=dag,
)

# Arbitrage analysis task
arbitrage_analysis_task = PythonOperator(
    task_id='extract_and_analyze_arbitrage',
    python_callable=extract_and_analyze_arbitrage,
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
extract_currency_task >> transform_currency_analytics_task
extract_currency_task >> arbitrage_analysis_task
extract_gems_task >> transform_gems_task

# Market summary depends on all transformation tasks
[transform_currency_task, transform_currency_analytics_task, transform_gems_task, arbitrage_analysis_task] >> market_summary_task