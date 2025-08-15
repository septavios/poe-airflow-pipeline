from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import requests
import pandas as pd
import json
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Dict, List, Any


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
    'poe_data_extraction',
    default_args=default_args,
    description='Extract Path of Exile economic data from poe.ninja API',
    schedule=timedelta(hours=6),  # Run every 6 hours
    catchup=False,
    tags=['poe', 'gaming', 'economics', 'data_science'],
)

# Configuration
LEAGUE = 'Settlers'  # Current league - update as needed
BASE_URL = 'https://poe.ninja/api/data'
DATA_DIR = '/opt/airflow/logs/poe_data'  # Store data in logs directory

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
                    INSERT INTO extraction_log (extraction_type, status, records_processed, error_message, extracted_at)
                    VALUES (%s, %s, %s, %s, %s)
                """, (extraction_type, status, record_count, error_message, datetime.now()))
                conn.commit()
    except Exception as e:
        print(f"Error logging extraction: {str(e)}")

def create_data_directory():
    """Create directory for storing extracted data"""
    os.makedirs(DATA_DIR, exist_ok=True)
    print(f"Data directory created: {DATA_DIR}")

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
                        INSERT INTO currency_data 
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
                        INSERT INTO skill_gems_data 
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
                        INSERT INTO divination_cards_data 
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
                        INSERT INTO unique_items_data 
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

def calculate_profit_opportunities(**context) -> Dict[str, Any]:
    """Calculate potential profit opportunities based on database data and insert results"""
    try:
        # Get latest data from database
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Get latest currency data
                cur.execute("""
                    SELECT * FROM currency_data 
                    WHERE league = %s AND timestamp >= NOW() - INTERVAL '1 hour'
                    ORDER BY timestamp DESC
                """, (LEAGUE,))
                currency_data = cur.fetchall()
                
                # Get latest gems data
                cur.execute("""
                    SELECT * FROM skill_gems_data 
                    WHERE league = %s AND timestamp >= NOW() - INTERVAL '1 hour'
                    ORDER BY timestamp DESC
                """, (LEAGUE,))
                gems_data = cur.fetchall()
        
        if not currency_data or not gems_data:
            print("No recent data found for profit calculation")
            return {'status': 'no_data'}
        
        # Calculate profit opportunities for gems
        profitable_gems = []
        for gem in gems_data:
            if gem['chaos_value'] > 0 and gem['listing_count'] >= 10:
                # Simple profit calculation based on gem level and quality
                base_value = gem['chaos_value']
                gem_level = gem['gem_level'] or 1
                gem_quality = gem['gem_quality'] or 0
                
                # Estimate leveling profit (simplified)
                if gem_level < 20 and not gem['corrupted']:
                    potential_profit = base_value * 0.5  # 50% profit assumption
                    profit_data = {
                        'item_name': gem['name'],
                        'item_type': 'skill_gem',
                        'current_value': base_value,
                        'potential_profit': potential_profit,
                        'profit_percentage': (potential_profit / base_value) * 100,
                        'confidence_score': min(gem['listing_count'] / 50.0, 1.0),  # Confidence based on listings
                        'analysis_details': json.dumps({
                            'gem_level': gem_level,
                            'gem_quality': gem_quality,
                            'listing_count': gem['listing_count'],
                            'strategy': 'gem_leveling'
                        }),
                        'league': LEAGUE,
                        'timestamp': datetime.now()
                    }
                    profitable_gems.append(profit_data)
        
        # Sort by profit percentage
        profitable_gems.sort(key=lambda x: x['profit_percentage'], reverse=True)
        
        # Insert profit opportunities into database
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Clear old profit opportunities for this league
                cur.execute("DELETE FROM profit_opportunities WHERE league = %s", (LEAGUE,))
                
                # Insert new opportunities
                for opportunity in profitable_gems:
                    cur.execute("""
                        INSERT INTO profit_opportunities 
                        (item_name, item_type, current_value, potential_profit, profit_percentage, 
                         confidence_score, analysis_details, league, timestamp)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        opportunity['item_name'],
                        opportunity['item_type'],
                        opportunity['current_value'],
                        opportunity['potential_profit'],
                        opportunity['profit_percentage'],
                        opportunity['confidence_score'],
                        opportunity['analysis_details'],
                        opportunity['league'],
                        opportunity['timestamp']
                    ))
                conn.commit()
        
        # Also save to file for backup
        analysis_data = {
            'analysis_timestamp': datetime.now().isoformat(),
            'league': LEAGUE,
            'total_gems_analyzed': len(gems_data),
            'profitable_opportunities': len(profitable_gems),
            'top_profitable_gems': profitable_gems[:20],  # Top 20
            'currency_rates': {item['currency_name']: item['chaos_value'] 
                             for item in currency_data if item['currency_name']}
        }
        
        filename = f"{DATA_DIR}/profit_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, 'w') as f:
            json.dump(analysis_data, f, indent=2, default=str)
        
        log_extraction('profit_opportunities', 'success', len(profitable_gems))
        print(f"Profit analysis saved: {len(profitable_gems)} opportunities to database and {filename}")
        return {'filename': filename, 'opportunities': len(profitable_gems)}
        
    except Exception as e:
        log_extraction('profit_opportunities', 'error', 0, str(e))
        print(f"Error calculating profit opportunities: {str(e)}")
        raise

# Task definitions
create_directory_task = PythonOperator(
    task_id='create_data_directory',
    python_callable=create_data_directory,
    dag=dag,
)

fetch_currency_task = PythonOperator(
    task_id='fetch_currency_data',
    python_callable=fetch_currency_data,
    dag=dag,
)

fetch_gems_task = PythonOperator(
    task_id='fetch_skill_gems_data',
    python_callable=fetch_skill_gems_data,
    dag=dag,
)

fetch_cards_task = PythonOperator(
    task_id='fetch_divination_cards_data',
    python_callable=fetch_divination_cards_data,
    dag=dag,
)

fetch_uniques_task = PythonOperator(
    task_id='fetch_unique_items_data',
    python_callable=fetch_unique_items_data,
    dag=dag,
)

calculate_profits_task = PythonOperator(
    task_id='calculate_profit_opportunities',
    python_callable=calculate_profit_opportunities,
    dag=dag,
)

# Task dependencies
create_directory_task >> [fetch_currency_task, fetch_gems_task, fetch_cards_task, fetch_uniques_task]
[fetch_currency_task, fetch_gems_task] >> calculate_profits_task