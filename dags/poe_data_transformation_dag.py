from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import json
import os
import numpy as np
from typing import Dict, List, Any
import psycopg2
from psycopg2.extras import RealDictCursor

# Default arguments for the DAG
default_args = {
    'owner': 'data_scientist',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

# DAG definition
dag = DAG(
    'poe_data_transformation',
    default_args=default_args,
    description='Transform and analyze Path of Exile economic data',
    schedule=timedelta(hours=6),  # Run after extraction DAG
    catchup=False,
    tags=['poe', 'transformation', 'analytics', 'data_science'],
)

# Configuration
DATA_DIR = '/opt/airflow/logs/poe_data'
OUTPUT_DIR = '/opt/airflow/logs/poe_analytics'

# Database configuration
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'postgres'),
    'database': os.getenv('POSTGRES_DB', 'airflow'),
    'user': os.getenv('POSTGRES_USER', 'airflow'),
    'password': os.getenv('POSTGRES_PASSWORD', 'airflow'),
    'port': os.getenv('POSTGRES_PORT', '5432')
}

def get_db_connection():
    """Get database connection"""
    return psycopg2.connect(**DB_CONFIG)

def log_transformation(transformation_type: str, records_processed: int, status: str = 'success', error_message: str = None):
    """Log transformation activity to database"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO poe_extraction_log (data_type, records_extracted, status, error_message, extracted_at)
                    VALUES (%s, %s, %s, %s, %s)
                """, (f"transform_{transformation_type}", records_processed, status, error_message, datetime.now()))
                conn.commit()
    except Exception as e:
        print(f"Failed to log transformation: {e}")

def create_output_directory():
    """Create directory for storing transformed data"""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"Output directory created: {OUTPUT_DIR}")

def get_latest_data_file(prefix: str) -> str:
    """Get the latest data file with given prefix"""
    files = [f for f in os.listdir(DATA_DIR) if f.startswith(prefix)]
    if not files:
        raise FileNotFoundError(f"No files found with prefix {prefix}")
    return sorted(files)[-1]

def transform_currency_data(**context) -> Dict[str, Any]:
    """Transform currency data for market analysis"""
    try:
        # Load currency data from database
        with get_db_connection() as conn:
            df = pd.read_sql_query("""
                SELECT currency_name, chaos_value, listing_count, extracted_at
                FROM poe_currency_data 
                WHERE extracted_at >= NOW() - INTERVAL '24 hours'
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
            'total_market_cap': df['market_cap'].sum(),
            'avg_chaos_value': df['chaos_value'].mean(),
            'median_chaos_value': df['chaos_value'].median(),
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
                        high_value_currencies, summary_data, extracted_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    datetime.now().date(),
                    summary_stats['total_currencies'],
                    summary_stats['avg_chaos_value'],
                    summary_stats['high_value_currencies'],
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
        
        # Also save as CSV for easy analysis
        csv_file = f"{OUTPUT_DIR}/currency_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(csv_file, index=False)
        
        log_transformation('currency', len(df))
        print(f"Currency analysis saved to database and backup files: {output_file}, {csv_file}")
        return {'json_file': output_file, 'csv_file': csv_file, 'records': len(df)}
        
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
                       listing_count, level_required, corrupted, extracted_at
                FROM poe_skill_gems_data 
                WHERE extracted_at >= NOW() - INTERVAL '24 hours'
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
        df['level_required'] = pd.to_numeric(df['level_required'], errors='coerce')
        
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
            'avg_chaos_value': df['chaos_value'].mean(),
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
        
        # Insert market summary into database
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO poe_market_summary (
                        summary_date, total_gems, avg_gem_chaos_value, 
                        high_value_gems, summary_data, extracted_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    datetime.now().date(),
                    summary_stats['total_gems'],
                    summary_stats['avg_chaos_value'],
                    summary_stats['high_value_gems'],
                    json.dumps(summary_stats),
                    datetime.now()
                ))
                conn.commit()
        
        # Save backup files
        output_file = f"{OUTPUT_DIR}/gems_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        analysis_data = {
            'summary_stats': summary_stats,
            'top_profit_opportunities': profitable_gems.head(20)[[
                'name', 'chaos_value', 'estimated_leveling_profit', 'profit_margin_percent',
                'gem_level', 'gem_quality', 'listing_count', 'gem_category'
            ]].to_dict('records'),
            'awakened_gems_analysis': df[df['gem_category'] == 'Awakened'][[
                'name', 'chaos_value', 'gem_level', 'listing_count'
            ]].to_dict('records')
        }
        
        with open(output_file, 'w') as f:
            json.dump(analysis_data, f, indent=2)
        
        # Save detailed CSV
        csv_file = f"{OUTPUT_DIR}/gems_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(csv_file, index=False)
        
        log_transformation('gems', len(df))
        print(f"Gems analysis saved to database and backup files: {output_file}, {csv_file}")
        return {'json_file': output_file, 'csv_file': csv_file, 'records': len(df)}
        
    except Exception as e:
        error_msg = f"Error transforming gems data: {str(e)}"
        print(error_msg)
        log_transformation('gems', 0, 'error', str(e))
        raise

def transform_divination_cards_data(**context) -> Dict[str, Any]:
    """Transform divination cards data for investment analysis"""
    try:
        # Load cards data from database
        with get_db_connection() as conn:
            df = pd.read_sql_query("""
                SELECT card_name, chaos_value, divine_value, stack_size, listing_count, extracted_at
                FROM poe_divination_cards_data 
                WHERE extracted_at >= NOW() - INTERVAL '24 hours'
                ORDER BY extracted_at DESC
            """, conn)
        
        if df.empty:
            print("No recent divination cards data found in database")
            log_transformation('cards', 0, 'error', 'No recent data found')
            return {'records': 0}
        
        # Clean and transform data
        df['chaos_value'] = pd.to_numeric(df['chaos_value'], errors='coerce')
        df['divine_value'] = pd.to_numeric(df['divine_value'], errors='coerce')
        df['stack_size'] = pd.to_numeric(df['stack_size'], errors='coerce')
        df['listing_count'] = pd.to_numeric(df['listing_count'], errors='coerce')
        
        # Calculate value per card in stack
        df['value_per_card'] = np.where(df['stack_size'] > 0, 
                                      df['chaos_value'] / df['stack_size'], 
                                      df['chaos_value'])
        
        # Categorize cards by value
        def categorize_card_value(chaos_value):
            if pd.isna(chaos_value) or chaos_value <= 0:
                return 'No Value'
            elif chaos_value < 1:
                return 'Low Value'
            elif chaos_value < 10:
                return 'Medium Value'
            elif chaos_value < 100:
                return 'High Value'
            else:
                return 'Premium Value'
        
        df['value_category'] = df['chaos_value'].apply(categorize_card_value)
        
        # Identify high-value cards
        high_value_cards = df[df['chaos_value'] > 50].copy()
        
        # Create summary statistics
        summary_stats = {
            'total_cards': len(df),
            'avg_chaos_value': df['chaos_value'].mean(),
            'median_chaos_value': df['chaos_value'].median(),
            'high_value_cards': len(high_value_cards),
            'most_expensive_card': df.loc[df['chaos_value'].idxmax(), 'card_name'] if not df.empty else None,
            'most_liquid_card': df.loc[df['listing_count'].idxmax(), 'card_name'] if not df.empty else None,
            'value_distribution': df['value_category'].value_counts().to_dict(),
            'timestamp': datetime.now().isoformat()
        }
        
        # Insert market summary into database
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO poe_market_summary (
                        summary_date, total_cards, avg_card_chaos_value, 
                        high_value_cards, most_liquid_card, summary_data, extracted_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    datetime.now().date(),
                    summary_stats['total_cards'],
                    summary_stats['avg_chaos_value'],
                    summary_stats['high_value_cards'],
                    summary_stats['most_liquid_card'],
                    json.dumps(summary_stats),
                    datetime.now()
                ))
                conn.commit()
        
        # Save backup files
        output_file = f"{OUTPUT_DIR}/cards_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        analysis_data = {
            'summary_stats': summary_stats,
            'top_value_cards': df.nlargest(20, 'chaos_value')[[
                'name', 'chaos_value', 'stack_size', 'value_per_card', 'listing_count'
            ]].to_dict('records'),
            'high_liquidity_cards': df.nlargest(20, 'listing_count')[[
                'name', 'chaos_value', 'listing_count', 'value_category'
            ]].to_dict('records')
        }
        
        with open(output_file, 'w') as f:
            json.dump(analysis_data, f, indent=2)
        
        # Save CSV
        csv_file = f"{OUTPUT_DIR}/cards_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(csv_file, index=False)
        
        log_transformation('cards', len(df))
        print(f"Cards analysis saved to database and backup files: {output_file}, {csv_file}")
        return {'json_file': output_file, 'csv_file': csv_file, 'records': len(df)}
        
    except Exception as e:
        error_msg = f"Error transforming cards data: {str(e)}"
        print(error_msg)
        log_transformation('cards', 0, 'error', str(e))
        raise

def create_market_summary(**context) -> Dict[str, Any]:
    """Create comprehensive market summary from all data sources"""
    try:
        # Load latest analysis data from database
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Get latest summaries for each type
                cur.execute("""
                    SELECT DISTINCT ON (summary_type) summary_type, summary_data, created_at
                    FROM poe_market_summary 
                    WHERE created_at >= NOW() - INTERVAL '24 hours'
                    ORDER BY summary_type, created_at DESC
                """)
                summaries = cur.fetchall()
        
        market_summary = {
            'timestamp': datetime.now().isoformat(),
            'data_sources': {
                'summaries_found': len(summaries),
                'summary_types': [s['summary_type'] for s in summaries]
            }
        }
        
        # Process each summary type
        for summary in summaries:
            summary_data = json.loads(summary['summary_data']) if isinstance(summary['summary_data'], str) else summary['summary_data']
            market_summary[summary['summary_type']] = summary_data
        
        # Get profit opportunities from database
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT item_name, profit_chaos, profit_margin_percent, market_liquidity
                    FROM poe_profit_opportunities 
                    WHERE created_at >= NOW() - INTERVAL '24 hours'
                    ORDER BY profit_margin_percent DESC
                    LIMIT 10
                """)
                top_opportunities = cur.fetchall()
                market_summary['top_profit_opportunities'] = [dict(row) for row in top_opportunities]
        
        # Insert comprehensive market summary into database
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO poe_market_summary (
                        summary_date, summary_data, extracted_at
                    )
                    VALUES (%s, %s, %s)
                """, (
                    datetime.now().date(),
                    json.dumps(market_summary),
                    datetime.now()
                ))
                conn.commit()
        
        # Save backup file
        summary_file = f"{OUTPUT_DIR}/market_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(summary_file, 'w') as f:
            json.dump(market_summary, f, indent=2)
        
        log_transformation('market_summary', len(summaries))
        print(f"Comprehensive market summary saved to database and backup file: {summary_file}")
        return {'summary_file': summary_file}
        
    except Exception as e:
        error_msg = f"Error creating market summary: {str(e)}"
        print(error_msg)
        log_transformation('market_summary', 0, 'error', str(e))
        raise

# Task definitions
create_output_dir_task = PythonOperator(
    task_id='create_output_directory',
    python_callable=create_output_directory,
    dag=dag,
)

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

transform_cards_task = PythonOperator(
    task_id='transform_cards_data',
    python_callable=transform_divination_cards_data,
    dag=dag,
)

create_summary_task = PythonOperator(
    task_id='create_market_summary',
    python_callable=create_market_summary,
    dag=dag,
)

# Task dependencies
create_output_dir_task >> [transform_currency_task, transform_gems_task, transform_cards_task]
[transform_currency_task, transform_gems_task, transform_cards_task] >> create_summary_task