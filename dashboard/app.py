from flask import Flask, render_template, jsonify, request
import psycopg2
import os
from datetime import datetime, timedelta
import json

app = Flask(__name__)

from urllib.parse import urlparse

# Database configuration from environment variable
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://airflow:airflow@postgres:5432/airflow')
url = urlparse(DATABASE_URL)

DB_CONFIG = {
    'host': url.hostname or os.getenv('POSTGRES_HOST', 'postgres'),
    'port': url.port or os.getenv('POSTGRES_PORT', '5432'),
    'database': url.path[1:] if url.path else os.getenv('POSTGRES_DB', 'airflow'),
    'user': url.username or os.getenv('POSTGRES_USER', 'airflow'),
    'password': url.password or os.getenv('POSTGRES_PASSWORD', 'airflow')
}

def get_db_connection():
    """Create database connection"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None

def execute_query(query, params=None):
    """Execute database query and return results"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        cursor.execute(query, params or [])
        
        # Get column names
        columns = [desc[0] for desc in cursor.description]
        
        # Fetch all results
        rows = cursor.fetchall()
        
        # Convert to list of dictionaries
        result = []
        for row in rows:
            row_dict = {}
            for i, value in enumerate(row):
                if isinstance(value, datetime):
                    row_dict[columns[i]] = value.isoformat()
                elif hasattr(value, '__class__') and value.__class__.__name__ == 'Decimal':
                    row_dict[columns[i]] = float(value)
                else:
                    row_dict[columns[i]] = value
            result.append(row_dict)
        
        return result
    except Exception as e:
        print(f"Query execution error: {e}")
        return []
    finally:
        if conn:
            conn.close()

@app.route('/')
def dashboard():
    """Main dashboard page"""
    # Get overview statistics
    stats = {}
    
    # Currency data count
    currency_count = execute_query("SELECT COUNT(*) as count FROM poe_currency_data")
    stats['currency_count'] = currency_count[0]['count'] if currency_count else 0
    
    # Skill gems count
    gems_count = execute_query("SELECT COUNT(*) as count FROM poe_skill_gems_data")
    stats['gems_count'] = gems_count[0]['count'] if gems_count else 0
    
    # Divination cards count
    cards_count = execute_query("SELECT COUNT(*) as count FROM poe_divination_cards_data")
    stats['cards_count'] = cards_count[0]['count'] if cards_count else 0
    
    # Profit opportunities count
    profit_count = execute_query("SELECT COUNT(*) as count FROM poe_profit_opportunities")
    stats['profit_count'] = profit_count[0]['count'] if profit_count else 0
    
    # Market summary count
    summary_count = execute_query("SELECT COUNT(*) as count FROM poe_market_summary")
    stats['summary_count'] = summary_count[0]['count'] if summary_count else 0
    
    # Last update time
    last_update = execute_query("""
        SELECT MAX(extracted_at) as last_update 
        FROM (
            SELECT MAX(extracted_at) as extracted_at FROM poe_currency_data
            UNION ALL
            SELECT MAX(extracted_at) as extracted_at FROM poe_skill_gems_data
            UNION ALL
            SELECT MAX(extracted_at) as extracted_at FROM poe_divination_cards_data
        ) as combined
    """)
    stats['last_update'] = last_update[0]['last_update'] if last_update and last_update[0]['last_update'] else 'Never'
    
    return render_template('dashboard.html', stats=stats)

@app.route('/currency')
def currency_data():
    """Currency data page"""
    query = """
        SELECT currency_type_name, chaos_equivalent, 
               receive_sparkline, pay_sparkline, 
               low_confidence, count, extracted_at
        FROM poe_currency_data 
        WHERE extracted_at >= NOW() - INTERVAL '1 day'
        ORDER BY chaos_equivalent DESC
        LIMIT 100
    """
    
    data = execute_query(query)
    return render_template('currency.html', currency_data=data)

@app.route('/gems')
def gems_data():
    """Skill gems data page"""
    page = request.args.get('page', 1, type=int)
    per_page = 50
    offset = (page - 1) * per_page
    
    query = """
        SELECT gem_name, chaos_value, divine_value, 
               count, listing_count, low_confidence, 
               corrupted, extracted_at
        FROM poe_skill_gems_data 
        WHERE extracted_at >= NOW() - INTERVAL '1 day'
        ORDER BY chaos_value DESC
        LIMIT %s OFFSET %s
    """
    
    data = execute_query(query, [per_page, offset])
    
    # Get total count for pagination
    total_query = """
        SELECT COUNT(*) as total 
        FROM poe_skill_gems_data 
        WHERE extracted_at >= NOW() - INTERVAL '1 day'
    """
    total_result = execute_query(total_query)
    total = total_result[0]['total'] if total_result else 0
    
    return render_template('gems.html', 
                         gems_data=data, 
                         page=page, 
                         per_page=per_page, 
                         total=total)

@app.route('/cards')
def cards_data():
    """Divination cards data page"""
    search = request.args.get('search', '')
    
    if search:
        query = """
            SELECT card_name, chaos_value, divine_value, 
                   count, listing_count, low_confidence, 
                   extracted_at
            FROM poe_divination_cards_data 
            WHERE extracted_at >= NOW() - INTERVAL '1 day'
            AND LOWER(card_name) LIKE LOWER(%s)
            ORDER BY chaos_value DESC
            LIMIT 100
        """
        data = execute_query(query, [f'%{search}%'])
    else:
        query = """
            SELECT card_name, chaos_value, divine_value, 
                   count, listing_count, low_confidence, 
                   extracted_at
            FROM poe_divination_cards_data 
            WHERE extracted_at >= NOW() - INTERVAL '1 day'
            ORDER BY chaos_value DESC
            LIMIT 100
        """
        data = execute_query(query)
    
    return render_template('cards.html', cards_data=data, search=search)

@app.route('/profit')
def profit_opportunities():
    """Profit opportunities page"""
    query = """
        SELECT item_name, opportunity_type, current_chaos_value, 
               historical_avg_chaos_value, profit_percentage, 
               confidence_score, recommendation, risk_level, extracted_at
        FROM poe_profit_opportunities 
        WHERE extracted_at >= NOW() - INTERVAL '1 day'
        ORDER BY profit_percentage DESC
        LIMIT 100
    """
    
    data = execute_query(query)
    return render_template('profit.html', profit_data=data)

@app.route('/market-summary')
def market_summary():
    """Market summary page"""
    query = """
    SELECT 
        summary_date,
        total_currencies,
        total_gems,
        total_cards,
        total_unique_items,
        avg_currency_chaos_value,
        avg_gem_chaos_value,
        avg_card_chaos_value,
        avg_unique_chaos_value,
        high_value_currencies,
        high_value_gems,
        high_value_cards,
        most_liquid_currency,
        most_liquid_gem,
        most_liquid_card,
        market_volatility,
        extracted_at
    FROM poe_market_summary 
    ORDER BY summary_date DESC 
    LIMIT 30
    """
    
    summary_data = execute_query(query)
    return render_template('summary.html', summary_data=summary_data)

@app.route('/api/currency-chart')
def currency_chart_data():
    """API endpoint for currency chart data"""
    query = """
        SELECT currency_name, chaos_value
        FROM poe_currency_data 
        WHERE extracted_at >= NOW() - INTERVAL '1 day'
        AND chaos_value > 0
        ORDER BY chaos_value DESC
        LIMIT 20
    """
    
    data = execute_query(query)
    return jsonify(data)

@app.route('/api/profit-chart')
def profit_chart_data():
    """API endpoint for profit opportunities chart data"""
    query = """
        SELECT item_name, profit_percentage
        FROM poe_profit_opportunities 
        WHERE extracted_at >= NOW() - INTERVAL '1 day'
        AND profit_percentage > 0
        ORDER BY profit_percentage DESC
        LIMIT 20
    """
    
    data = execute_query(query)
    return jsonify(data)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)