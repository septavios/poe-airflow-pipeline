from flask import Flask, render_template, jsonify, request
import psycopg2
import os
from datetime import datetime, timedelta
import json
from functools import lru_cache
import time

app = Flask(__name__)

# Simple in-memory cache for Divine Orb values
divine_orb_cache = {}
CACHE_DURATION = 300  # 5 minutes

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

        result = []
        if cursor.description:
            # Get column names when a result set is returned
            columns = [desc[0] for desc in cursor.description]

            # Fetch all results
            rows = cursor.fetchall()

            # Convert to list of dictionaries
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

        # Commit any changes (safe for SELECT queries as well)
        conn.commit()

        return result
    except Exception as e:
        print(f"Query execution error: {e}")
        if conn:
            conn.rollback()
        return []
    finally:
        if conn:
            conn.close()

def get_cached_divine_orb_value(league):
    """Get Divine Orb value with caching"""
    cache_key = f"divine_orb_{league}"
    current_time = time.time()
    
    # Check if we have a cached value that's still valid
    if cache_key in divine_orb_cache:
        cached_data, timestamp = divine_orb_cache[cache_key]
        if current_time - timestamp < CACHE_DURATION:
            return cached_data
    
    # Fetch fresh data
    divine_orb_query = """
        SELECT chaos_value as divine_chaos_value
        FROM poe_currency_data 
        WHERE currency_name = 'Divine Orb'
        AND league = %s
        AND extracted_at >= NOW() - INTERVAL '7 days'
        ORDER BY extracted_at DESC
        LIMIT 1
    """
    
    divine_orb_data = execute_query(divine_orb_query, [league])
    if divine_orb_data and divine_orb_data[0]['divine_chaos_value'] is not None:
        divine_chaos_value = divine_orb_data[0]['divine_chaos_value']
    else:
        divine_chaos_value = 1  # Default fallback value
    
    # Cache the result
    divine_orb_cache[cache_key] = (divine_chaos_value, current_time)
    
    return divine_chaos_value

@app.route('/')
def dashboard():
    """Main dashboard page"""
    # Get league parameter, default to 'Mercenaries'
    league = request.args.get('league', 'Mercenaries')
    
    # Get overview statistics
    stats = {}
    stats['league'] = league
    
    # Currency data count
    currency_count = execute_query("SELECT COUNT(*) as count FROM poe_currency_data WHERE league = %s", [league])
    stats['currency_count'] = currency_count[0]['count'] if currency_count else 0
    
    # Skill gems count
    gems_count = execute_query("SELECT COUNT(*) as count FROM poe_skill_gems_data WHERE league = %s", [league])
    stats['gems_count'] = gems_count[0]['count'] if gems_count else 0
    
    # Divination cards count
    cards_count = execute_query("SELECT COUNT(*) as count FROM poe_divination_cards_data WHERE league = %s", [league])
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
            SELECT MAX(extracted_at) as extracted_at FROM poe_currency_data WHERE league = %s
            UNION ALL
            SELECT MAX(extracted_at) as extracted_at FROM poe_skill_gems_data WHERE league = %s
            UNION ALL
            SELECT MAX(extracted_at) as extracted_at FROM poe_divination_cards_data WHERE league = %s
        ) as combined
    """, [league, league, league])
    stats['last_update'] = last_update[0]['last_update'] if last_update and last_update[0]['last_update'] else 'Never'
    
    return render_template('dashboard.html', stats=stats)

@app.route('/currency')
def currency_data():
    """Currency data page with pagination"""
    # Get parameters
    league = request.args.get('league', 'Mercenaries')
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 50))
    
    # Calculate offset
    offset = (page - 1) * per_page
    
    # Get the Divine Orb value for conversion using cache
    divine_chaos_value = get_cached_divine_orb_value(league)
    
    # Get total count for pagination
    count_query = """
        SELECT COUNT(*) as total
        FROM poe_currency_data 
        WHERE extracted_at >= NOW() - INTERVAL '7 days'
        AND league = %s
    """
    
    total_result = execute_query(count_query, [league])
    total_items = total_result[0]['total'] if total_result else 0
    
    # Get paginated data with enhanced fields including buy/sell prices and images
    query = """
        SELECT currency_name, chaos_value, chaos_equivalent,
               sparkline, pay_sparkline, receive_sparkline,
               low_confidence_pay_sparkline, low_confidence_receive_sparkline,
               low_confidence, count, listing_count, data_point_count,
               includes_secondary, trade_info, extracted_at,
               buy_price, sell_price, price_gap_percentage, price_gap_absolute,
               icon_url, trade_id
        FROM poe_currency_data 
        WHERE extracted_at >= NOW() - INTERVAL '7 days'
        AND league = %s
        ORDER BY chaos_value DESC
        LIMIT %s OFFSET %s
    """
    
    data = execute_query(query, [league, per_page, offset])
    
    # Calculate Divine Value for each currency
    for item in data:
        if item['chaos_value'] is not None and divine_chaos_value and divine_chaos_value > 0:
            item['divine_value'] = round(item['chaos_value'] / divine_chaos_value, 2)
        else:
            item['divine_value'] = 0
    
    # Calculate pagination info
    total_pages = (total_items + per_page - 1) // per_page
    has_prev = page > 1
    has_next = page < total_pages
    
    pagination = {
        'page': page,
        'per_page': per_page,
        'total': total_items,
        'total_pages': total_pages,
        'has_prev': has_prev,
        'has_next': has_next,
        'prev_num': page - 1 if has_prev else None,
        'next_num': page + 1 if has_next else None
    }
    
    return render_template('currency.html', currency_data=data, pagination=pagination, league=league, divine_chaos_value=divine_chaos_value)

@app.route('/gems')
def gems_data():
    """Skill gems data page"""
    # Get league parameter, default to 'Mercenaries'
    league = request.args.get('league', 'Mercenaries')
    page = request.args.get('page', 1, type=int)
    per_page = 50
    offset = (page - 1) * per_page
    
    query = """
        SELECT gem_name, chaos_value, divine_value, 
               count, listing_count, low_confidence, 
               corrupted, extracted_at
        FROM poe_skill_gems_data 
        WHERE extracted_at >= NOW() - INTERVAL '7 days'
        AND league = %s
        ORDER BY chaos_value DESC
        LIMIT %s OFFSET %s
    """
    
    data = execute_query(query, [league, per_page, offset])
    
    # Get total count for pagination
    total_query = """
        SELECT COUNT(*) as total 
        FROM poe_skill_gems_data 
        WHERE extracted_at >= NOW() - INTERVAL '7 days'
        AND league = %s
    """
    total_result = execute_query(total_query, [league])
    total = total_result[0]['total'] if total_result else 0
    
    return render_template('gems.html', 
                         gems_data=data, 
                         page=page, 
                         per_page=per_page, 
                         total=total,
                         league=league)

@app.route('/cards')
def cards_data():
    """Divination cards data page"""
    # Get league parameter, default to 'Mercenaries'
    league = request.args.get('league', 'Mercenaries')
    search = request.args.get('search', '')
    
    if search:
        query = """
            SELECT card_name, chaos_value, divine_value, 
                   count, listing_count, low_confidence, 
                   extracted_at
            FROM poe_divination_cards_data 
            WHERE extracted_at >= NOW() - INTERVAL '7 days'
            AND league = %s
            AND LOWER(card_name) LIKE LOWER(%s)
            ORDER BY chaos_value DESC
            LIMIT 100
        """
        data = execute_query(query, [league, f'%{search}%'])
    else:
        query = """
            SELECT card_name, chaos_value, divine_value, 
                   count, listing_count, low_confidence, 
                   extracted_at
            FROM poe_divination_cards_data 
            WHERE extracted_at >= NOW() - INTERVAL '7 days'
            AND league = %s
            ORDER BY chaos_value DESC
            LIMIT 100
        """
        data = execute_query(query, [league])
    
    return render_template('cards.html', cards_data=data, search=search, league=league)

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

@app.route('/arbitrage')
def arbitrage_opportunities():
    """Display arbitrage opportunities page"""
    try:
        # Get league from query parameter
        league = request.args.get('league', 'mercenaries')
        
        # Fetch arbitrage opportunities data
        arbitrage_data = execute_query("""
            SELECT 
                currency_name,
                buy_price_chaos,
                sell_price_chaos,
                spread_chaos,
                spread_percentage,
                listing_count_difference,
                arbitrage_opportunity,
                pay_listing_count,
                receive_listing_count,
                confidence_score,
                extracted_at
            FROM poe_arbitrage_opportunities 
            WHERE league = %s 
            AND extracted_at >= NOW() - INTERVAL '24 hours'
            ORDER BY spread_percentage DESC, confidence_score DESC
            LIMIT 100
        """, [league])
        
        # Get summary statistics
        summary_stats = execute_query("""
            SELECT 
                COUNT(*) as total_currencies,
                COUNT(CASE WHEN arbitrage_opportunity = true THEN 1 END) as profitable_opportunities,
                AVG(spread_percentage) as avg_spread_percentage,
                MAX(spread_percentage) as max_spread_percentage,
                AVG(confidence_score) as avg_confidence_score
            FROM poe_arbitrage_opportunities 
            WHERE league = %s 
            AND extracted_at >= NOW() - INTERVAL '24 hours'
        """, [league])
        
        stats = summary_stats[0] if summary_stats else {
            'total_currencies': 0,
            'profitable_opportunities': 0,
            'avg_spread_percentage': 0,
            'max_spread_percentage': 0,
            'avg_confidence_score': 0
        }
        
        return render_template('arbitrage.html', 
                             arbitrage_data=arbitrage_data, 
                             stats=stats,
                             league=league)
    except Exception as e:
        print(f"Error in arbitrage_opportunities: {e}")
        return render_template('arbitrage.html', 
                             arbitrage_data=[], 
                             stats={},
                             league='mercenaries')

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
    # Get league parameter, default to 'Mercenaries'
    league = request.args.get('league', 'Mercenaries')
    
    query = """
        SELECT currency_name, chaos_value
        FROM poe_currency_data 
        WHERE extracted_at >= NOW() - INTERVAL '1 day'
        AND league = %s
        AND chaos_value > 0
        ORDER BY chaos_value DESC
        LIMIT 20
    """
    
    data = execute_query(query, [league])
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

@app.route('/api/arbitrage-chart')
def arbitrage_chart_data():
    """API endpoint for arbitrage chart data"""
    try:
        league = request.args.get('league', 'mercenaries')
        
        data = execute_query("""
            SELECT 
                currency_name, 
                spread_percentage, 
                confidence_score,
                buy_price_chaos,
                sell_price_chaos
            FROM poe_arbitrage_opportunities 
            WHERE league = %s 
            AND arbitrage_opportunity = true
            AND extracted_at >= NOW() - INTERVAL '24 hours'
            ORDER BY spread_percentage DESC 
            LIMIT 20
        """, [league])
        
        return jsonify(data)
    except Exception as e:
        print(f"Error in arbitrage_chart_data: {e}")
        return jsonify([])

@app.route('/api/leagues')
def get_leagues():
    """API endpoint to fetch available leagues"""
    query = """
        SELECT league_name, display_name, is_active, description
        FROM poe_leagues 
        ORDER BY is_active DESC, display_name ASC
    """
    
    data = execute_query(query)
    return jsonify(data)

@app.route('/api/leagues', methods=['POST'])
def add_league():
    """API endpoint to add a new league"""
    data = request.get_json()
    
    if not data or 'league_name' not in data or 'display_name' not in data:
        return jsonify({'error': 'league_name and display_name are required'}), 400
    
    query = """
        INSERT INTO poe_leagues (league_name, display_name, is_active, description)
        VALUES (%s, %s, %s, %s)
        RETURNING id, league_name, display_name, is_active
    """
    
    try:
        result = execute_query(query, [
            data['league_name'],
            data['display_name'],
            data.get('is_active', True),
            data.get('description', '')
        ])
        return jsonify(result[0] if result else {}), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/leagues/<league_name>', methods=['PUT'])
def update_league(league_name):
    """API endpoint to update a league"""
    data = request.get_json()
    
    if not data:
        return jsonify({'error': 'Request body is required'}), 400
    
    # Build dynamic update query
    update_fields = []
    params = []
    
    if 'display_name' in data:
        update_fields.append('display_name = %s')
        params.append(data['display_name'])
    
    if 'is_active' in data:
        update_fields.append('is_active = %s')
        params.append(data['is_active'])
    
    if 'description' in data:
        update_fields.append('description = %s')
        params.append(data['description'])
    
    if not update_fields:
        return jsonify({'error': 'No valid fields to update'}), 400
    
    params.append(league_name)
    
    query = f"""
        UPDATE poe_leagues 
        SET {', '.join(update_fields)}, updated_at = CURRENT_TIMESTAMP
        WHERE league_name = %s
        RETURNING id, league_name, display_name, is_active, description
    """
    
    try:
        result = execute_query(query, params)
        if not result:
            return jsonify({'error': 'League not found'}), 404
        return jsonify(result[0])
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/leagues/<league_name>', methods=['DELETE'])
def delete_league(league_name):
    """API endpoint to delete a league"""
    # Check if league has associated data
    check_query = """
        SELECT COUNT(*) as count FROM (
            SELECT 1 FROM poe_currency_data WHERE league = %s LIMIT 1
            UNION ALL
            SELECT 1 FROM poe_skill_gems_data WHERE league = %s LIMIT 1
            UNION ALL
            SELECT 1 FROM poe_divination_cards_data WHERE league = %s LIMIT 1
            UNION ALL
            SELECT 1 FROM poe_unique_items_data WHERE league = %s LIMIT 1
        ) as combined
    """
    
    try:
        result = execute_query(check_query, [league_name, league_name, league_name, league_name])
        if result and result[0]['count'] > 0:
            return jsonify({
                'error': 'Cannot delete league with associated data. Set is_active to false instead.'
            }), 400
        
        delete_query = "DELETE FROM poe_leagues WHERE league_name = %s RETURNING league_name"
        result = execute_query(delete_query, [league_name])
        
        if not result:
            return jsonify({'error': 'League not found'}), 404
        
        return jsonify({'message': f'League {league_name} deleted successfully'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    # Enable debug mode for hot-reload in development
    debug_mode = os.getenv('FLASK_DEBUG', 'False').lower() == 'true'
    app.run(host='0.0.0.0', port=5000, debug=debug_mode)