#!/bin/bash

# POE Airflow Data Clearing Script
# This script clears all historical data for fresh testing

echo "🧹 Starting complete data cleanup..."
echo "======================================"

# Step 1: Clear POE database tables
echo "📊 Clearing POE database tables..."
docker-compose exec -T postgres psql -U airflow -d airflow -c "
TRUNCATE TABLE poe_currency_data;
TRUNCATE TABLE poe_skill_gems_data;
TRUNCATE TABLE poe_divination_cards_data;
TRUNCATE TABLE poe_unique_items_data;
TRUNCATE TABLE poe_market_summary;
TRUNCATE TABLE poe_profit_opportunities;
TRUNCATE TABLE poe_extraction_log;
" > /dev/null 2>&1

if [ $? -eq 0 ]; then
    echo "✅ POE database tables cleared successfully"
else
    echo "❌ Failed to clear POE database tables"
    exit 1
fi

# Step 2: Clear Airflow metadata
echo "🔧 Clearing Airflow metadata..."
docker-compose exec -T postgres psql -U airflow -d airflow -c "
TRUNCATE TABLE dag_run CASCADE;
TRUNCATE TABLE task_instance CASCADE;
TRUNCATE TABLE task_instance_history CASCADE;
TRUNCATE TABLE job CASCADE;
TRUNCATE TABLE log CASCADE;
TRUNCATE TABLE xcom CASCADE;
TRUNCATE TABLE rendered_task_instance_fields CASCADE;
TRUNCATE TABLE dag_run_note CASCADE;
TRUNCATE TABLE task_instance_note CASCADE;
TRUNCATE TABLE variable CASCADE;
DELETE FROM variable WHERE key LIKE '%poe%';
" > /dev/null 2>&1

if [ $? -eq 0 ]; then
    echo "✅ Airflow metadata cleared successfully"
else
    echo "❌ Failed to clear Airflow metadata"
    exit 1
fi

# Step 3: Restart Airflow services
echo "🔄 Restarting Airflow services..."
docker-compose restart airflow-scheduler airflow-dag-processor airflow-apiserver > /dev/null 2>&1

if [ $? -eq 0 ]; then
    echo "✅ Airflow services restarted successfully"
else
    echo "❌ Failed to restart Airflow services"
    exit 1
fi

# Step 4: Verify cleanup
echo "🔍 Verifying cleanup..."
COUNTS=$(docker-compose exec -T postgres psql -U airflow -d airflow -c "
SELECT 
    (SELECT COUNT(*) FROM poe_currency_data) as poe_currency,
    (SELECT COUNT(*) FROM poe_divination_cards_data) as poe_cards,
    (SELECT COUNT(*) FROM poe_market_summary) as poe_summary,
    (SELECT COUNT(*) FROM poe_extraction_log) as poe_logs,

    (SELECT COUNT(*) FROM dag_run) as dag_runs,
    (SELECT COUNT(*) FROM task_instance) as task_instances;
" -t 2>/dev/null | grep -E '^[[:space:]]*[0-9]')

if echo "$COUNTS" | grep -q "0.*0.*0.*0.*0.*0"; then
    echo "✅ All data successfully cleared!"
    echo "📈 Ready for fresh data pipeline execution"
else
    echo "⚠️  Some data may still remain. Check manually if needed."
fi

echo "======================================"
echo "🎉 Data cleanup completed!"
echo "💡 You can now run your DAG for fresh testing"
echo "🌐 Check Airflow UI at http://localhost:8080"
echo "📊 Check Dashboard at http://localhost:5001"