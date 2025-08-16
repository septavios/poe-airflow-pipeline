# POE Airflow Scripts

## Data Cleanup Script

### `clear_all_data.sh`

This script provides a complete data reset for the POE Airflow pipeline, perfect for fresh testing and development.

#### What it does:
1. **Clears POE Database Tables**:
   - `poe_currency_data`
   - `poe_divination_cards_data`
   - `poe_market_summary`
   - `poe_extraction_log`

2. **Clears Airflow Metadata**:
   - DAG runs and task instances
   - Task history and logs
   - XCom data and variables
   - Job records and notes

3. **Restarts Services**:
   - Airflow scheduler
   - Airflow DAG processor
   - Airflow API server

4. **Verifies Cleanup**:
   - Confirms all tables are empty
   - Provides status feedback

#### Usage:

```bash
# Make sure you're in the POE_Airflow directory
cd /path/to/POE_Airflow

# Run the cleanup script
./scripts/clear_all_data.sh
```

#### Output:
The script provides clear visual feedback with emojis and status messages:
- ✅ Success indicators
- ❌ Error indicators
- 🔍 Verification steps
- 🎉 Completion confirmation

#### When to use:
- Before running fresh tests
- When debugging data pipeline issues
- After making changes to DAG logic
- When you need a clean slate for development

#### Safety:
- The script only affects POE-related data and metadata
- System Airflow configurations remain intact
- Docker containers continue running
- No permanent damage to the setup

#### Troubleshooting:
If the script fails:
1. Ensure Docker containers are running: `docker-compose ps`
2. Check database connectivity: `docker-compose exec postgres psql -U airflow -d airflow -c "\l"`
3. Verify Airflow services are healthy
4. Run individual commands manually if needed