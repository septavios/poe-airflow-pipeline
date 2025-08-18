# Path of Exile Market Data Pipeline

A comprehensive data pipeline built with Apache Airflow for extracting, transforming, and analyzing Path of Exile market data from the poe.ninja API.

## 🎯 Overview

This project provides an automated data pipeline that:
- Extracts real-time market data from Path of Exile's economy API
- Stores data in a PostgreSQL database
- Performs data transformation and analysis
- Generates market insights and analytics
- Provides Jupyter notebooks for data science exploration

## 🏗️ Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   poe.ninja     │───▶│   Airflow DAGs   │───▶│   PostgreSQL    │
│      API        │    │                  │    │    Database     │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌──────────────────┐
                       │   Analytics &    │
                       │   Jupyter        │
                       └──────────────────┘
```

### Components

- **Unified Pipeline DAG**: Single DAG that handles both data extraction and transformation in sequence
- **PostgreSQL Database**: Stores all market data with proper indexing
- **Web Dashboard**: Flask-based dashboard for data visualization and monitoring
- **Jupyter Notebooks**: Interactive data analysis and visualization
- **Docker Compose**: Containerized deployment with all services

## 📊 Data Sources

The pipeline extracts data from poe.ninja API endpoints:
- Currency exchange rates
- Skill gem prices
- Divination card values
- Unique item prices
- Market trends and sparklines

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose
- At least 4GB RAM available
- Python 3.8+ (for local development)

### Installation

1. **Clone the repository**
   ```bash
   git clone <your-repo-url>
   cd POE_Airflow
   ```

2. **Set up environment variables**
   
   Create your environment file from the template:
   ```bash
   cp .env.example .env
   ```
   
   **Important**: Edit the `.env` file with your specific configurations:
   
   **Required Changes:**
   - Generate a new Fernet key for Airflow encryption:
     ```bash
     python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
     ```
     Replace `your-fernet-key-here` with the generated key.
   
   - Generate a new secret key for Airflow webserver:
     ```bash
     python -c "import secrets; print(secrets.token_urlsafe(32))"
     ```
     Replace `your-secret-key-here` with the generated key.
   
   - Update `POE_LEAGUE` to the current Path of Exile league (e.g., "mercenaries", "Settlers").
   
   **Optional Changes:**
   - Modify database credentials if you prefer different usernames/passwords
   - Adjust Airflow configuration settings based on your needs
   
   **Security Note**: Never commit your actual `.env` file to version control. The `.gitignore` file is configured to exclude it.

3. **Start the services**
   ```bash
   docker-compose up -d
   ```

4. **Initialize the database**
   ```bash
   docker exec poe_airflow-postgres-1 psql -U airflow -d airflow -f /docker-entrypoint-initdb.d/init_database.sql
   ```

5. **Access the services**
   - Airflow UI: http://localhost:8080 (admin/admin)
   - Web Dashboard: http://localhost:5001
   - Jupyter: http://localhost:8888
   - PostgreSQL: localhost:5432

## 📁 Project Structure

```
POE_Airflow/
├── dags/                          # Airflow DAG definitions
│   └── poe_unified_pipeline_dag.py # Unified data extraction and transformation pipeline
├── sql/                           # Database schemas and scripts
│   └── init_database.sql         # Database initialization
├── scripts/                       # Utility scripts
│   └── init_db.py                # Database setup script
├── notebooks/                     # Jupyter notebooks
│   └── poe_market_analysis.ipynb # Market analysis notebook
├── config/                        # Configuration files
│   └── airflow.cfg               # Airflow configuration
├── dashboard/                     # Web dashboard application
│   ├── app.py                    # Flask application
│   ├── Dockerfile                # Dashboard container configuration
│   ├── templates/                # HTML templates
│   │   ├── base.html            # Base template with styling
│   │   ├── dashboard.html       # Main dashboard page
│   │   ├── currency.html        # Currency data view
│   │   ├── gems.html            # Skill gems data view
│   │   ├── cards.html           # Divination cards view
│   │   ├── profit.html          # Profit opportunities view
│   │   └── summary.html         # Market summary view
│   └── requirements.txt          # Dashboard dependencies
├── logs/                          # Application logs and data
│   ├── poe_data/                 # Raw extracted data
│   └── poe_analytics/            # Processed analytics
├── docker-compose.yaml           # Docker services definition
├── .env                          # Environment variables
└── README.md                     # This file
```

## 🔧 Configuration

### Environment Variables

The `.env` file contains all configuration settings for the application. Here's a breakdown of the key variables:

#### Database Configuration
```bash
POSTGRES_USER=airflow          # PostgreSQL username
POSTGRES_PASSWORD=airflow      # PostgreSQL password (change for production)
POSTGRES_DB=airflow           # Database name
POSTGRES_HOST=postgres        # Database host (container name in Docker)
POSTGRES_PORT=5432           # Database port
```

#### Airflow Core Configuration
```bash
AIRFLOW__CORE__EXECUTOR=LocalExecutor                    # Airflow executor type
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow  # Database connection string
AIRFLOW__CORE__FERNET_KEY=your-fernet-key-here         # Encryption key (MUST be changed)
AIRFLOW__WEBSERVER__SECRET_KEY=your-secret-key-here     # Webserver secret (MUST be changed)
AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true        # Pause new DAGs by default
AIRFLOW__CORE__LOAD_EXAMPLES=false                     # Don't load example DAGs
```

#### API Configuration
```bash
POE_LEAGUE=mercenaries        # Current Path of Exile league name
```

#### Dashboard Configuration
```bash
DASHBOARD_PORT=5001           # Port for the web dashboard
```

#### Security Requirements

⚠️ **Critical**: The following keys MUST be changed before deployment:

1. **Fernet Key**: Used for encrypting sensitive data in Airflow
   ```bash
   python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
   ```

2. **Secret Key**: Used for securing the Airflow webserver
   ```bash
   python -c "import secrets; print(secrets.token_urlsafe(32))"
   ```

3. **Database Credentials**: Change default passwords for production use

### League Configuration

Update the league name in your `.env` file:
```bash
POE_LEAGUE=mercenaries  # Set to current league
```

The DAG will automatically read this value from the environment variable. If not set, it defaults to 'Settlers'.

## 📈 DAG Overview

### Unified Pipeline DAG (`poe_unified_pipeline`)

**Schedule**: Every 6 hours  
**Extraction Tasks**:
- `fetch_currency_data`: Extract currency exchange rates
- `fetch_skill_gems_data`: Extract skill gem prices
- `fetch_divination_cards_data`: Extract divination card values
- `fetch_unique_items_data`: Extract unique item prices

**Transformation Tasks**:
- `analyze_currency_trends`: Currency market analysis
- `analyze_gem_market`: Skill gem market insights
- `analyze_card_market`: Divination card trends
- `analyze_profit_opportunities`: Identify profitable trading opportunities
- `generate_market_summary`: Overall market summary

**Task Dependencies**: Extraction tasks run in parallel, followed by transformation tasks that depend on the extracted data.

## 🚀 DAG Operations

### Manual DAG Execution

To manually trigger the POE unified pipeline DAG:

```bash
# Trigger the DAG manually
docker exec poe_airflow-airflow-scheduler-1 airflow dags trigger poe_unified_pipeline
```

### Checking DAG Status

**List recent DAG runs:**
```bash
# View recent DAG runs and their status
docker exec poe_airflow-airflow-scheduler-1 airflow dags list-runs poe_unified_pipeline
```

**Check individual task status:**
```bash
# List all tasks in the DAG
docker exec poe_airflow-airflow-scheduler-1 airflow tasks list poe_unified_pipeline

# Check specific task logs (replace YYYY-MM-DD with actual date)
docker exec poe_airflow-airflow-scheduler-1 airflow tasks log poe_unified_pipeline extract_currency_data YYYY-MM-DD 1
```

**Monitor data extraction:**
```bash
# Check database for latest extractions
docker exec poe_airflow-postgres-1 psql -U airflow -d airflow -c "SELECT extraction_type, status, records_processed, extracted_at FROM poe_extraction_log ORDER BY extracted_at DESC LIMIT 10;"

# Check total records in currency table
docker exec poe_airflow-postgres-1 psql -U airflow -d airflow -c "SELECT COUNT(*) as total_records, MAX(extracted_at) as latest_extraction FROM poe_currency_data;"
```

**Access monitoring interfaces:**
- **Airflow Web UI**: http://localhost:8080 (admin/admin)
- **POE Dashboard**: http://localhost:5001
- **Database**: localhost:5432 (airflow/airflow)

### DAG Scheduling

The DAG runs automatically every 6 hours. To modify the schedule:

1. Edit the `schedule` parameter in `dags/poe_unified_pipeline_dag.py`
2. Restart the Airflow services: `docker-compose restart`

## 🗄️ Database Schema

### Tables

- **poe_currency_data**: Currency exchange rates and market data
- **poe_skill_gems_data**: Skill gem prices with level/quality variants
- **poe_divination_cards_data**: Divination card values and stack sizes
- **poe_unique_items_data**: Unique item prices with variants
- **poe_market_summary**: Aggregated market analytics
- **poe_profit_opportunities**: Identified profitable trading opportunities
- **poe_extraction_log**: Pipeline execution logs

### Key Indexes

- Currency name and chaos value indexes for fast lookups
- Timestamp indexes for time-series analysis
- Composite indexes for variant-based queries

## 🖥️ Web Dashboard

The integrated web dashboard provides real-time visualization of Path of Exile market data with a modern, dark-themed interface optimized for readability.

### Dashboard Features

**Main Dashboard** (`http://localhost:5001`)
- **Statistics Overview**: Key metrics including total currencies, skill gems, divination cards, and unique items
- **Market Summary**: Latest update timestamps and data freshness indicators
- **Top Currency Values**: Visual charts showing highest-value currencies
- **Profit Opportunities**: Quick access to profitable trading opportunities
- **Navigation**: Easy access to detailed views for each data category

**Currency Data View** (`http://localhost:5001/currency`)
- Real-time currency exchange rates with chaos orb values
- Confidence indicators for data reliability
- Price trend sparklines for visual trend analysis
- Last updated timestamps for data freshness
- Summary statistics including total currencies and highest values

**Skill Gems View** (`http://localhost:5001/gems`)
- Comprehensive gem pricing with level and quality variants
- Corrupted vs non-corrupted gem comparisons
- Market confidence indicators
- Detailed gem statistics and trends

**Divination Cards View** (`http://localhost:5001/cards`)
- Card values with stack size information
- Confidence ratings for price accuracy
- Market trend indicators
- Summary statistics for card market overview

**Profit Opportunities View** (`http://localhost:5001/profit`)
- Identified arbitrage opportunities across markets
- Profit percentage calculations
- Current market values and potential returns
- Risk assessment through confidence indicators

**Market Summary View** (`http://localhost:5001/summary`)
- Aggregated market analytics and insights
- Historical trend analysis
- Market health indicators
- Cross-category comparisons

### Dashboard Technology Stack

- **Backend**: Flask web framework with PostgreSQL integration
- **Frontend**: Bootstrap 5 with custom dark theme
- **Styling**: Modern CSS with Inter font family for improved readability
- **Data Visualization**: Integrated charts and sparklines
- **Responsive Design**: Mobile-friendly interface

### Dashboard Configuration

The dashboard is automatically configured through Docker Compose:

```yaml
poe-dashboard:
  build: ./dashboard
  ports:
    - "5001:5000"
  environment:
    - DATABASE_URL=postgresql://airflow:airflow@postgres:5432/airflow
  depends_on:
    - postgres
```

### Customizing the Dashboard

**Theme Customization**: Modify `dashboard/templates/base.html` to adjust:
- Color schemes and typography
- Layout and spacing
- Interactive elements and hover effects

**Adding New Views**: Create new templates in `dashboard/templates/` and add corresponding routes in `dashboard/app.py`

**Data Integration**: The dashboard automatically connects to the PostgreSQL database and displays real-time data from all pipeline extractions.

## 📊 Analytics & Insights

### Available Metrics

- **Currency Analysis**: Exchange rate trends, volatility, liquidity scores
- **Gem Market**: Price trends by level/quality, profitable opportunities
- **Card Market**: Value distribution, stack size analysis
- **Unique Items**: Price ranges, variant comparisons
- **Profit Opportunities**: Cross-market arbitrage identification

### Jupyter Notebooks

The included notebook provides:
- Interactive data exploration
- Market trend visualization
- Statistical analysis
- Custom query examples

## 🔍 Monitoring & Logs

### Airflow Monitoring

- DAG execution status in Airflow UI
- Task logs and error tracking
- Retry mechanisms for failed tasks

### Data Quality

- Extraction logs with record counts
- Data validation and error handling
- Backup JSON files for all extractions

## 🛠️ Development

### Adding New Data Sources

1. Create extraction function in `poe_unified_pipeline_dag.py`
2. Add corresponding database table in `sql/init_database.sql`
3. Create transformation logic in the same DAG file
4. Update task dependencies in the DAG

### Local Development

```bash
# Install dependencies
pip install apache-airflow pandas psycopg2-binary requests

# Run tests
python -m pytest tests/

# Format code
black dags/ scripts/
```

## 🚨 Troubleshooting

### Common Issues

1. **Database Connection Errors**
   - Verify PostgreSQL container is running
   - Check database credentials in `.env`
   - Test connection: `docker exec poe_airflow-postgres-1 pg_isready -U airflow`

2. **API Rate Limiting**
   - poe.ninja has rate limits; DAG includes retry logic
   - Adjust schedule if needed
   - Check API response in logs for rate limit messages

3. **Memory Issues**
   - Ensure sufficient Docker memory allocation
   - Monitor container resource usage
   - Restart containers if memory usage is high

4. **Dashboard Not Loading**
   - Check if dashboard container is running: `docker ps | grep dashboard`
   - Verify port 5001 is not in use: `lsof -i :5001`
   - Check dashboard logs: `docker logs poe_airflow-poe-dashboard-1`
   - Restart dashboard: `docker-compose restart poe-dashboard`

5. **Data Not Updating**
   - Check DAG status in Airflow UI
   - Verify DAG is unpaused: `docker exec poe_airflow-airflow-scheduler-1 airflow dags state poe_unified_pipeline`
   - Check extraction logs for API errors
   - Manually trigger DAG: `docker exec poe_airflow-airflow-scheduler-1 airflow dags trigger poe_unified_pipeline`

### Logs Location

- Airflow logs: `logs/dag_id=*/`
- Extracted data: `logs/poe_data/`
- Analytics output: `logs/poe_analytics/`
- Dashboard logs: `docker logs poe_airflow-poe-dashboard-1`

### Debug Commands

#### Quick Health Check
```bash
# Check all services status
docker-compose ps

# Quick data validation
docker exec poe_airflow-postgres-1 psql -U airflow -d airflow -c "SELECT COUNT(*) FROM poe_currency_data WHERE extracted_at >= NOW() - INTERVAL '1 hour';"

# Check latest DAG run
docker exec poe_airflow-airflow-scheduler-1 airflow dags list-runs -d poe_unified_pipeline --limit 1
```

#### Dashboard Debugging
```bash
# Check dashboard container status
docker inspect poe_airflow-poe-dashboard-1 --format='{{.State.Status}}'

# Test dashboard database connection
docker exec poe_airflow-poe-dashboard-1 python -c "import psycopg2; conn = psycopg2.connect('postgresql://airflow:airflow@postgres:5432/airflow'); print('Connection successful')"

# Check dashboard port binding
docker port poe_airflow-poe-dashboard-1

# View dashboard application logs
docker logs poe_airflow-poe-dashboard-1 --follow
```

#### Data Pipeline Debugging
```bash
# Check for failed tasks in last 24 hours
docker exec poe_airflow-airflow-scheduler-1 airflow tasks states-for-dag-run poe_unified_pipeline $(docker exec poe_airflow-airflow-scheduler-1 airflow dags list-runs -d poe_unified_pipeline --limit 1 --output table | tail -1 | awk '{print $2}')

# View specific task logs
docker exec poe_airflow-airflow-scheduler-1 airflow tasks log poe_unified_pipeline fetch_currency_data $(date +%Y-%m-%d) 1

# Check API connectivity
docker exec poe_airflow-airflow-scheduler-1 curl -s "https://poe.ninja/api/data/currencyoverview?league=mercenaries&type=Currency" | jq '.lines | length'

# Validate database schema
docker exec poe_airflow-postgres-1 psql -U airflow -d airflow -c "\dt poe_*"
```

#### Performance Debugging
```bash
# Check database query performance
docker exec poe_airflow-postgres-1 psql -U airflow -d airflow -c "SELECT query, mean_exec_time, calls FROM pg_stat_statements WHERE query LIKE '%poe_%' ORDER BY mean_exec_time DESC LIMIT 5;"

# Monitor real-time container stats
docker stats --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}\t{{.BlockIO}}"

# Check disk space usage
docker system df

# Analyze database size
docker exec poe_airflow-postgres-1 psql -U airflow -d airflow -c "SELECT schemaname,tablename,attname,n_distinct,correlation FROM pg_stats WHERE tablename LIKE 'poe_%';"
```

## 🐳 Docker Commands Reference

### Container Management

```bash
# Start all services
docker-compose up -d

# Stop all services
docker-compose down

# View running containers
docker-compose ps

# View container logs
docker-compose logs airflow-scheduler
docker-compose logs airflow-webserver
docker-compose logs postgres
```

### Airflow DAG Management

```bash
# List all DAGs
docker exec poe_airflow-airflow-scheduler-1 airflow dags list

# Trigger a DAG manually
docker exec poe_airflow-airflow-scheduler-1 airflow dags trigger poe_unified_pipeline

# Pause/Unpause DAGs
docker exec poe_airflow-airflow-scheduler-1 airflow dags pause poe_unified_pipeline
docker exec poe_airflow-airflow-scheduler-1 airflow dags unpause poe_unified_pipeline

# List DAG runs
docker exec poe_airflow-airflow-scheduler-1 airflow dags list-runs -d poe_unified_pipeline --limit 5
```

### Database Operations

```bash
# Connect to PostgreSQL
docker exec -it poe_airflow-postgres-1 psql -U airflow -d airflow

# Check table record counts
docker exec poe_airflow-postgres-1 psql -U airflow -d airflow -c "SELECT 'poe_currency_data' as table_name, COUNT(*) as record_count FROM poe_currency_data UNION ALL SELECT 'poe_skill_gems_data' as table_name, COUNT(*) as record_count FROM poe_skill_gems_data;"

# View recent data
docker exec poe_airflow-postgres-1 psql -U airflow -d airflow -c "SELECT * FROM poe_market_summary ORDER BY summary_date DESC LIMIT 3;"

# Check profit opportunities
docker exec poe_airflow-postgres-1 psql -U airflow -d airflow -c "SELECT item_name, current_chaos_value, profit_percentage FROM poe_profit_opportunities ORDER BY profit_percentage DESC LIMIT 10;"
```

### Log Management

```bash
# Find recent DAG run logs
docker exec poe_airflow-airflow-scheduler-1 find /opt/airflow/logs/dag_id=poe_unified_pipeline -name "*.log" | tail -5

# View specific task log
docker exec poe_airflow-airflow-scheduler-1 cat "/opt/airflow/logs/dag_id=poe_unified_pipeline/run_id=manual__2025-08-15T17:25:14.082210+00:00_K6S1Bmpl/task_id=fetch_currency_data/attempt=1.log"

# Check for errors in logs
docker exec poe_airflow-airflow-scheduler-1 find /opt/airflow/logs -name "*.log" -exec grep -l "ERROR\|FAILED" {} \;
```

### Debugging Commands

```bash
# Check Airflow scheduler status
docker exec poe_airflow-airflow-scheduler-1 airflow jobs check --job-type SchedulerJob

# Test database connection
docker exec poe_airflow-airflow-scheduler-1 airflow db check

# List Airflow connections
docker exec poe_airflow-airflow-scheduler-1 airflow connections list

# Check DAG import errors
docker exec poe_airflow-airflow-scheduler-1 airflow dags list-import-errors
```

### Data Validation

```bash
# Check data freshness (last extraction time)
docker exec poe_airflow-postgres-1 psql -U airflow -d airflow -c "SELECT 'poe_currency_data' as table_name, MAX(extracted_at) as last_update FROM poe_currency_data UNION ALL SELECT 'poe_skill_gems_data' as table_name, MAX(extracted_at) as last_update FROM poe_skill_gems_data;"

# Verify data integrity
docker exec poe_airflow-postgres-1 psql -U airflow -d airflow -c "SELECT COUNT(*) as total_gems, COUNT(DISTINCT gem_name) as unique_gems FROM poe_skill_gems_data WHERE extracted_at >= NOW() - INTERVAL '1 day';"

# Check for duplicate records
docker exec poe_airflow-postgres-1 psql -U airflow -d airflow -c "SELECT gem_name, COUNT(*) as count FROM poe_skill_gems_data GROUP BY gem_name HAVING COUNT(*) > 1 LIMIT 10;"

# Validate profit opportunities data
docker exec poe_airflow-postgres-1 psql -U airflow -d airflow -c "SELECT COUNT(*) as total_opportunities, AVG(profit_percentage) as avg_profit FROM poe_profit_opportunities WHERE created_at >= NOW() - INTERVAL '1 day';"

# Check market summary generation
docker exec poe_airflow-postgres-1 psql -U airflow -d airflow -c "SELECT summary_date, total_items_tracked, avg_chaos_value FROM poe_market_summary ORDER BY summary_date DESC LIMIT 5;"
```

### Performance Monitoring

```bash
# Check container resource usage
docker stats poe_airflow-airflow-scheduler-1 poe_airflow-postgres-1 poe_airflow-poe-dashboard-1 --no-stream

# Monitor database connections
docker exec poe_airflow-postgres-1 psql -U airflow -d airflow -c "SELECT count(*) as active_connections FROM pg_stat_activity WHERE state = 'active';"

# Check disk usage
docker exec poe_airflow-postgres-1 du -sh /var/lib/postgresql/data

# Monitor dashboard container logs
docker logs poe_airflow-poe-dashboard-1 --tail 50

# Check dashboard health
curl -f http://localhost:5001/health || echo "Dashboard not responding"
```

## 📝 API Documentation

### poe.ninja API Endpoints

- Currency: `https://poe.ninja/api/data/currencyoverview?league={league}&type=Currency`
- Skill Gems: `https://poe.ninja/api/data/itemoverview?league={league}&type=SkillGem`
- Divination Cards: `https://poe.ninja/api/data/itemoverview?league={league}&type=DivinationCard`
- Unique Items: `https://poe.ninja/api/data/itemoverview?league={league}&type=UniqueJewel`

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- [poe.ninja](https://poe.ninja) for providing the market data API
- [Apache Airflow](https://airflow.apache.org/) for the workflow orchestration
- [Path of Exile](https://www.pathofexile.com/) community for the amazing game

## 📞 Support

For issues and questions:
1. Check the troubleshooting section
2. Review Airflow logs
3. Open an issue on GitHub

---

**Happy Trading, Exile!** 🗡️⚡