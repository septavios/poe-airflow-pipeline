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
   
   - Update `POE_LEAGUE` to the current Path of Exile league (e.g., "Settlers", "Crucible").
   
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
│   ├── templates/                # HTML templates
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
POE_LEAGUE=Settlers           # Current Path of Exile league name
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

Update the league name in `dags/poe_unified_pipeline_dag.py`:
```python
LEAGUE = 'Settlers'  # Update to current league
```

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

2. **API Rate Limiting**
   - poe.ninja has rate limits; DAG includes retry logic
   - Adjust schedule if needed

3. **Memory Issues**
   - Ensure sufficient Docker memory allocation
   - Monitor container resource usage

### Logs Location

- Airflow logs: `logs/dag_id=*/`
- Extracted data: `logs/poe_data/`
- Analytics output: `logs/poe_analytics/`

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
```

### Performance Monitoring

```bash
# Check container resource usage
docker stats poe_airflow-airflow-scheduler-1 poe_airflow-postgres-1 --no-stream

# Monitor database connections
docker exec poe_airflow-postgres-1 psql -U airflow -d airflow -c "SELECT count(*) as active_connections FROM pg_stat_activity WHERE state = 'active';"

# Check disk usage
docker exec poe_airflow-postgres-1 du -sh /var/lib/postgresql/data
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