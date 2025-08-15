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

- **Data Extraction DAG**: Fetches currency, skill gems, divination cards, and unique items data
- **Data Transformation DAG**: Processes and analyzes the extracted data
- **PostgreSQL Database**: Stores all market data with proper indexing
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
   - Jupyter: http://localhost:8888
   - PostgreSQL: localhost:5432

## 📁 Project Structure

```
POE_Airflow/
├── dags/                          # Airflow DAG definitions
│   ├── poe_data_extraction_dag.py # Data extraction pipeline
│   └── poe_data_transformation_dag.py # Data transformation pipeline
├── sql/                           # Database schemas and scripts
│   └── init_database.sql         # Database initialization
├── scripts/                       # Utility scripts
│   └── init_db.py                # Database setup script
├── notebooks/                     # Jupyter notebooks
│   └── poe_market_analysis.ipynb # Market analysis notebook
├── config/                        # Configuration files
│   └── airflow.cfg               # Airflow configuration
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

Update the league name in `dags/poe_data_extraction_dag.py`:
```python
LEAGUE = 'Settlers'  # Update to current league
```

## 📈 DAGs Overview

### Data Extraction DAG (`poe_data_extraction`)

**Schedule**: Every 6 hours  
**Tasks**:
- `fetch_currency_data`: Extract currency exchange rates
- `fetch_skill_gems_data`: Extract skill gem prices
- `fetch_divination_cards_data`: Extract divination card values
- `fetch_unique_items_data`: Extract unique item prices
- `analyze_profit_opportunities`: Identify profitable trading opportunities

### Data Transformation DAG (`poe_data_transformation`)

**Schedule**: Every 6 hours (after extraction)  
**Tasks**:
- `analyze_currency_trends`: Currency market analysis
- `analyze_gem_market`: Skill gem market insights
- `analyze_card_market`: Divination card trends
- `generate_market_summary`: Overall market summary

## 🗄️ Database Schema

### Tables

- **currency_data**: Currency exchange rates and market data
- **skill_gems_data**: Skill gem prices with level/quality variants
- **divination_cards_data**: Divination card values and stack sizes
- **unique_items_data**: Unique item prices with variants
- **market_summary**: Aggregated market analytics
- **extraction_log**: Pipeline execution logs

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

1. Create extraction function in `poe_data_extraction_dag.py`
2. Add corresponding database table in `sql/init_database.sql`
3. Create transformation logic in `poe_data_transformation_dag.py`
4. Update DAG dependencies

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