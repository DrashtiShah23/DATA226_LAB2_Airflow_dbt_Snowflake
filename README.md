# 📊 Stock Forecast Analytics Pipeline

**Lab 2: End-to-End Data Analytics with Snowflake, Airflow, dbt & BI Tools**

Authors: Drashti Shah & Dhruv Patel | DATA 226 - Spring 2025

---

## 🎯 Overview

Automated data pipeline that extracts stock market data, performs ML forecasting, and visualizes insights through interactive dashboards.

**Tech Stack:** Apache Airflow · Snowflake · dbt · Preset/Superset · Docker

---

## 🏗️ Architecture

```
yfinance API → Airflow ETL → Snowflake RAW → ML Forecast → dbt Transform → BI Dashboard
```

**Pipeline Flow:**
1. Extract OHLCV data from Yahoo Finance (AAPL, NVDA)
2. Load into Snowflake RAW tables
3. Train ML models & generate 14-day forecasts
4. Transform with dbt (moving averages, returns, volatility)
5. Visualize in Preset/Superset dashboard

---

## 📁 Project Structure

```
├── dags/
│   ├── lab1_etl.py                    # ETL: yfinance → Snowflake
│   ├── lab1_forecast_parallel.py      # ML training & forecasting
│   └── build_elt_with_dbt.py          # dbt orchestration
├── dbt/
│   ├── models/
│   │   ├── input/                     # Staging models
│   │   └── output/stock_summary.sql   # Analytics model
│   ├── snapshots/                     # SCD Type 2 tracking
│   └── dbt_project.yml
└── docker-compose.yaml                # Airflow environment
```

---

## 🚀 Quick Start

### 1. Start Airflow
```bash
docker-compose up -d
```

### 2. Configure Snowflake Connection
In Airflow UI, create connection `snowflake_conn`:
- **Type:** Snowflake
- **Login/Password:** Your credentials
- **Extra:** `{"account": "...", "database": "...", "warehouse": "...", "role": "..."}`

### 3. Set Airflow Variables
```json
{
  "yf_tickers": ["AAPL", "NVDA"],
  "train_lookback_days": "180",
  "forecast_horizon_days": "14"
}
```

### 4. Trigger Pipeline
Run `yfinance_etl` DAG → automatically triggers forecast → triggers dbt

---

## 📊 Key Features

### ETL Pipeline
- **Idempotent loads** with SQL transactions
- **Error handling** with try/catch blocks
- **Dynamic symbol processing** via Airflow Variables

### dbt Transformations
- **Moving averages** (7-day, 30-day)
- **Daily returns** & price ranges
- **Data quality tests** (not_null, accepted_values)
- **SCD Type 2 snapshots** for historical tracking

### ML Forecasting
- **Snowflake native ML** models per symbol
- **14-day predictions** with 95% confidence intervals
- **Parallel execution** using Airflow pools

### BI Dashboard
- **KPIs:** Latest prices, total returns
- **Charts:** Price trends, volatility analysis, moving averages
- **Interactive filters** for date ranges

---

## 📈 Sample Queries

```sql
-- View analytics summary
SELECT * FROM ANALYTICS.STOCK_SUMMARY 
WHERE SYMBOL = 'AAPL' 
ORDER BY DT DESC LIMIT 30;

-- Check forecasts
SELECT * FROM ANALYTICS.AAPL_FORECAST 
WHERE PREDICTION_FOR >= CURRENT_DATE();

-- Compare actual vs predicted
SELECT * FROM ANALYTICS.AAPL_FINAL 
ORDER BY DT DESC;
```

---

## 🧪 dbt Commands

```bash
# Run models
dbt run --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt

# Run tests
dbt test --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt

# Create snapshots
dbt snapshot --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt
```

---

## 📸 Screenshots

See `/screenshots` folder for:
- Airflow DAG runs
- dbt command outputs
- BI dashboard visualizations

---

## 🔒 Security

**Not included in repo:**
- `profiles.yml` (dbt credentials)
- Snowflake passwords
- Airflow secrets

All sensitive data managed via Airflow Connections and environment variables.

---

## 📚 Lab Requirements Checklist

✅ ETL with Airflow  
✅ Idempotent SQL transactions  
✅ dbt models, tests & snapshots  
✅ dbt scheduled via Airflow  
✅ BI dashboard with 2+ visualizations  
✅ Proper use of Airflow connections/variables  
✅ GitHub repository with clear structure

---

## 🙏 Acknowledgements

- Prof. [Name] - DATA 226
- Snowflake for free tier credits
- Apache Airflow, dbt Labs, Preset communities

---

