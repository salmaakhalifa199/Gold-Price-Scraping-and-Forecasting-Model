# 💰 Gold Price Forecasting System

> An end-to-end data engineering and machine learning pipeline that automates daily gold price collection, storage, analysis, and forecasting — with an interactive Streamlit dashboard for real-time visualization.

![Python](https://img.shields.io/badge/Python-3.11+-blue?logo=python)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.x-017CEE?logo=apacheairflow)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Neon%20Cloud-336791?logo=postgresql)
![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B?logo=streamlit)
![Docker](https://img.shields.io/badge/Docker-Astronomer-2496ED?logo=docker)
[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://gold-price-scraping-and-forecasting-model.streamlit.app/)

---

## 📋 Table of Contents

- [Overview](#-overview)
- [Key Results](#-key-results)
- [Architecture](#-architecture)
- [Pipeline Tasks](#-pipeline-tasks)
- [Tech Stack](#-tech-stack)
- [Project Structure](#-project-structure)
- [Getting Started](#-getting-started)
- [Dashboard Features](#-dashboard-features)
- [Database Schema](#-database-schema)
- [Forecasting Models](#-forecasting-models)
- [Environment Variables](#-environment-variables)

---

## 🔍 Overview

🚀 **Live Dashboard:** [gold-price-scraping-and-forecasting-model.streamlit.app](https://gold-price-scraping-and-forecasting-model.streamlit.app/)

This project automates the full lifecycle of gold price data — from scraping to forecasting — using a production-grade Apache Airflow pipeline deployed via Astronomer. The system runs daily, scraping gold futures (GC=F) from Yahoo Finance, cleaning and storing the data in a cloud PostgreSQL database, performing exploratory data analysis, and generating 30-day price forecasts using ARIMA and SARIMA models.

**Project Objectives:**
- Automate daily gold price extraction from Yahoo Finance (GC=F ticker)
- Store structured OHLCV data in PostgreSQL with upsert integrity guarantees
- Conduct rigorous EDA to uncover trends, seasonality, and anomalies
- Build and compare ARIMA vs SARIMA models via grid search
- Serve forecasts and historical data in a live Streamlit dashboard

---

## 📊 Key Results

| Metric | Value | Notes |
|---|---|---|
| Data Collected | 365 days (Feb 2025 – Mar 2026) | GC=F Gold Futures via yfinance |
| Price Range | $2,836 – $5,586 | All-time high recorded Jan 2026 |
| Total Return | ~77% | Feb 2025 to Feb 2026 |
| Best Model | ARIMA / SARIMA (auto-selected) | Based on RMSE comparison |
| Forecast Horizon | 30 days ahead | With 95% confidence intervals |
| Dashboard | Live on Streamlit Cloud | Connected to Neon PostgreSQL |

---

## 🏗️ Architecture

```
yfinance (GC=F)
       │
       ▼
┌─────────────────────────────────────────────────────────┐
│                  Apache Airflow DAG                      │
│                  (Astronomer / Docker)                   │
│                                                         │
│  scrape → clean → postgres → EDA                        │
│                           ↘  validate → forecast        │
│                                            ↓            │
│                                    save_forecast_to_db  │
│                                            ↓            │
│                               generate_summary → cleanup│
└─────────────────────────────────────────────────────────┘
       │                              │
       ▼                              ▼
PostgreSQL (Neon Cloud)         CSV files (local)
       │
       ▼
Streamlit Dashboard
```

---

## 🔁 Pipeline Tasks

The DAG `gold_price_scraper` runs on a `@daily` schedule with `catchup=False`.

| # | Task ID | Description |
|---|---|---|
| 1 | `scrape_gold_prices` | Downloads OHLCV data via yfinance; saves raw CSV; pushes via XCom |
| 2 | `clean_data` | Runs cleaning pipeline; handles duplicates, missing values, outliers |
| 3 | `insert_to_postgres` | Upserts cleaned data into `gold_prices` table using `ON CONFLICT` logic |
| 4 | `perform_eda` | Generates 7 statistical visualizations; computes summary stats |
| 5 | `validate_data` | Validates required columns exist and dataset is non-empty |
| 6 | `run_forecasting` | Grid-searches ARIMA & SARIMA params; selects best by RMSE; 30-day forecast |
| 7 | `save_forecast_to_db` | Persists forecast rows and model metrics to PostgreSQL |
| 8 | `generate_summary` | Logs a full pipeline run summary report |
| 9 | `cleanup_old_files` | Deletes CSV files older than 30 days |

**Task Dependency Graph:**

```
scrape_gold_prices
       │
   clean_data
   ┌────┴──────────────┐
   │                   │
insert_to_postgres  perform_eda
   │                   │
   └──────┬────────────┘
          │
      validate_data
          │
      run_forecasting
          │
    save_forecast_to_db
          │
    generate_summary
          │
    cleanup_old_files
```

---

## 🛠️ Tech Stack

| Component | Technology | Purpose |
|---|---|---|
| Data Collection | `yfinance` | Gold futures data via Yahoo Finance |
| Orchestration | Apache Airflow (Astronomer) | DAG scheduling, retry logic, monitoring |
| Data Storage | PostgreSQL (Neon Cloud) | Serverless cloud database |
| Data Processing | `pandas`, `numpy` | DataFrame manipulation |
| Statistical Analysis | `scipy.stats` | ADF test, z-score anomaly detection |
| Time Series Modeling | `statsmodels` | ARIMA, SARIMAX, seasonal decomposition |
| Model Evaluation | `scikit-learn` | MSE, MAE metrics |
| EDA Visualization | `matplotlib`, `seaborn` | Static plots saved as PNG |
| Dashboard | `Streamlit`, `Plotly` | Interactive web app with live DB |
| Containerization | Docker (Astronomer) | Consistent dev/prod environment |
| Language | Python 3.11+ | All components |

---

## 📁 Project Structure

```
airflow/
├── dags/
│   ├── gold_scraper_dag.py        # Main pipeline DAG (9 tasks)
│   └── exampledag.py              # Example astronaut ETL DAG
│
├── include/
│   ├── data/                      # CSV outputs (raw + cleaned, date-stamped)
│   │   └── eda/                   # EDA plots (PNG)
│   └── utils/
│       ├── scrape_gold.py         # yfinance scraper with incremental update logic
│       ├── data_cleaner.py        # GoldDataCleaner class
│       ├── eda_analyzer.py        # GoldPriceEDA class (7 plot types)
│       ├── forecasting_model.py   # ARIMA/SARIMA grid search & forecasting
│       ├── postgres_handler.py    # PostgreSQL CRUD operations
│       └── db_connector.py        # Streamlit-compatible DB connector
│
├── streamlit_app/
│   ├── streamlit_app.py           # Full interactive dashboard
│   ├── requirements.txt
│   └── .streamlit/
│       └── secrets.toml           # DB credentials (not committed)
│
├── tests/
│   └── dags/
│       └── test_dag_example.py    # DAG integrity tests
│
├── Dockerfile                     # Astronomer runtime image
├── requirements.txt               # Python dependencies
└── packages.txt                   # OS-level dependencies
```

---

## 🚀 Getting Started

### Prerequisites

- [Astronomer CLI](https://www.astronomer.io/docs/astro/cli/install-cli) installed
- Docker Desktop running
- PostgreSQL database (local or [Neon Cloud](https://neon.tech))

### 1. Clone the repository

```bash
git clone https://github.com/your-username/gold-price-forecasting.git
cd gold-price-forecasting/airflow
```

### 2. Set up environment variables

Create a `.env` file in the `airflow/` directory:

```env
DB_HOST=host.docker.internal
DB_PORT=5432
DB_NAME=gold_prices_db
DB_USER=postgres
DB_PASSWORD=your_password
DB_SSLMODE=disable
```

### 3. Start Airflow locally

```bash
astro dev start
```

This spins up 5 Docker containers: Postgres, Scheduler, DAG Processor, API Server, and Triggerer. The Airflow UI will open at `http://localhost:8080`.

### 4. Create database tables

Run the following SQL in your PostgreSQL instance before triggering the DAG:

```sql
CREATE TABLE IF NOT EXISTS gold_prices (
    id            SERIAL PRIMARY KEY,
    price_date    DATE UNIQUE NOT NULL,
    open_price    NUMERIC(10, 4),
    high_price    NUMERIC(10, 4),
    low_price     NUMERIC(10, 4),
    close_price   NUMERIC(10, 4),
    volume        BIGINT,
    change_percent NUMERIC(8, 4),
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS gold_forecasts (
    id              SERIAL PRIMARY KEY,
    forecast_date   DATE NOT NULL,
    predicted_price NUMERIC(10, 4),
    lower_bound     NUMERIC(10, 4),
    upper_bound     NUMERIC(10, 4),
    model_name      VARCHAR(50),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS model_performance (
    id            SERIAL PRIMARY KEY,
    model_name    VARCHAR(50),
    rmse          NUMERIC(12, 4),
    mae           NUMERIC(12, 4),
    mape          NUMERIC(8, 4),
    training_date DATE,
    train_size    INTEGER,
    test_size     INTEGER,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 5. Trigger the DAG

In the Airflow UI, enable and trigger the `gold_price_scraper` DAG manually for the first run.

### 6. Run the Streamlit dashboard

```bash
cd streamlit_app
pip install -r requirements.txt
streamlit run streamlit_app.py
```

---

## 📈 Dashboard Features

🔗 **Live App:** [gold-price-scraping-and-forecasting-model.streamlit.app](https://gold-price-scraping-and-forecasting-model.streamlit.app/)

The Streamlit dashboard connects live to PostgreSQL and provides:

**KPI Banner**
- Current price with daily % change delta
- All-time high / low in selected date range
- Total return, 30-day volatility, average volume

**Main Chart**
- Historical close price (blue) + AI forecast (orange dashed)
- 95% confidence interval shading
- Toggle forecast on/off via sidebar

**Additional Charts** (toggleable)
- OHLC Price Comparison
- Candlestick Chart (last 90 days)
- Volume Analysis (color-coded by price direction)
- Moving Averages (7, 30, 90-day)

**Model Performance Section**
- RMSE, MAE, MAPE metrics
- Model comparison table (ARIMA vs SARIMA)
- Forecast vs Actual accuracy chart

**Data Tables**
- Filterable raw data table with CSV download
- Forecast table with CSV download

---

## 🗄️ Database Schema

| Table | Key Columns | Description |
|---|---|---|
| `gold_prices` | `price_date`, `open/high/low/close_price`, `volume` | Historical OHLCV data with upsert support |
| `gold_forecasts` | `forecast_date`, `predicted_price`, `lower/upper_bound`, `model_name` | 30-day forecast outputs |
| `model_performance` | `model_name`, `rmse`, `mae`, `mape`, `train_size`, `test_size` | Model evaluation metrics per run |

---

## 🤖 Forecasting Models

### Model Selection Logic

1. **Stationarity check** — ADF test determines if differencing is needed
2. **Seasonality check** — Seasonal decomposition computes seasonal strength score
3. **ARIMA grid search** — Tests all `(p, d, q)` combinations up to `(5, 2, 5)`, selects by lowest AIC
4. **SARIMA grid search** — Runs if seasonality is detected; tests seasonal parameters with period=12
5. **Auto-selection** — Compares ARIMA vs SARIMA on test-set RMSE; best model wins

### Output

- Test-period predictions with 95% confidence bounds
- 30-day future forecast with confidence intervals
- All results persisted to `gold_forecasts` and `model_performance` tables

---

## ⚙️ Environment Variables

| Variable | Default | Description |
|---|---|---|
| `DB_HOST` | `host.docker.internal` | PostgreSQL host |
| `DB_PORT` | `5432` | PostgreSQL port |
| `DB_NAME` | `gold_prices_db` | Database name |
| `DB_USER` | `postgres` | Database user |
| `DB_PASSWORD` | `postgres` | Database password |
| `DB_SSLMODE` | `disable` | SSL mode (`disable` / `require`) |

> For Neon Cloud, set `DB_SSLMODE=require` and update host/credentials accordingly.

---

## 📄 License

This project is for educational and demonstration purposes.

---

*Pipeline: Apache Airflow • Database: PostgreSQL • Dashboard: Streamlit • Models: ARIMA/SARIMA*
