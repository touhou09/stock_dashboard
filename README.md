# Stock Dashboard - Dividend Stock Data Pipeline based on Medallion Architecture

A scalable data pipeline project for processing US dividend stock data and S&P 500 data. Built using the Medallion Architecture pattern with Bronze → Silver → Gold layers.

Access the live service through the link below:  
[Dashboard](https://stock-dashboard-front-240269058578.asia-northeast3.run.app/)

![img1](/img/img1.png)

## 🏗️ Architecture Overview

### Medallion Architecture
- **Bronze Layer**: Raw data collection and storage (Delta Lake)
- **Silver Layer**: Data cleansing and dividend metrics calculation (Delta Lake)
- **Gold Layer**: Analytical view creation (BigQuery View)

### Technology Stack
- **Language**: Python 3.12
- **Package Management**: uv
- **Data Storage**: Delta Lake (Google Cloud Storage)
- **Data Collection**: yfinance, Wikipedia API
- **Data Processing**: pandas, pyarrow
- **Deployment**: Docker, Cloud Run

## 📁 Project Structure

```
stock_dashboard/
├── src/
│   ├── app/                           # Application layer
│   │   ├── main.py                   # Main execution file
│   │   ├── bronze/                   # Bronze Layer
│   │   │   ├── bronze_layer_delta.py # Bronze Layer core logic
│   │   │   └── bronze_layer_orchestrator.py # Bronze Layer orchestration
│   │   └── silver/                   # Silver Layer
│   │       └── silver_layer_delta.py # Silver Layer core logic
│   └── utils/                        # Common utility modules
│       ├── data_collectors.py        # Data collection functions
│       ├── data_storage.py           # Delta Lake storage functions
│       └── data_validators.py        # Data validation functions
├── tests/                            # Test files
│   ├── test_stock_dashboard.py       # Integration tests
│   └── conftest.py                   # Test configuration
├── pyproject.toml                    # Project configuration
├── dockerfile                        # Docker configuration
└── README.md
```

## 📊 Data Pipeline Schema

### 🥉 Bronze Layer (Raw Data)

#### 1. Bronze Price Data (`bronze_price_daily`)
- **Partition**: `date={collection_date}` (e.g., `date=2025-09-29`)
- **Schema**:
  - `date`: Collection date (date)
  - `ticker`: Stock symbol (string)
  - `open`: Opening price (double)
  - `high`: High price (double)
  - `low`: Low price (double)
  - `close`: Closing price (double)
  - `volume`: Trading volume (long)
  - `adj_close`: Adjusted closing price (double)
  - `ingest_at`: Ingestion timestamp (timestamp)

#### 2. Bronze Dividend Events (`bronze_dividend_events`)
- **Partition**: `date={collection_date}` (e.g., `date=2025-09-29`)
- **Schema**:
  - `ex_date`: Ex-dividend date (date) - Actual dividend payment date
  - `ticker`: Stock symbol (string)
  - `amount`: Dividend amount (double)
  - `date`: Collection date (date) - Date when data was collected
  - `ingest_at`: Ingestion timestamp (timestamp)

### 🥈 Silver Layer (Cleansed Data)

#### 3. Silver Dividend Metrics (`silver_dividend_metrics_daily`)
- **Partition**: `date={collection_date}` (e.g., `date=2025-09-29`)
- **Schema**:
  - `date`: Collection date (date)
  - `ticker`: Stock symbol (string)
  - `last_price`: Latest stock price (double)
  - `market_cap`: Market capitalization (long) - Currently set to 0
  - `dividend_ttm`: TTM dividend (double) - Total dividends for the last 12 months
  - `dividend_yield_ttm`: TTM dividend yield (double) - (TTM dividend / stock price) × 100
  - `div_count_1y`: Annual dividend count (long)
  - `last_div_date`: Latest dividend date (date)
  - `updated_at`: Update timestamp (timestamp)

### 🔑 Key Features
- **Unified Partition Structure**: All tables partitioned in `date={collection_date}` format
- **Dividend Events Table**: Contains both `ex_date` (dividend payment date) and `date` (collection date) columns
- **Compression Optimization**: ZSTD compression applied for improved storage efficiency
- **Auto Optimization**: Performance optimization using Delta Lake's autoOptimize feature

### 🥇 Gold Layer (Analytical/Aggregated Views)

#### 1. Price Time Series Base (`vw_price_timeseries_base`)

* **Path**: `stock-dashboard-472700.bronze_dividend_events.vw_price_timeseries_base`
* **Window (Default)**: Last 3 years
* **Description**: Standard form of daily price time series. Includes day-over-day returns (based on close/adjusted close) and serves as a common base for other views.
* **Schema**:

  * `date` (DATE): Trading date
  * `ticker` (STRING): Stock symbol
  * `open` (FLOAT64): Opening price
  * `high` (FLOAT64): High price
  * `low` (FLOAT64): Low price
  * `close` (FLOAT64): Closing price
  * `adj_close` (FLOAT64): Adjusted closing price
  * `volume` (INT64): Trading volume
  * `ingest_at` (TIMESTAMP): Ingestion timestamp
  * `prev_close` (FLOAT64): Previous day's closing price
  * `prev_adj_close` (FLOAT64): Previous day's adjusted closing price
  * `ret_1d_close` (FLOAT64): Day-over-day return (close price basis)
  * `ret_1d_adj` (FLOAT64): Day-over-day return (adjusted close price basis)

---

#### 2. Price Time Series Enriched (`vw_price_timeseries_enriched`)

* **Path**: `stock-dashboard-472700.bronze_dividend_events.vw_price_timeseries_enriched`
* **Window (Default)**: Last 3 years
* **Description**: Extended time series with commonly used moving averages, volatility, and drawdown metrics for charts and research.
* **Schema**:

  * (Same fields as base) `date`, `ticker`, `open`, `high`, `low`, `close`, `adj_close`, `volume`, `ingest_at`, `prev_close`, `prev_adj_close`, `ret_1d_close`, `ret_1d_adj`
  * `ma_7` (FLOAT64): 7-day moving average (close price)
  * `ma_30` (FLOAT64): 30-day moving average (close price)
  * `ma_120` (FLOAT64): 120-day moving average (close price)
  * `vol_30d` (FLOAT64): 30-day rolling volatility (daily return standard deviation)
  * `rolling_peak` (FLOAT64): Highest adjusted close price from past to current
  * `drawdown` (FLOAT64): Current adjusted close price decline ratio from peak

---

#### 3. Monthly Returns (`vw_returns_monthly`)

* **Path**: `stock-dashboard-472700.bronze_dividend_events.vw_returns_monthly`
* **Window (Default)**: Last 5 years
* **Description**: Monthly returns calculated from month-start/month-end prices. Suitable for bar/heatmap charts.
* **Schema**:

  * `month` (DATE): Month (date truncated to 1st day)
  * `ticker` (STRING): Stock symbol
  * `month_open` (FLOAT64): Month's first trading day closing price
  * `month_close` (FLOAT64): Month's last trading day closing price
  * `ret_1m` (FLOAT64): Monthly return = (month_close - month_open) / month_open

---

#### 4. Quarterly Returns (`vw_returns_quarterly`)

* **Path**: `stock-dashboard-472700.bronze_dividend_events.vw_returns_quarterly`
* **Window (Default)**: Last 3 years
* **Description**: Quarter-over-quarter returns calculated from quarter-start/quarter-end prices.
* **Schema**:

  * `qtr` (DATE): Quarter (1st day of the quarter's first month)
  * `ticker` (STRING): Stock symbol
  * `qtr_open` (FLOAT64): Quarter's first trading day closing price
  * `qtr_close` (FLOAT64): Quarter's last trading day closing price
  * `ret_qoq` (FLOAT64): Quarterly return = (qtr_close - qtr_open) / qtr_open

---

#### 5. Latest Price Snapshot (`vw_price_latest_snapshot`)

* **Path**: `stock-dashboard-472700.bronze_dividend_events.vw_price_latest_snapshot`
* **Window (Default)**: Latest 1 record (no time limit)
* **Description**: Provides only the **most recent** trading day (+latest ingest) row for each ticker. Source for dashboard cards/rankings.
* **Schema**:

  * `date` (DATE): Latest trading date
  * `ticker` (STRING): Stock symbol
  * `open`, `high`, `low`, `close`, `adj_close` (FLOAT64)
  * `volume` (INT64)
  * `ingest_at` (TIMESTAMP)

---

#### 6. Latest Dividend Metrics Snapshot (`vw_dividend_metrics_latest`)

* **Path**: `stock-dashboard-472700.bronze_dividend_events.vw_dividend_metrics_latest`
* **Window (Default)**: Latest 1 record (both metrics/price)
* **Description**: Summary view joining the latest row of `silver_dividend_metrics_daily` with the **latest price snapshot**.
* **Schema**:

  * `ticker` (STRING)
  * `metrics_date` (DATE): Metrics reference date
  * `metrics_last_price` (FLOAT64): Latest stock price at metrics calculation time
  * `market_cap` (INT64): Market capitalization (may be 0 currently)
  * `dividend_ttm` (FLOAT64): Total dividends for the last 12 months
  * `dividend_yield_ttm` (FLOAT64): TTM dividend yield (%)
  * `div_count_1y` (INT64): Annual dividend count
  * `last_div_date` (DATE): Latest dividend date
  * `updated_at` (TIMESTAMP): Metrics update timestamp
  * `price_date` (DATE): Latest price reference date
  * `latest_close` (FLOAT64): Latest closing price
  * `latest_adj_close` (FLOAT64): Latest adjusted closing price

---

#### 7. Dividend Calendar (2 Years) (`vw_dividend_calendar_2y`)

* **Path**: `stock-dashboard-472700.bronze_dividend_events.vw_dividend_calendar_2y`
* **Window (Default)**: Last 2 years
* **Description**: Dividend event schedule for the last 2 years (for table/calendar widgets).
* **Schema**:

  * `ex_date` (DATE): Ex-dividend date (actual payment reference date)
  * `ticker` (STRING)
  * `amount` (FLOAT64): Dividend amount (per share)
  * `collect_date` (DATE): Collection date (original `date`)
  * `ingest_at` (TIMESTAMP): Ingestion timestamp

---

#### 8. Dividend Yield Ranking (Daily, 2 Years) (`vw_dividend_yield_rank_daily`)

* **Path**: `stock-dashboard-472700.bronze_dividend_events.vw_dividend_yield_rank_daily`
* **Window (Default)**: Last 2 years
* **Description**: Daily dividend yield ranking in descending order. Ready for use in Top-N filters/ranking tables.
* **Schema**:

  * `date` (DATE)
  * `ticker` (STRING)
  * `last_price` (FLOAT64)
  * `dividend_ttm` (FLOAT64)
  * `dividend_yield_ttm` (FLOAT64)
  * `div_count_1y` (INT64)
  * `last_div_date` (DATE)
  * `updated_at` (TIMESTAMP)
  * `yield_rank` (INT64): Dividend yield ranking within date (1=highest)

---

#### 9. Market Summary (2 Years) (`vw_market_daily_summary`)

* **Path**: `stock-dashboard-472700.bronze_dividend_events.vw_market_daily_summary`
* **Window (Default)**: Last 2 years
* **Description**: Daily summary of broad market breadth (advancing/declining/flat counts, average returns, total volume).
* **Schema**:

  * `date` (DATE)
  * `up_cnt` (INT64): Advancing stocks count (daily return > 0)
  * `down_cnt` (INT64): Declining stocks count (daily return < 0)
  * `flat_cnt` (INT64): Flat stocks count (daily return = 0)
  * `avg_ret_1d` (FLOAT64): Average daily return
  * `total_volume` (INT64): Total volume

---

#### 10. Daily Top Movers (180 Days) (`vw_top_movers_daily`)

* **Path**: `stock-dashboard-472700.bronze_dividend_events.vw_top_movers_daily`
* **Window (Default)**: Last 180 days
* **Description**: Preprocessed ranking for quickly selecting top/bottom performers by daily returns.
* **Schema**:

  * `date` (DATE)
  * `ticker` (STRING)
  * `close` (FLOAT64)
  * `adj_close` (FLOAT64)
  * `ret_1d_adj` (FLOAT64): Daily return (adjusted close price basis)
  * `rn_desc` (INT64): **Advancing** ranking within date (1=highest gain)
  * `rn_asc` (INT64): **Declining** ranking within date (1=highest loss)

---

#### 11. Factor: Dividend Yield Quintile vs Forward 1M Returns (3 Years) (`vw_factor_dividend_vs_fwd_return`)

* **Path**: `stock-dashboard-472700.bronze_dividend_events.vw_factor_dividend_vs_fwd_return`
* **Window (Default)**: Last 3 years
* **Description**: Factor study aggregation comparing dividend yield quintiles (1=high dividend) with average 21-trading-day forward returns.
* **Schema**:

  * `date` (DATE)
  * `dy_quintile` (INT64): Dividend yield quintile (1~5, 1 is highest group)
  * `avg_fwd_ret_21` (FLOAT64): Average 21-trading-day forward return
  * `n` (INT64): Sample stock count for each quintile


## 🚀 Installation and Execution

### 1. Install uv
```bash
# Install uv (Linux/macOS)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Or via pip
pip install uv
```

### 2. Project Setup
```bash
# Create virtual environment and install dependencies
uv sync

# Install including development dependencies
uv sync --dev
```

### 3. Environment Variables Setup
```bash
# Google Cloud authentication setup
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/service-account-key.json
```

### 4. Execution Commands

#### Bronze Layer (Raw Data Collection)
```bash
# Bronze Layer full collection (price + dividend)
uv run python -m src.app.main --mode bronze-full --date 2025-09-29

# Collect price data only
uv run python -m src.app.main --mode bronze-price --date 2025-09-29

# Collect dividend data only
uv run python -m src.app.main --mode bronze-dividend --date 2025-09-29
```

#### Silver Layer (Cleansed Data Generation)
```bash
# Execute Silver Layer (dividend metrics calculation)
uv run python -m src.app.main --mode silver --date 2025-09-29
```

## 🧪 Development Tools

### Code Quality Management
```bash
# Code formatting
uv run black .

# Import sorting
uv run isort .

# Linting
uv run flake8 .

# Type checking
uv run mypy .
```

### Testing
```bash
# Run all tests
uv run pytest

# Run specific test file
uv run pytest tests/test_stock_dashboard.py -v

# Check test coverage
uv run pytest --cov=src
```

## 📈 Data Collection Status

### Currently Collected Data
- **S&P 500 Stocks**: 503 stocks
- **Price Data**: Daily OHLCV data
- **Dividend Data**: Dividend payment dates and amounts
- **Collection Frequency**: Manual execution (scheduling planned for future)

### Dividend Metrics Calculation
- **TTM Dividend Yield**: Based on total dividends for the last 12 months
- **Dividend Count**: Annual dividend payment frequency
- **Latest Dividend Date**: Most recent dividend payment date

## 🔧 Configuration Files

### pyproject.toml
- Project metadata and dependency definitions
- Development tools configuration (black, isort, flake8, mypy, pytest)

### dockerfile
- Python 3.12 based Docker image
- Dependency management using uv
- Cloud Run optimized

## 🚧 Future Plans

- Introduction of dividend stock analysis agent using n8n
- Expansion to various stock markets beyond S&P 500, including NASDAQ

## 📞 Contact

If you have any questions about the project, please create an issue.

---
