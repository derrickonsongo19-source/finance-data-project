# Personal Finance Data Pipeline Project

## 🏗️ Medallion Architecture

This project implements a **Medallion Architecture** with three distinct layers:

### Bronze Layer (Raw Data)
- **Location**: `data/bronze/`
- **Content**: Raw ingested data from simulated banking APIs, CSV exports, and web scraping.
- **Format**: Parquet files preserving original schema.
- **Example**: `transactions_raw.parquet`, `news_raw.parquet`

### Silver Layer (Cleaned & Validated Data)
- **Location**: `data/silver/`
- **Content**: Cleaned, typed, and validated data with enforced data contracts.
- **Processing**: Deduplication, date parsing, category standardization, null handling.
- **Validation**: Data contracts using Pydantic/Great Expectations.
- **Example**: `transactions_clean.parquet`, `news_clean.parquet`

### Gold Layer (Business-Ready Aggregates)
- **Location**: `data/gold/`
- **Content**: Aggregated business metrics ready for reporting and analytics.
- **Examples**:
  - `daily_summary.parquet`: Daily transaction counts, spending, income
  - `category_summary.parquet`: Spending by category with percentages
  - `account_balance.parquet`: Current balance per account
- **Database**: DuckDB tables and a unified `financial_dashboard` view.

### Data Flow
1. **Ingestion** → Bronze (raw)
2. **Cleaning & Validation** → Silver (clean)
3. **Aggregation & Modeling** → Gold (business-ready)

### Technology Used per Layer
- **Bronze**: Polars (reading), FastAPI (mock APIs)
- **Silver**: Polars (transformations), Data Contracts (validation)
- **Gold**: Polars (aggregations), DuckDB (querying)

All layers are queryable via DuckDB and can be accessed through the FastAPI service for real-time insights.
