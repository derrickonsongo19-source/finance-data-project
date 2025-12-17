import duckdb
import polars as pl
from datetime import datetime

print("=== SILVER LAYER: Cleaned, Typed Data ===")

# Connect to DuckDB
conn = duckdb.connect('finance_data.db')

# 1. Load bronze data into Polars
print("1. Loading bronze data...")
transactions_df = pl.read_parquet('data/bronze/transactions_raw.parquet')
news_df = pl.read_parquet('data/bronze/news_raw.parquet')

print(f"   → Transactions: {transactions_df.shape[0]} rows")
print(f"   → News: {news_df.shape[0]} rows")

# 2. Clean transactions
print("\n2. Cleaning transactions...")

# Remove duplicates
transactions_df = transactions_df.unique()

# Check current data types
print(f"   Current schema: {transactions_df.schema}")

# Convert date from Datetime to Date (extract just the date part)
transactions_df = transactions_df.with_columns(
    pl.col("date").dt.date().alias("date")
)

# Ensure string columns are Utf8
transactions_df = transactions_df.with_columns([
    pl.col("category").cast(pl.Utf8),
    pl.col("account").cast(pl.Utf8)
])

# Remove nulls in critical columns
transactions_df = transactions_df.filter(
    pl.col("date").is_not_null() &
    pl.col("amount").is_not_null() &
    pl.col("category").is_not_null()
)

# Add a unique transaction ID if missing
if "id" not in transactions_df.columns:
    transactions_df = transactions_df.with_row_index("id")

# 3. Clean news data
print("3. Cleaning news data...")
# Convert timestamp string to Datetime
news_df = news_df.with_columns(
    pl.col("timestamp").str.to_datetime("%Y-%m-%d %H:%M:%S.%f")
)

# Ensure sentiment is Float64
news_df = news_df.with_columns(
    pl.col("sentiment").cast(pl.Float64)
)

# 4. Save to silver layer
print("\n4. Saving to silver layer...")
transactions_df.write_parquet('data/silver/transactions_clean.parquet')
news_df.write_parquet('data/silver/news_clean.parquet')

# 5. Create silver tables in DuckDB
conn.execute("""
    CREATE OR REPLACE TABLE silver_transactions AS 
    SELECT * FROM 'data/silver/transactions_clean.parquet'
""")

conn.execute("""
    CREATE OR REPLACE TABLE silver_news AS 
    SELECT * FROM 'data/silver/news_clean.parquet'
""")

# Show results
print("\n✅ SILVER LAYER CREATED:")
print(f"   Clean transactions: {transactions_df.shape[0]} rows")
print(f"   Clean news: {news_df.shape[0]} rows")

# Show sample of cleaned data
print("\n📋 Sample of cleaned transactions:")
print(transactions_df.head(3))

conn.close()
