import duckdb
import pandas as pd
from datetime import datetime

print("=== BRONZE LAYER: Raw Data Ingestion ===")

# Connect to DuckDB (creates database file if doesn't exist)
conn = duckdb.connect('finance_data.db')

# 1. Load raw transactions from PostgreSQL
print("1. Loading transactions from PostgreSQL...")
transactions_df = conn.execute("""
    SELECT * FROM postgres_scan(
        'host=localhost port=5432 dbname=postgres user=postgres password=mysecretpassword',
        'public',
        'transactions'
    )
""").df()

# Save to bronze as Parquet (raw format)
transactions_df.to_parquet('data/bronze/transactions_raw.parquet')
print(f"   → Saved {len(transactions_df)} transactions to bronze layer")

# 2. Load raw news data
print("2. Loading news data from CSV...")
news_df = pd.read_csv('financial_news.csv')
news_df.to_parquet('data/bronze/news_raw.parquet')
print(f"   → Saved {len(news_df)} news items to bronze layer")

# 3. Create bronze schema table
conn.execute("""
    CREATE OR REPLACE TABLE bronze_transactions AS 
    SELECT * FROM 'data/bronze/transactions_raw.parquet'
""")

conn.execute("""
    CREATE OR REPLACE TABLE bronze_news AS 
    SELECT * FROM 'data/bronze/news_raw.parquet'
""")

# Show bronze tables
print("\n✅ BRONZE LAYER CREATED:")
print(conn.execute("SHOW TABLES").df())

conn.close()
