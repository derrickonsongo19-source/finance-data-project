import duckdb
import polars as pl
from datetime import datetime
from data_contracts import DataContract, ColumnContract
import os

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

# Parse dates only if they're strings
if transactions_df["date"].dtype == pl.Utf8:
    transactions_df = transactions_df.with_columns(
        pl.col("date").str.strptime(pl.Date, "%Y-%m-%d")
    )
# If it's already datetime, keep it as is
elif transactions_df["date"].dtype in [pl.Datetime, pl.Datetime('us'), pl.Datetime('ms'), pl.Datetime('ns')]:
    # Convert datetime to date
    transactions_df = transactions_df.with_columns(
        pl.col("date").dt.date().alias("date")
    )

# Clean amounts
transactions_df = transactions_df.with_columns(
    pl.col("amount").cast(pl.Float64)
)

# Fill null categories with "uncategorized"
transactions_df = transactions_df.with_columns(
    pl.col("category").fill_null("uncategorized")
)

# Lowercase and clean categories
category_mapping = {
    "groceries": "food",
    "grocery store": "food",
    "food": "food",
    "dining": "food",
    "restaurant": "food",
    "coffee": "food",
    "transport": "transport",
    "fuel": "transport",
    "gas": "transport",
    "uber": "transport",
    "electronics": "shopping",
    "shopping": "shopping",
    "netflix": "entertainment",
    "spotify": "entertainment",
    "salary": "income",
    "salary deposit": "income",
    "utilities": "utilities",
    "electricity": "utilities",
    "water": "utilities",
}

# First lowercase category
if "category" in transactions_df.columns:
    transactions_df = transactions_df.with_columns(
        pl.col("category").str.to_lowercase().alias("category")
    )

# Then map using replace
transactions_df = transactions_df.with_columns(
    pl.col("category").replace(category_mapping, default="other")
)

# Fill any nulls with "uncategorized"
transactions_df = transactions_df.with_columns(
    pl.col("category").fill_null("uncategorized")
)

transactions_df = transactions_df.with_columns(
    pl.col("id").cast(pl.Utf8).alias("transaction_id"),
    pl.col("account").alias("account_id")
)

# Drop the original id and account columns
transactions_clean_df = transactions_df.drop(["id", "account"])

print(f"   → Cleaned transactions: {transactions_clean_df.shape[0]} rows")

# 3. Clean news data
print("\n3. Cleaning news data...")

# Check if timestamp column exists and clean it
if "timestamp" in news_df.columns:
    if news_df["timestamp"].dtype == pl.Utf8:
        # Try parsing with microseconds format
        try:
            news_clean_df = news_df.with_columns(
                pl.col("timestamp").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S%.f").alias("published_at"),
                pl.col("sentiment").cast(pl.Float64).alias("sentiment_score")
            )
        except:
            # If microsecond parsing fails, try without microseconds
            news_clean_df = news_df.with_columns(
                pl.col("timestamp").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S").alias("published_at"),
                pl.col("sentiment").cast(pl.Float64).alias("sentiment_score")
            )
    else:
        news_clean_df = news_df.with_columns(
            pl.col("timestamp").alias("published_at"),
            pl.col("sentiment").cast(pl.Float64).alias("sentiment_score")
        )
else:
    # If no timestamp, use current time
    news_clean_df = news_df.with_columns(
        pl.lit(datetime.now()).alias("published_at"),
        pl.col("sentiment").cast(pl.Float64).alias("sentiment_score")
    )

# Drop nulls and rename for consistency
news_clean_df = news_clean_df.drop_nulls().select(["published_at", "headline", "sentiment_score"])

print(f"   → Cleaned news: {news_clean_df.shape[0]} rows")

# 4. Data Contract Validation
print("\n4. Validating against data contract...")

# Define Silver layer contract that matches ACTUAL data schema
silver_contract = DataContract(
    contract_name="silver_transactions_contract",
    columns=[
        ColumnContract(name="transaction_id", dtype="Utf8", nullable=False),
        ColumnContract(name="amount", dtype="Float64", nullable=False, min_value=0.01),
        ColumnContract(name="date", dtype="Date", nullable=False),
        ColumnContract(name="category", dtype="Utf8", nullable=True),
        ColumnContract(name="account_id", dtype="Utf8", nullable=False)
        # Note: merchant and description columns are not in our data, so they're removed
    ]
)

# Validate transactions
validation_result = silver_contract.validate_dataframe(transactions_clean_df)

if validation_result["valid"]:
    print("   ✅ Data contract validation passed")
else:
    print("   ❌ Data contract validation failed:")
    for error in validation_result["errors"]:
        print(f"      - {error}")
    raise ValueError("Data contract validation failed")

# 5. Save cleaned data
print("\n5. Saving cleaned data...")

# Create directories if they don't exist
os.makedirs('data/silver', exist_ok=True)
os.makedirs('data/contracts', exist_ok=True)

# Save to Parquet
transactions_clean_df.write_parquet('data/silver/transactions_clean.parquet')
news_clean_df.write_parquet('data/silver/news_clean.parquet')

print(f"   → Saved to 'data/silver/transactions_clean.parquet'")
print(f"   → Saved to 'data/silver/news_clean.parquet'")

# 6. Save the data contract
with open("data/contracts/silver_transactions_contract.json", "w") as f:
    f.write(silver_contract.to_json())
print("   📄 Contract saved to 'data/contracts/silver_transactions_contract.json'")

# 7. Update DuckDB with cleaned data
print("\n6. Updating DuckDB...")

conn.execute("""
    CREATE OR REPLACE TABLE silver_transactions AS
    SELECT * FROM read_parquet('data/silver/transactions_clean.parquet')
""")

conn.execute("""
    CREATE OR REPLACE TABLE silver_news AS
    SELECT * FROM read_parquet('data/silver/news_clean.parquet')
""")

print("   ✅ Silver layer tables created in DuckDB")

# Show sample
print("\n📊 Sample of cleaned transactions:")
print(conn.execute("SELECT * FROM silver_transactions LIMIT 3").fetchall())

conn.close()
print("\n🎉 Silver layer processing complete with data contract validation!")
