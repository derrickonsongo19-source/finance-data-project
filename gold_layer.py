import duckdb
import polars as pl

print("=== GOLD LAYER: Aggregated Business-Ready Data ===")

# Connect to DuckDB
conn = duckdb.connect('finance_data.db')

# 1. Load silver data
print("1. Loading silver data...")
transactions_df = pl.read_parquet('data/silver/transactions_clean.parquet')

# 2. Create aggregated business tables
print("\n2. Creating gold layer aggregations...")

# A. Daily spending summary
daily_summary = transactions_df.group_by("date").agg([
    pl.count().alias("transaction_count"),
    pl.sum("amount").alias("total_amount"),
    pl.col("amount").filter(pl.col("amount") < 0).sum().alias("total_spending"),
    pl.col("amount").filter(pl.col("amount") > 0).sum().alias("total_income")
]).sort("date")

# B. Category spending analysis
category_summary = transactions_df.filter(pl.col("amount") < 0).group_by("category").agg([
    pl.count().alias("transaction_count"),
    pl.sum("amount").alias("total_spent"),
    pl.mean("amount").alias("avg_transaction"),
    (pl.col("amount").sum() / transactions_df.filter(pl.col("amount") < 0).select(pl.sum("amount")).item() * 100).alias("percentage_of_total")
]).sort("total_spent")

# C. Account balance tracking
account_balance = transactions_df.group_by("account_id").agg([
    pl.sum("amount").alias("current_balance"),
    pl.count().alias("transaction_count"),
    pl.col("amount").filter(pl.col("amount") < 0).sum().alias("total_withdrawals"),
    pl.col("amount").filter(pl.col("amount") > 0).sum().alias("total_deposits")
])

# 3. Save gold layer data
print("\n3. Saving gold layer data...")
daily_summary.write_parquet('data/gold/daily_summary.parquet')
category_summary.write_parquet('data/gold/category_summary.parquet')
account_balance.write_parquet('data/gold/account_balance.parquet')

# 4. Create gold tables in DuckDB
conn.execute("CREATE OR REPLACE TABLE gold_daily_summary AS SELECT * FROM 'data/gold/daily_summary.parquet'")
conn.execute("CREATE OR REPLACE TABLE gold_category_summary AS SELECT * FROM 'data/gold/category_summary.parquet'")
conn.execute("CREATE OR REPLACE TABLE gold_account_balance AS SELECT * FROM 'data/gold/account_balance.parquet'")

# 5. Create a unified view for reporting
conn.execute("""
    CREATE OR REPLACE VIEW financial_dashboard AS
    SELECT 
        d.date,
        d.transaction_count,
        d.total_spending,
        d.total_income,
        c.category,
        c.total_spent as category_spending,
        a.account_id as account,
        a.current_balance
    FROM gold_daily_summary d
    CROSS JOIN gold_category_summary c
    CROSS JOIN gold_account_balance a
    ORDER BY d.date DESC
    LIMIT 50
""")

# 6. Show results
print("\n✅ GOLD LAYER CREATED:")
print(f"   Daily summary: {daily_summary.shape[0]} days")
print(f"   Categories: {category_summary.shape[0]} categories")
print(f"   Accounts: {account_balance.shape[0]} accounts")

print("\n📊 Sample Gold Data:")
print("\nDaily Summary (last 3 days):")
print(daily_summary.tail(3))

print("\nTop Spending Categories:")
print(category_summary.head(5))

print("\nAccount Balances:")
print(account_balance)

# Show view
print("\n📈 Financial Dashboard View created: 'financial_dashboard'")
print(conn.execute("SELECT * FROM financial_dashboard LIMIT 3").df())

conn.close()
