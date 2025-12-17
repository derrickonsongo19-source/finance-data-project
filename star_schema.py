import duckdb
import polars as pl
from datetime import datetime

print("=== STAR SCHEMA IMPLEMENTATION ===")

# Connect to DuckDB
conn = duckdb.connect('finance_data.db')

# 1. Create Dimension Tables
print("1. Creating dimension tables...")

# Date dimension
conn.execute("""
    CREATE OR REPLACE TABLE dim_date AS
    SELECT DISTINCT
        date as date_key,
        EXTRACT(YEAR FROM date) as year,
        EXTRACT(MONTH FROM date) as month,
        EXTRACT(DAY FROM date) as day,
        EXTRACT(QUARTER FROM date) as quarter,
        EXTRACT(DOW FROM date) as day_of_week,
        CASE WHEN EXTRACT(DOW FROM date) IN (0, 6) THEN 'Weekend' ELSE 'Weekday' END as day_type
    FROM silver_transactions
    ORDER BY date
""")

# Category dimension
conn.execute("""
    CREATE OR REPLACE TABLE dim_category AS
    SELECT 
        ROW_NUMBER() OVER (ORDER BY category) as category_id,
        category as category_name,
        CASE 
            WHEN category IN ('Food', 'Coffee', 'Restaurant') THEN 'Dining'
            WHEN category IN ('Shopping', 'Electronics') THEN 'Retail'
            WHEN category IN ('Transport', 'Fuel') THEN 'Transportation'
            WHEN category LIKE '%Salary%' OR category LIKE '%Deposit%' THEN 'Income'
            ELSE 'Other'
        END as category_group
    FROM (
        SELECT DISTINCT category FROM silver_transactions
        UNION ALL
        SELECT 'Unknown'
    )
    ORDER BY category_name
""")

# Account dimension
conn.execute("""
    CREATE OR REPLACE TABLE dim_account AS
    SELECT 
        ROW_NUMBER() OVER (ORDER BY account) as account_id,
        account as account_name,
        CASE 
            WHEN account LIKE '%Checking%' THEN 'Checking'
            WHEN account LIKE '%Savings%' THEN 'Savings'
            WHEN account LIKE '%Credit%' THEN 'Credit Card'
            ELSE 'Other'
        END as account_type
    FROM (
        SELECT DISTINCT account FROM silver_transactions
        UNION ALL
        SELECT 'Unknown'
    )
    ORDER BY account_name
""")

# Merchant dimension
conn.execute("""
    CREATE OR REPLACE TABLE dim_merchant AS
    SELECT 
        ROW_NUMBER() OVER (ORDER BY category) as merchant_id,
        category as merchant_name,
        CASE 
            WHEN category IN ('Netflix', 'Streaming') THEN 'Subscription'
            WHEN category IN ('Grocery Store', 'Supermarket') THEN 'Groceries'
            WHEN category LIKE '%Tech%' OR category = 'Electronics' THEN 'Technology'
            ELSE 'General'
        END as merchant_type
    FROM (
        SELECT DISTINCT category FROM silver_transactions
        WHERE category NOT LIKE '%Salary%' AND category NOT LIKE '%Deposit%'
        UNION ALL
        SELECT 'Other'
    )
    ORDER BY merchant_name
""")

# 2. Create Fact Table
print("2. Creating fact table...")

conn.execute("""
    CREATE OR REPLACE TABLE fact_transactions AS
    SELECT 
        t.id as transaction_id,
        t.date as transaction_date,
        d.date_key,
        c.category_id,
        a.account_id,
        m.merchant_id,
        t.amount,
        CASE WHEN t.amount < 0 THEN 'Expense' ELSE 'Income' END as transaction_type,
        ABS(t.amount) as absolute_amount,
        CURRENT_TIMESTAMP as loaded_at
    FROM silver_transactions t
    LEFT JOIN dim_date d ON t.date = d.date_key
    LEFT JOIN dim_category c ON t.category = c.category_name
    LEFT JOIN dim_account a ON t.account = a.account_name
    LEFT JOIN dim_merchant m ON t.category = m.merchant_name
    ORDER BY t.date DESC
""")

# 3. Sample analytics query
print("\n3. Sample star schema query...")

result = conn.execute("""
    SELECT 
        d.year,
        d.month,
        c.category_group,
        a.account_type,
        COUNT(*) as transaction_count,
        SUM(f.amount) as net_amount,
        SUM(CASE WHEN f.transaction_type = 'Expense' THEN f.absolute_amount ELSE 0 END) as total_expenses,
        SUM(CASE WHEN f.transaction_type = 'Income' THEN f.absolute_amount ELSE 0 END) as total_income
    FROM fact_transactions f
    JOIN dim_date d ON f.date_key = d.date_key
    JOIN dim_category c ON f.category_id = c.category_id
    JOIN dim_account a ON f.account_id = a.account_id
    GROUP BY d.year, d.month, c.category_group, a.account_type
    ORDER BY d.year DESC, d.month DESC, total_expenses DESC
""").df()

# 4. Show results
print("\n✅ STAR SCHEMA CREATED:")
print(f"   Dim Tables: date, category, account, merchant")
print(f"   Fact Table: fact_transactions with {conn.execute('SELECT COUNT(*) FROM fact_transactions').fetchone()[0]} rows")

print("\n📋 Dimension Table Sizes:")
tables = ['dim_date', 'dim_category', 'dim_account', 'dim_merchant']
for table in tables:
    count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    print(f"   {table}: {count} rows")

print("\n📊 Sample Analytics Query Results:")
print(result.head(5))

print("\n⭐ Star schema ready for analysis!")
conn.close()
